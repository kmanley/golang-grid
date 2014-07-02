/*
jobs are in memory from time they are created until job.IsInFinalState()
jobs that are in final state are purged from in-memory store after X minutes
getJob(memOnly=False) will transparently pull a job that is on disk (not in mem) into mem
there are only a few places where getJob(id, memOnly=False) should be used, e.g. retryJob

jobs and tasks will enqueue state changes to db persister channel

worker stuff is never persisted to db

need another set of apis for pulling bulk data from db for gui

check group about how to serialize error over json, or just serialize as string
make our own error type that can serialize itself to json?
*/

package grid

import (
	"container/heap"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"
)

type model struct {
	Mutex   sync.Mutex
	Fifo    bool
	Jobs    JobHeap
	jobMap  JobMap
	Workers WorkerMap
}

// this is only here to support unit tests
func resetModel() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.jobMap = make(JobMap)
	Model.Jobs = Model.Jobs[0:0]
	Model.Workers = make(WorkerMap)
}

func init() {
	resetModel()
}

var Model = &model{} // TODO: lowercase
var prng = rand.New(rand.NewSource(time.Now().Unix()))
var lastJobID JobID

const JOBID_FORMAT = "060102150405.999999"

// TODO: replace with guid? or add distributor ID prefix? the problem with the existing
// scheme is that there could be dupes in an environment with more than 1 grid
// OTOH this scheme has a nice time-ordered property
func newJobID() (newID JobID) {
	for {
		now := time.Now()
		newID = JobID(strings.Replace(now.UTC().Format(JOBID_FORMAT), ".", "", 1))
		for len(newID) < len(JOBID_FORMAT)-1 {
			newID = newID + "0"
		}
		// this is a safeguard when creating jobs quickly in a loop, to ensure IDs are
		// still unique even if clock resolution is too low to always provide a unique
		// value for the format string
		if newID != lastJobID {
			break
		} else {
			fmt.Println("*** paused creating new job ID; clock resolution lower than expected") // TODO: log warning
		}
	}
	lastJobID = newID
	return newID
}

func getJob(jobID JobID, memOnly bool) (job *Job, err error) {
	job, found := Model.jobMap[jobID]
	if !found {
		if memOnly {
			err = ERR_INVALID_JOB_ID
		} else {
			job, err = loadJobFromDB(jobID)
		}
	}
	return
}

func loadJobFromDB(jobID JobID) (job *Job, err error) {
	// TODO:
	err = ERR_INVALID_JOB_ID
	return
}

func CreateJob(jobDef *JobDefinition) (jobID JobID, err error) {
	// TODO: handle AssignSingleTaskPerWorker
	jobDef.Ctrl.CompiledWorkerNameRegex, err = regexp.Compile(jobDef.Ctrl.WorkerNameRegex)
	if err != nil {
		return "", err // TODO: wrap error
	}

	jobID = newJobID()
	newJob := NewJob(jobID, jobDef.Cmd, jobDef.Description, (jobDef.Data).([]interface{}),
		jobDef.Ctx, jobDef.Ctrl)

	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.jobMap[jobID] = newJob
	heap.Push(&(Model.Jobs), newJob)
	fmt.Println("created job", newJob.ID)
	return newJob.ID, nil
}

func SetJobPriority(jobID JobID, priority int8) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}
	if !job.isWorking() {
		return ERR_WRONG_JOB_STATE
	}
	// update the priority heap
	Model.Jobs.ChangePriority(job, priority)
	return nil
}

func SetJobMaxConcurrency(jobID JobID, maxcon uint32) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}
	if !job.isWorking() {
		return ERR_WRONG_JOB_STATE
	}

	job.setMaxConcurrency(maxcon)
	return nil
}

func SuspendJob(jobID JobID, graceful bool) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}
	if !job.isWorking() {
		return ERR_WRONG_JOB_STATE
	}

	job.suspend(graceful)
	return nil
}

func CancelJob(jobID JobID) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, err := getJob(jobID, false) // allow pulling from DB, e.g. could be suspended
	if err != nil {
		return err
	}
	// we allow canceling a suspended job
	if !(job.isWorking() || job.State() == JOB_SUSPENDED) {
		return ERR_WRONG_JOB_STATE
	}
	job.cancel()
	return nil
}

func RetryJob(jobID JobID) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, err := getJob(jobID, false)
	if err != nil {
		return err
	}
	if job.isWorking() {
		return ERR_WRONG_JOB_STATE
	}
	return job.retry()
}

// NOTE: caller must hold mutex
func getJobForWorker(workerName string) (job *Job) {
	now := time.Now()
	candidates := make([]*Job, 0, Model.Jobs.Len())
	jobs := Model.Jobs.Copy()
	for jobs.Len() > 0 {
		// note: this pops in priority + createdtime order
		job = (heap.Pop(jobs)).(*Job)

		// TODO: consider best order of the following clauses for performance
		// TODO: add support for hidden workers?
		if !job.isWorking() {
			// job is suspended, cancelled, or done
			continue
		}

		if job.Ctrl.StartTime.After(now) {
			// job not scheduled to start yet
			continue
		}
		if job.IdleTasks.Len() < 1 {
			// all tasks are currently being executed
			continue
		}
		maxcon := job.getMaxConcurrency()
		if (maxcon > 0) && (uint32(len(job.RunningTasks)) >= maxcon) {
			// job is configured with a max concurrency and that has been reached
			continue
		}
		// TODO: don't access Job.Ctrl directly, use accessor functions on Job
		if (job.Ctrl.CompiledWorkerNameRegex != nil) && (!job.Ctrl.CompiledWorkerNameRegex.MatchString(workerName)) {
			// job is configured for specific workers and the requesting worker is not a match
			continue
		}
		if len(candidates) > 0 && (job.Ctrl.JobPriority != candidates[0].Ctrl.JobPriority) {
			// there are already higher priority candidates so no point in considering this
			// job or any others that will follow
			break
		}
		// if we got this far, then this job is a candidate for selection
		candidates = append(candidates, job)
	}
	if len(candidates) < 1 {
		return nil
	}
	if Model.Fifo {
		return candidates[0]
	} else {
		return candidates[prng.Intn(len(candidates))]
	}
}

// NOTE: caller must hold mutex
func getOrCreateWorker(workerName string) *Worker {
	worker, found := Model.Workers[workerName]
	if !found {
		worker = NewWorker(workerName)
		Model.Workers[workerName] = worker
	}
	return worker
}

func forgetWorker(workerName string) {
	delete(Model.Workers, workerName)
}

func reallocateWorkerTask(worker *Worker) {
	jobID := worker.CurrJob
	job, err := getJob(jobID, true)
	if err != nil {
		return
	}
	job.reallocateWorkerTask(worker)
}

func GetTaskForWorker(workerName string) (task *Task) {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	worker := getOrCreateWorker(workerName)
	if worker.isWorking() {
		// Out of sync; we think this worker already has a task but it is requesting
		// a new one. Since our view is the truth, we need to reallocate the task we
		// thought this worker was working on. Note we don't return an error in this
		// case, we go ahead and allocate a new task
		reallocateWorkerTask(worker)
	}
	job := getJobForWorker(workerName)
	if job == nil {
		// no work available right now
		return
	}
	task = job.allocateTask(worker)
	return
}

func SetTaskDone(workerName string, jobID JobID, taskSeq int, result interface{},
	stdout string, stderr string, taskError error) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()

	worker := getOrCreateWorker(workerName)
	if !(worker.CurrJob == jobID && worker.CurrTask == taskSeq) {
		// Out of sync; the worker is reporting a result for a different job/task to
		// what we think it's working on. Our view is the truth so we reallocate the
		// job we thought the worker was working on and reject its result.
		// TODO: logging
		reallocateWorkerTask(worker)
		// Ignore the error; the worker will request a new task and get back in sync
		return nil
	}

	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}

	task := job.getRunningTask(taskSeq)
	if task == nil {
		// TODO: logging
		return ERR_TASK_NOT_RUNNING
	}

	job.setTaskDone(worker, task, result, stdout, stderr, taskError)

	return nil
}

func CheckJobStatus(workerName string, jobID JobID, taskSeq int) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()

	// TODO: this block is copied from SetTaskDone, refactor
	worker := getOrCreateWorker(workerName)
	if !(worker.CurrJob == jobID && worker.CurrTask == taskSeq) {
		// Out of sync; the worker is asking about a different job/task to
		// what we think it's working on. Our view is the truth so we reallocate the
		// job we thought the worker was working on
		// TODO: what about jobID, taskSeq - do we need to reallocate that too??
		// TODO: do we really need to store worker.Job, worker.Task? I guess so since we display
		//          workers and what they're working on
		// TODO: logging
		reallocateWorkerTask(worker)
		// Ignore the error; the worker will request a new task and get back in sync
		return nil
	}

	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}

	// state := job.State()
	// TODO: if state == JOB_CANCELLED || state == JOB_DONE_ERR || state == JOB

	// TODO: need to consider suspended task with graceful vs. graceless param...
	task := job.getRunningTask(taskSeq)
	if task == nil {
		// TODO: logging
		return ERR_TASK_NOT_RUNNING
	}
	return nil
}

func GetJobResult(jobID JobID) ([]interface{}, error) {
	// TODO: after this func is done get basic distributor/worker/client setup and demonstrate
	// job running end to end
	// next, add web ui
	// next, add db persistence
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()

	job, err := getJob(jobID, false)
	if err != nil {
		return nil, err
	}

	state := job.State()
	if state == JOB_DONE_OK {
		res := job.getResult()
		return res, nil
	} else {
		return nil, &ErrorWrongJobState{State: state}
	}

}

// TODO:
func GetJobErrors(jobID JobID) {}

func PrintStats() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	fmt.Println(len(Model.jobMap), "job(s)")
	jobs := Model.Jobs.Copy()
	for jobs.Len() > 0 {
		// note: this pops in priority + createdtime order
		job := (heap.Pop(jobs)).(*Job)
		jobID := job.ID
		started := job.Started.Format(time.RFC822)
		if job.Started.IsZero() {
			started = "N/A"
		}
		fmt.Println("job", jobID, job.Description, job.State(), "started:", started, "idle:", job.IdleTasks.Len(),
			"running:", len(job.RunningTasks), "done:", len(job.CompletedTasks))
	}
}

/* TODO:
func sanityCheck() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	if len(Model.JobMap) != Model.Jobs.Len() {
		panic("# of entries in JobMap and Jobs don't match")
	}
	jobs := Model.Jobs.Copy()
	for jobs.Len() > 0 {
		// note: this pops in priority + createdtime order
		heapJob := (heap.Pop(jobs)).(*Job)
		mapJob, found := Model.JobMap[heapJob.ID]
		if !found {
			panic("job found in heap but not map")
		}
		if mapJob != heapJob {
			panic("heap job != map job")
		}
		// TODO: lots of other checks to do
	}
}
*/

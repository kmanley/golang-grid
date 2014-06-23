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
	JobMap  JobMap
	Workers WorkerMap
}

// this is only here to support unit tests
func resetModel() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.JobMap = make(JobMap)
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
	Model.JobMap[jobID] = newJob
	heap.Push(&(Model.Jobs), newJob)
	fmt.Println("created job", newJob.ID)
	return newJob.ID, nil
}

func SetJobPriority(jobID JobID, priority int8) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, exists := Model.JobMap[jobID]
	if !exists {
		return ERR_INVALID_JOB_ID
	}
	// update the priority heap
	Model.Jobs.ChangePriority(job, priority)
	return nil
}

func SetJobMaxConcurrency(jobID JobID, maxcon uint32) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, exists := Model.JobMap[jobID]
	if !exists {
		return ERR_INVALID_JOB_ID
	}
	job.setMaxConcurrency(maxcon)
	return nil
}

func SuspendJob(jobID JobID, graceful bool) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, exists := Model.JobMap[jobID]
	if !exists {
		return ERR_INVALID_JOB_ID
	}
	job.suspend(graceful)
	return nil
}

func CancelJob(jobID JobID) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, exists := Model.JobMap[jobID]
	if !exists {
		return ERR_INVALID_JOB_ID
	}
	job.cancel()
	return nil
}

func RetryJob(jobID JobID) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	job, exists := Model.JobMap[jobID]
	if !exists {
		return ERR_INVALID_JOB_ID
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
		state := job.State()
		if !(state == JOB_RUNNING || state == JOB_WAITING) {
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
	job, exists := Model.JobMap[jobID]
	if !exists {
		// TODO: log a warning
		return
	}
	job.reallocateWorkerTask(worker)
}

func GetTaskForWorker(workerName string) (task *Task) {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	worker := getOrCreateWorker(workerName)
	if worker.IsWorking() {
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

	job, found := Model.JobMap[jobID]
	if !found {
		// TODO: logging
		return nil
	}

	task := job.getRunningTask(taskSeq)
	if task == nil {
		// TODO: logging
		return nil
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

	job, found := Model.JobMap[jobID]
	if !found {
		// TODO: logging
		return ERR_INVALID_JOB_ID
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

func PrintStats() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	fmt.Println(len(Model.JobMap), "job(s)")
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

/*
TODO: look into using RLock/RWLock instead of mutex in all cases, this will
allow multiple readers/single writer.

jobs are in memory from time they are created until job.IsInFinalState()
jobs that are in final state are purged from in-memory store after X minutes
getJob(memOnly=False) will transparently pull a job that is on disk (not in mem) into mem
there are only a few places where getJob(id, memOnly=False) should be used, e.g. retryJob

jobs and tasks will enqueue state changes to db persister channel

worker stuff is never persisted to db

need another set of apis for pulling bulk data from db for gui

check group about how to serialize error over json, or just serialize as string
make our own error type that can serialize itself to json?

// TODO: get basic distributor/worker/client setup and demonstrate
// job running end to end
// next, add web ui
// next, add db persistence

job errors - do we want to store this in the job or note it by setting error on a task?
  e.g. job timeout

TODO: background goroutine which
 - checks for job timeout (cancel or suspend in this case?)
 - checks for task timeout - in this case set error for the task; the job could continue if continueontaskerror is true
 - check for client abandoned
 - check for hung workers and reallocate tasks

*/

package grid

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type model struct {
	Mutex               sync.Mutex
	Fifo                bool
	Jobs                JobHeap
	jobMap              JobMap
	Workers             WorkerMap
	prng                *rand.Rand
	lastJobID           JobID
	HUNG_WORKER_TIMEOUT time.Duration
	ShutdownFlag        chan bool
}

// this is only here to support unit tests
func resetModel() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.jobMap = make(JobMap)
	Model.Jobs = Model.Jobs[0:0]
	Model.Workers = make(WorkerMap)
	Model.prng = rand.New(rand.NewSource(time.Now().Unix()))
	Model.lastJobID = ""
	Model.HUNG_WORKER_TIMEOUT = 30 * time.Second
	Model.ShutdownFlag = make(chan bool)
}

func init() {
	resetModel()
}

const JOBID_FORMAT = "060102150405.999999"

var Model = &model{} // TODO: lowercase

// TODO: replace with guid? add distributor ID prefix? add random suffix?
// the problem with the existing
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
		if newID != Model.lastJobID {
			break
		} else {
			fmt.Println("*** paused creating new job ID; clock resolution lower than expected") // TODO: log warning
		}
	}
	Model.lastJobID = newID
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
	jobID = jobDef.ID
	if jobID == "" {
		jobID = newJobID()
	}

	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	// TODO: make sure jobID isn't already in jobMap; this can happen if user specifies their
	// own jobid and would cause chaos

	newJob, err := NewJob(jobID, jobDef.Cmd, jobDef.Description, (jobDef.Data).([]interface{}),
		jobDef.Ctx, jobDef.Ctrl)
	if err != nil {
		return "", err
	}

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

func CancelJob(jobID JobID, reason string) error {
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
	job.cancel(reason)
	return nil
}

// TODO: confirm cancelled job can't be retried but suspended job can
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
		return candidates[Model.prng.Intn(len(candidates))]
	}
}

// NOTE: caller must hold mutex
func getOrCreateWorker(workerName string) *Worker {
	worker, found := Model.Workers[workerName]
	if !found {
		worker = NewWorker(workerName)
		Model.Workers[workerName] = worker
	}
	// NOTE: getOrCreateWorker is only called in functions that
	// are called by workers, so we update the worker's
	// last contact time in this central place
	worker.updateLastContact()
	return worker
}

func forgetWorker(workerName string) {
	delete(Model.Workers, workerName)
}

func reallocateWorkerTask(worker *Worker) {
	jobID := worker.CurrJob
	job, err := getJob(jobID, true)
	if err != nil {
		return // TODO: log
	}
	fmt.Println(fmt.Sprintf("reallocating %s task %s %d", worker, worker.CurrJob, worker.CurrTask)) // TODO: proper logging
	job.reallocateWorkerTask(worker)
}

func GetWorkerTask(workerName string) *WorkerTask {
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
		return nil
	}
	task := job.allocateTask(worker)
	ret := &WorkerTask{job.ID, task.Seq, job.Cmd, task.Indata, &job.Ctx}
	return ret
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

	// TODO: much of this function is copied from SetTaskDone, refactor
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
		return ERR_WORKER_OUT_OF_SYNC
	}

	job, err := getJob(jobID, true)
	if err != nil {
		return err
	}

	// Note we don't have to check the job state; only that the Task is
	// still in the jobs runmap. For example job could be suspended; if it were suspended
	// gracefully, the task will still be in the runmap, if it were graceless, the Task
	// would have already been moved to the idlemap
	task := job.getRunningTask(taskSeq)
	if task == nil {
		// TODO: logging
		return ERR_TASK_NOT_RUNNING
	}

	err = job.timedOut()
	if err != nil {
		// TODO: worker should call setTaskDone in response to this, with this same error.
		// That's better than us calling setTaskDone here, since it gives us a chance to
		// capture stdout/stderr and possibly diagnose why the task timed out. We rely on
		// the worker to honor this return code.
		return err
	}

	err = job.taskTimedOut(task)
	if err != nil {
		// TODO: worker should call setTaskDone in response to this, with this same error.
		// That's better than us calling setTaskDone here, since it gives us a chance to
		// capture stdout/stderr and possibly diagnose why the task timed out. We rely on
		// the worker to honor this return code.
		return err
	}

	//if job.clientTimedOut()

	// If there is a higher priority job with idle tasks, then we might need to kill this
	// lower priority task to allow the higher priority task to make progress
	job2 := getJobForWorker(workerName)
	if (job2 != nil) && (job2.Ctrl.JobPriority > job.Ctrl.JobPriority) && (job2.numIdleTasks() > 0) {
		// there is a higher priority task available with idle tasks, so this task
		// should be preempted. However, if there's another running task for this
		// job that has been running for less time, then spare this task and preempt
		// the other (it will be preempted when *its* worker calls checkJobStatus)
		task2 := job.getShortestRunningTask()
		if task2 == task {
			job.reallocateRunningTask(task)
			return ERR_TASK_PREEMPTED_PRIORITY
		}
	}

	// Someone may have modified this job's max concurrency via GUI or API such that
	// it is currently running with too high a concurrency. If so, throttle it back
	// by reallocating this task. Here we don't discern between tasks that have been running
	// a long vs. short time. That would be difficult to do and probably not worth the effort.
	if (job.getMaxConcurrency() > 0) && (uint32(job.numRunningTasks()) > job.getMaxConcurrency()) {
		job.reallocateRunningTask(task)
		return ERR_TASK_PREEMPTED_CONCURRENCY
	}

	// No error - worker should continue working on this task
	return nil
}

func GetJobResult(jobID JobID) ([]interface{}, error) {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()

	job, err := getJob(jobID, false)
	if err != nil {
		return nil, err
	}

	state := job.State()
	switch state {
	case JOB_WAITING, JOB_RUNNING, JOB_SUSPENDED:
		return nil, &JobNotFinished{State: state, PctComplete: job.percentComplete()}
	case JOB_CANCELLED, JOB_DONE_ERR:
		// TODO: support GetJobErrors() for the error case
		return nil, &ErrorJobFailed{State: state, Reason: job.getFailureReason()}
	default:
		res := job.getResult()
		return res, nil
	}
}

// TODO: caller must hold mutex
func getHungWorkers() []*Worker {
	ret := make([]*Worker, 0, int(math.Max(1.0, float64(len(Model.Workers)/10))))
	for _, worker := range Model.Workers {
		if worker.elapsedSinceLastContact() > Model.HUNG_WORKER_TIMEOUT {
			ret = append(ret, worker)
		}
	}
	return ret
}

func ReallocateHungTasks() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	hungWorkers := getHungWorkers()
	for i := range hungWorkers {
		hungWorker := hungWorkers[i]
		fmt.Println("hung worker", hungWorker) // TODO: proper logging
		reallocateWorkerTask(hungWorker)
		forgetWorker(hungWorker.Name)
	}
}

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

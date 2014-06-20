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
		if (job.Ctrl.MaxConcurrency > 0) && (uint32(len(job.RunningTasks)) >= job.Ctrl.MaxConcurrency) {
			// job is configured with a max concurrency and that has been reached
			continue
		}
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

func GetTaskForWorker(workerName string) (task *Task) {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	worker := getOrCreateWorker(workerName)
	if worker.IsWorking() {
		// TODO: if worker already has a task according to our records, put that Task back on idle heap
	}
	job := getJobForWorker(workerName)
	if job == nil {
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
	if worker.CurrJob != jobID || worker.CurrTask != taskSeq {
		// TODO: our data is the truth, so log & reject
		fmt.Println("worker out of sync", jobID, taskSeq, worker.CurrJob, worker.CurrTask)
		return ERR_WORKER_OUT_OF_SYNC
	}

	job, found := Model.JobMap[jobID]
	if !found {
		// TODO: log and reject
		return ERR_INVALID_JOB_ID
	}

	task := job.getRunningTask(taskSeq)
	if task == nil {
		return ERR_TASK_NOT_RUNNING
	}

	// TODO: need to update the worker to mark it as no longer working on this task
	job.setTaskDone(task, result, stdout, stderr, taskError)

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

package grid

import (
	"container/heap"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"time"
)

type model struct {
	Mutex   sync.Mutex
	Fifo    bool
	JobMap  map[JobID]*Job
	JobHeap JobHeap
	Workers WorkerMap
}

func init() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.JobMap = make(map[JobID]*Job)
	Model.Workers = make(WorkerMap)
}

var Model = &model{}
var prng = rand.New(rand.NewSource(time.Now().Unix()))

func CreateJob(jobDef *JobDefinition) (jobID JobID, err error) {
	// TODO: handle AssignSingleTaskPerWorker
	jobDef.Ctrl.CompiledWorkerNameRegex, err = regexp.Compile(jobDef.Ctrl.WorkerNameRegex)
	if err != nil {
		return 0, err // TODO: wrap error
	}

	jobID = JobID(100) // TODO:
	newJob := &Job{ID: jobID, Cmd: jobDef.Cmd, Description: jobDef.Description, Ctx: *jobDef.Ctx,
		Ctrl: *jobDef.Ctrl, Created: time.Now()}

	data := (jobDef.Data).([]interface{}) // TODO: use reflection to verify correct time, return err
	newJob.IdleTasks = make(TaskList, len(data))
	newJob.RunningTasks = make(TaskMap)
	newJob.CompletedTasks = make(TaskMap)
	for i, taskData := range data {
		newJob.IdleTasks[i] = &Task{Job: jobID, Seq: i, Indata: taskData}
	}

	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.JobMap[jobID] = newJob
	heap.Push(&(Model.JobHeap), newJob)
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
	Model.JobHeap.ChangePriority(job, priority)
	return nil
}

// NOTE: caller must hold mutex
func getJobForWorker(workerName string) (job *Job) {
	now := time.Now()
	candidates := make([]*Job, 0, Model.JobHeap.Len())
	jobs := Model.JobHeap.Copy()
	for jobs.Len() > 0 {
		// note: this pops in priority + createdtime order
		job = (heap.Pop(jobs)).(*Job)

		// TODO: consider best order of the following clauses for performance
		// TODO: add support for hidden workers?
		if job.Ctrl.StartTime.After(now) {
			// job not scheduled to start yet
			continue
		}
		if len(job.IdleTasks) < 1 {
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
func getWorker(workerName string) *Worker {
	worker, found := Model.Workers[workerName]
	if !found {
		worker = &Worker{Name: workerName}
		Model.Workers[workerName] = worker
	}
	return worker
}

func GetTaskForWorker(workerName string) (task *Task) {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	now := time.Now()
	worker := getWorker(workerName)
	if worker.CurrJob > 0 { // TODO: depends on type of JobID
		// TODO: if worker already has a task according to our records, reassign that Task
	}
	job := getJobForWorker(workerName)
	if job == nil || len(job.IdleTasks) < 1 {
		return
	}
	task = job.IdleTasks[0]
	job.IdleTasks = job.IdleTasks[1:]
	job.RunningTasks[task.Seq] = task
	task.Started = now
	task.Worker = workerName
	worker.CurrJob = job.ID
	worker.CurrTask = task.Seq
	if job.Started.IsZero() {
		job.Started = now
	}
	job.Status = JOB_WORKING
	return
}

func SetTaskDone(workerName string, jobID JobID, taskSeq int, result interface{},
	stdout string, stderr string) error {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	now := time.Now()

	worker := getWorker(workerName)
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

	task, found := job.RunningTasks[taskSeq]
	if !found {
		return ERR_TASK_NOT_RUNNING
	}

	task.Finished = now
	task.Outdata = result
	task.Stdout = stdout
	task.Stderr = stderr

	delete(job.RunningTasks, taskSeq)
	job.CompletedTasks[taskSeq] = task

	// is Job complete?
	if len(job.IdleTasks) == 0 && len(job.RunningTasks) == 0 {
		job.Finished = now
		if job.NumErrors > 0 {
			job.Status = JOB_DONE_ERR
		} else {
			job.Status = JOB_DONE_OK
		}
	}
	return nil
}

func PrintStats() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	fmt.Println(len(Model.JobMap), "job(s)")
	for jobID, job := range Model.JobMap {
		started := job.Started.Format(time.RubyDate)
		if job.Started.IsZero() {
			started = "N/A"
		}
		fmt.Println("job", jobID, job.State(), "started:", started, "idle:", len(job.IdleTasks),
			"running:", len(job.RunningTasks), "done:", len(job.CompletedTasks))
	}
}

/*
func sanityCheck() {
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	if len(Model.JobMap) != len(Model.JobHeap) {
		panic("# of entries in JobMap and JobHeap don't match")
	}
	jobs := Model.JobHeap.Copy()
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

	}
}
*/

package grid

import (
	"container/heap"
	"fmt"
	"regexp"
	"time"
)

type Context map[string]string

type JobID string

type JobControl struct {
	MaxConcurrency          uint32
	StartTime               time.Time
	ContinueJobOnTaskError  bool
	RemoteCurDir            string
	WorkerNameRegex         string
	CompiledWorkerNameRegex *regexp.Regexp
	// TODO: consider OSRegex as well, to limit to Workers matching a particular OS/version
	ProcessPriority int
	// TODO: later
	//AssignSingleTaskPerWorker bool
	//TaskWorkerAssignment      map[string][]uint32
	JobPriority          int8 // higher value means higher priority
	JobTimeout           uint32
	TaskTimeout          uint32
	TaskSeemsHungTimeout uint32
	AbandonedJobTimeout  uint32
	MaxTaskReallocations uint8
}

type JobDefinition struct {
	ID          JobID // not normally used; if specified then we don't generate an ID
	Cmd         string
	Data        interface{} // TODO: can this be []interface{} instead? or was there a json/gob marshaling issue?
	Description string
	Ctx         *Context
	Ctrl        *JobControl
}

func (this *JobDefinition) String() string {
	return fmt.Sprintf("%s, %v, %s", this.Cmd, this.Data, this.Description) + fmt.Sprintf("%+v", *this.Ctx) +
		fmt.Sprintf("%+v", *this.Ctrl)
}

const (
	JOB_WAITING = iota
	JOB_RUNNING
	JOB_SUSPENDED
	JOB_CANCELLED // same as suspended but can't be retried/resumed
	JOB_DONE_OK
	JOB_DONE_ERR
)

var JOB_STATES []string = []string{
	"JOB_WAITING",
	"JOB_RUNNING",
	"JOB_SUSPENDED",
	"JOB_CANCELLED",
	"JOB_DONE_OK",
	"JOB_DONE_ERR"}

type Job struct {
	ID          JobID
	Cmd         string
	Description string
	Ctrl        JobControl
	Ctx         Context
	//Status         int
	Created        time.Time
	Started        time.Time
	Suspended      time.Time
	Retried        time.Time // note: retried or resumed
	Cancelled      time.Time
	Finished       time.Time
	LastClientPoll time.Time
	NumTasks       int
	IdleTasks      TaskHeap
	RunningTasks   TaskMap
	CompletedTasks TaskMap
	HeapIndex      int
	NumErrors      int
}

type JobMap map[JobID]*Job

func NewJob(jobID JobID, cmd string, description string, data []interface{}, ctx *Context,
	ctrl *JobControl) *Job {
	now := time.Now()
	if ctx == nil {
		ctx = &Context{}
	}
	if ctrl == nil {
		ctrl = &JobControl{}
	}

	newJob := &Job{ID: jobID, Cmd: cmd, Description: description, Ctx: *ctx,
		Ctrl: *ctrl, Created: now}

	newJob.NumTasks = len(data)
	newJob.RunningTasks = make(TaskMap)
	newJob.CompletedTasks = make(TaskMap)
	for i, taskData := range data {
		heap.Push(&(newJob.IdleTasks), NewTask(jobID, i, taskData))
	}
	return newJob
}

func (this *Job) allocateTask(worker *Worker) *Task {
	if this.IdleTasks.Len() < 1 {
		return nil
	}
	now := time.Now()
	task := heap.Pop(&(this.IdleTasks)).(*Task)
	// TODO: replace all calls to task.Seq to a getter func
	this.RunningTasks[task.Seq] = task
	task.start(worker)
	if this.Started.IsZero() {
		this.Started = now
	}
	worker.assignTask(task)
	return task
}

func (this *Job) numIdleTasks() int {
	return this.IdleTasks.Len()
}

func (this *Job) numRunningTasks() int {
	return len(this.RunningTasks)
}

func (this *Job) getRunningTask(seq int) *Task {
	task, found := this.RunningTasks[seq]
	if !found {
		return nil
	}
	return task
}

func (this *Job) setTaskDone(worker *Worker, task *Task, result interface{},
	stdout string, stderr string, err error) {
	//if _, found := this.RunningTasks[task.Seq]; !found {
	//	// model must check task is running before calling job.setTaskDone
	//	panic(fmt.Sprintf("task %d not running in job %s", task.Seq, this.ID))
	//}
	now := time.Now()
	task.finish(result, stdout, stderr, err)
	this.CompletedTasks[task.Seq] = task
	delete(this.RunningTasks, task.Seq)
	if err != nil {
		this.NumErrors += 1
	}
	// if all tasks are completed, or we have an error and the job is setup to not continue on error
	// then mark the job as finished now
	if (len(this.CompletedTasks) == this.NumTasks) || (this.NumErrors > 0 && !this.Ctrl.ContinueJobOnTaskError) {
		this.Finished = now
	}
	worker.reset()
}

func (this *Job) suspend(graceful bool) {
	this.Suspended = time.Now()
	// in a graceful suspend, any running tasks are allowed to continue to run
	// in a graceless suspend, any running tasks will be terminated
	if !graceful {
		this._stopRunningTasks()
	}
}

func (this *Job) getMaxConcurrency() uint32 {
	return this.Ctrl.MaxConcurrency
}

func (this *Job) setMaxConcurrency(maxcon uint32) {
	this.Ctrl.MaxConcurrency = maxcon
}

func (this *Job) cancel() {
	this.Cancelled = time.Now()
	this._stopRunningTasks()
}

func (this *Job) _stopRunningTasks() {
	for _, task := range this.RunningTasks {
		this.reallocateRunningTask(task)
	}
}

func (this *Job) reallocateRunningTask(task *Task) {
	task.reset()
	heap.Push(&(this.IdleTasks), task)
	delete(this.RunningTasks, task.Seq)
}

func (this *Job) reallocateCompletedTask(task *Task) {
	task.reset()
	heap.Push(&(this.IdleTasks), task)
	delete(this.CompletedTasks, task.Seq)
}

func (this *Job) reallocateWorkerTask(worker *Worker) {
	task, found := this.RunningTasks[worker.CurrTask]
	if found {
		this.reallocateRunningTask(task)
	}
	worker.reset()
}

// Retries a failed job, or resumes a suspended job. Any tasks that had previously
// failed are retried.
func (this *Job) retry() error {
	// NOTE: resume also retries any tasks that failed prior to the suspend
	state := this.State()
	if !(state == JOB_SUSPENDED || state == JOB_DONE_ERR) {
		return ERR_WRONG_JOB_STATE
	}

	now := time.Now()
	this.Retried = now

	// re-enqueue any failed tasks
	for _, task := range this.CompletedTasks {
		if task.hasError() {
			this.reallocateCompletedTask(task)
		}
	}

	this.NumErrors = 0
	this.Finished = *new(time.Time)
	return nil
}

func (this *Job) State() int {
	if !this.Cancelled.IsZero() {
		return JOB_CANCELLED
	}
	if this.Suspended.After(this.Retried) {
		return JOB_SUSPENDED
	}
	if !this.Finished.IsZero() {
		if this.NumErrors > 0 {
			return JOB_DONE_ERR
		} else {
			return JOB_DONE_OK
		}
	}
	if len(this.RunningTasks) > 0 {
		return JOB_RUNNING
	}
	return JOB_WAITING
}

func (this *Job) StateString() string {
	return JOB_STATES[this.State()]
}

func (this *Job) isWorking() bool {
	state := this.State()
	if state == JOB_WAITING || state == JOB_RUNNING {
		return true
	}
	return false
}

func (this *Job) isFinalState() bool {
	return !this.isWorking()
}

func (this *Job) getResult() []interface{} {
	state := this.State()
	if state == JOB_DONE_OK {
		res := make([]interface{}, this.NumTasks)
		for seq, task := range this.CompletedTasks {
			res[seq] = task.Outdata
		}
		return res
	} else {
		// TODO:
		return nil
	}
}

func (this *Job) getShortestRunningTask() (minTask *Task) {
	now := time.Now()
	minDuration := time.Duration(1<<63 - 1) // start with max duration
	for _, task := range this.RunningTasks {
		elapsed := task.elapsedRunning(now)
		if elapsed < minDuration {
			minTask = task
			minDuration = elapsed
		}
	}
	return
}

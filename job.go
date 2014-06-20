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
	JobPriority          int8
	JobTimeout           uint32
	TaskTimeout          uint32
	TaskSeemsHungTimeout uint32
	AbandonedJobTimeout  uint32
	MaxTaskReallocations uint8
}

type JobDefinition struct {
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

/*
waiting - len(idletasks)==numtasks
working - len(runningtasks) > 0 TODO: but need to consider suspended/resumed, e.g. no suspended or resumed >suspended
suspended - suspended && !resumed TODO: then suspend needs to move from runningtasks->idletasks which will preempt on workers
canceled - suspended && !resumed && finished TODO: ditto
done-ok - len(completedtasks)==numtasks and finished and numerrors==0
done-err -finished and numerrors > 0

*/

const (
	JOB_WAITING = iota
	JOB_RUNNING
	JOB_SUSPENDED
	JOB_CANCELLED // same as suspended but can't be resumed
	JOB_DONE_OK
	JOB_DONE_ERR
)

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
	Resumed        time.Time
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
	this.RunningTasks[task.Seq] = task
	task.start(worker)
	if this.Started.IsZero() {
		this.Started = now
	}
	return task
}

func (this *Job) getRunningTask(seq int) *Task {
	task, found := this.RunningTasks[seq]
	if !found {
		return nil
	}
	return task
}

func (this *Job) setTaskDone(task *Task, result interface{}, stdout string, stderr string, err error) {
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
}

func (this *Job) suspend(graceful bool) {
	now := time.Now()
	this.Suspended = now
	// in a graceful suspend, any running tasks are allowed to continue to run
	// in a graceless suspend, any running tasks will be terminated
	if !graceful {
		for _, task := range this.RunningTasks {
			task.reset()
			heap.Push(&(this.IdleTasks), task)
		}
	}
}

func (this *Job) State() int {
	if !this.Cancelled.IsZero() {
		return JOB_CANCELLED
	}
	if this.Suspended.After(this.Resumed) {
		return JOB_SUSPENDED
	}
	if !this.Finished.IsZero() {
		if this.NumErrors > 0 {
			return JOB_DONE_ERR
		} else {
			return JOB_DONE_OK
		}
	}
	if this.IdleTasks.Len() == this.NumTasks {
		return JOB_WAITING
	}
	if len(this.RunningTasks) > 0 {
		return JOB_RUNNING
	}
	panic("unexpected job state")
}

//func (this *Job) allocateTask()

/*
func (this *Job) State() string {
	switch this.Status {
	default:
		return "idle"
	case JOB_WORKING:
		return "working"
	case JOB_SUSPENDED:
		return "suspended"
	case JOB_CANCELLED:
		return "cancelled"
	case JOB_DONE_OK:
		return "done-ok"
	case JOB_DONE_ERR:
		return "done-err"
	}
}
*/

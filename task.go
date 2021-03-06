// TODO: at some point try to lowercase (not export) as much as possible; don't want to do it yet
// bc not sure of implications for rpc/json serialization
package grid

import (
	_ "fmt"
	"time"
)

const (
	TASK_WAITING = iota
	TASK_RUNNING
	TASK_DONE_OK
	TASK_DONE_ERR
)

var TASK_STATES []string = []string{
	"TASK_WAITING",
	"TASK_RUNNING",
	"TASK_DONE_OK",
	"TASK_DONE_ERR"}

type Task struct {
	Job      JobID
	Seq      int
	Indata   interface{}
	Outdata  interface{}
	Started  time.Time
	Finished time.Time
	Worker   string
	Error    error
	// TODO: later
	//ExcludedWorkers map[string]bool
	Stdout string
	Stderr string
}

type TaskList []*Task
type TaskMap map[int]*Task

func NewTask(jobID JobID, seq int, data interface{}) *Task {
	// placeholder in case we need more initialization logic later
	return &Task{Job: jobID, Seq: seq, Indata: data}
}

func (this *Task) start(worker *Worker) {
	now := time.Now()
	this.Outdata = nil
	this.Started = now
	this.Finished = *new(time.Time)
	this.Worker = worker.Name
	this.Error = nil
	this.Stdout = ""
	this.Stderr = ""
	worker.assignTask(this)
}

func (this *Task) reset() {
	this.Outdata = nil
	this.Started = *new(time.Time)
	this.Finished = *new(time.Time)
	this.Worker = ""
	this.Error = nil
	this.Stdout = ""
	this.Stderr = ""
}

func (this *Task) finish(result interface{}, stdout string, stderr string, err error) {
	now := time.Now()
	this.Outdata = result
	this.Finished = now
	// leave this.Worker alone so there's a record of which worker did the task
	this.Error = err
	this.Stdout = stdout
	this.Stderr = stderr
}

func (this *Task) hasError() bool {
	return this.Error != nil
}

func (this *Task) State() int {
	started, finished := this.Started, this.Finished
	if started.IsZero() {
		return TASK_WAITING
	} else {
		// task has started
		if finished.IsZero() {
			return TASK_RUNNING
		} else {
			if this.hasError() {
				return TASK_DONE_ERR
			} else {
				return TASK_DONE_OK
			}
		}
	}
}

func (this *Task) StateString() string {
	return TASK_STATES[this.State()]
}

// Returns current elapsed time for a running task. To get elapsed time
// for a task that may have completed, use elapsed
func (this *Task) elapsedRunning(now time.Time) time.Duration {
	if !(this.State() == TASK_RUNNING) {
		return 0
	}
	return now.Sub(this.Started)
}

// Returns current elapsed time for a completed task
// Returns 0 for an unstarted or currently running task
func (this *Task) elapsed() time.Duration {
	if this.Finished.IsZero() {
		return 0
	}
	return this.Finished.Sub(this.Started)
}

type WorkerTask struct {
	Job  JobID
	Seq  int
	Cmd  string
	Data interface{}
	Ctx  *Context
}

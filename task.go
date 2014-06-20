// TODO: at some point try to lowercase (not export) as much as possible; don't want to do it yet
// bc not sure of implications for rpc/json serialization
package grid

import (
	_ "fmt"
	"time"
)

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

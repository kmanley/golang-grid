// TODO: at some point try to lowercase (not export) as much as possible; don't want to do it yet
// bc not sure of implications for rpc/json serialization
package grid

import (
	_ "fmt"
	"time"
)

type WorkerStats struct {
	Version   float32
	OSVersion string
	CurrDisk  uint64
	CurrMem   uint64
	CurrCpu   uint8
}

func (this *WorkerStats) reset() {
	this.Version = 0
	this.OSVersion = ""
	this.CurrDisk = 0
	this.CurrMem = 0
	this.CurrCpu = 0
}

type Worker struct {
	Name     string
	CurrJob  JobID
	CurrTask int
	Stats    WorkerStats
	LastPoll time.Time
}

func (this *Worker) IsWorking() bool {
	return len(this.CurrJob) > 0
}

type WorkerMap map[string]*Worker

func NewWorker(name string) *Worker {
	return &Worker{Name: name}
}

func (this *Worker) assignTask(task *Task) {
	now := time.Now()
	this.CurrJob = task.Job
	this.CurrTask = task.Seq
	this.LastPoll = now
}

func (this *Worker) reset() {
	this.CurrJob = ""
	this.CurrTask = 0
	// NOTE: we don't reset stats
	this.LastPoll = *new(time.Time)
}

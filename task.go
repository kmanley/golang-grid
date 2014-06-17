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

func NewTask() *Task {
	// placeholder in case we need more initialization logic later
	return &Task{}
}

type WorkerStats struct {
	Version   float32
	OSVersion string
	CurrDisk  uint64
	CurrMem   uint64
	CurrCpu   uint8
}

type Worker struct {
	Name        string
	CurrJob     JobID
	CurrTask    int
	Stats       WorkerStats
	LastContact time.Time
}

type WorkerMap map[string]*Worker

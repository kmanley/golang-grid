package grid

import (
	_ "fmt"
	"time"
)

type Task struct {
	//job             JobID
	Seq             uint32
	Indata          interface{}
	Outdata         interface{}
	Started         time.Time
	Finished        time.Time
	Worker          string
	ExcludedWorkers map[string]bool
	Stdout          string
	Stderr          string
}

type TaskList []*Task

func NewTask() *Task {
	return &Task{}
}

package grid

import (
	_ "fmt"
	"time"
)

type Task struct {
	job             JobID
	seq             uint32
	indata          interface{}
	outdata         interface{}
	started         time.Time
	finished        time.Time
	worker          string
	excludedWorkers map[string]bool
	stdout          string
	stderr          string
}

type TaskList []*Task

func NewTask() *Task {
	return &Task{}
}

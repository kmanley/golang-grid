package grid

import (
	"fmt"
	"regexp"
	"time"
)

type Context map[string]string

type JobID int // TODO: guid

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
	Data        interface{}
	Description string
	Ctx         *Context
	Ctrl        *JobControl
}

func (this *JobDefinition) String() string {
	return fmt.Sprintf("%s, %v, %s", this.Cmd, this.Data, this.Description) + fmt.Sprintf("%+v", *this.Ctx) +
		fmt.Sprintf("%+v", *this.Ctrl)
}

type Job struct {
	ID             JobID
	Cmd            string
	Description    string
	Ctrl           JobControl
	Ctx            Context
	Created        time.Time
	Started        time.Time
	Finished       time.Time
	LastCheck      time.Time
	IdleTasks      TaskList
	RunningTasks   TaskMap
	CompletedTasks TaskMap
	HeapIndex      int
}

//func NewJob() *Job {
//	return &Job{}
//}

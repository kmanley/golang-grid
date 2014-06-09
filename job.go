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

const (
	JOB_IDLE = iota
	JOB_WORKING
	JOB_SUSPENDED
	JOB_CANCELLED
	JOB_DONE_OK
	JOB_DONE_ERR
)

type Job struct {
	ID             JobID
	Cmd            string
	Description    string
	Ctrl           JobControl
	Ctx            Context
	Status         int
	Created        time.Time
	Started        time.Time
	Finished       time.Time
	LastCheck      time.Time
	IdleTasks      TaskList
	RunningTasks   TaskMap
	CompletedTasks TaskMap
	HeapIndex      int
	NumErrors      int
}

//func NewJob() *Job {
//	return &Job{}
//}

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

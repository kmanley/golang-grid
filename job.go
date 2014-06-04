package grid

import (
	_ "fmt"
	"regexp"
	"time"
)

type Context map[string]string

type JobID int // TODO: guid

type JobControl struct {
	MaxConcurrency            uint32
	StartTime                 time.Time
	ContinueJobOnTaskError    bool
	RemoteCurDir              string
	WorkerNameRegex           string
	workerNameRegex           regexp.Regexp
	ProcessPriority           int
	AssignSingleTaskPerWorker bool
	TaskWorkerAssignment      map[string][]uint32
	JobPriority               uint8
	JobTimeout                uint32
	TaskTimeout               uint32
	TaskSeemsHungTimeout      uint32
	AbandonedJobTimeout       uint32
	MaxTaskReallocations      uint8
}

type JobDefinition struct {
	Cmd         string
	Description string
	Control     *JobControl
	Context     *Context
}

type Job struct {
	id             JobID
	cmd            string
	description    string
	control        JobControl
	context        Context
	created        time.Time
	started        time.Time
	finished       time.Time
	lastCheck      time.Time
	idleTasks      TaskList
	runningTasks   TaskList
	completedTasks TaskList
}

func NewJob() *Job {
	return &Job{}
}

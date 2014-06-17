package grid

import (
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
	Data        interface{}
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
	JOB_WORKING
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
	Finished       time.Time
	LastClientPoll time.Time
	NumTasks       int
	IdleTasks      TaskList
	RunningTasks   TaskMap
	CompletedTasks TaskMap
	HeapIndex      int
	NumErrors      int
}

type JobMap map[JobID]*Job

func NewJob() *Job {
	// placeholder in case we need more initialization logic later
	return &Job{}
}

func (this *Job) State() int {
	if len(this.IdleTasks) == 0 && len(this.RunningTasks) == 0 {
		if this.NumErrors > 0 {
			return JOB_DONE_ERR
		} else {
			return JOB_DONE_OK
		}
	}
	return JOB_WAITING // TODO:

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

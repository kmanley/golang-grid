package grid

import (
	"errors"
	"fmt"
)

// TODO: create custom error struct to include extra data
var ERR_INVALID_JOB_ID = errors.New("invalid job id")
var ERR_WORKER_OUT_OF_SYNC = errors.New("worker is out of sync")
var ERR_TASK_NOT_RUNNING = errors.New("task is not running")
var ERR_WRONG_JOB_STATE = errors.New("job is in wrong state for requested action")
var ERR_TASK_PREEMPTED_PRIORITY = errors.New("task preempted by a higher priority task")
var ERR_TASK_PREEMPTED_CONCURRENCY = errors.New("task preempted to maintain max concurrency")

type ErrorWrongJobState struct {
	State int
}

func (this *ErrorWrongJobState) Error() string {
	return "wrong job state for requested action: " + JOB_STATES[this.State]
}

type JobNotFinished struct {
	State int
}

func (this *JobNotFinished) Error() string {
	return fmt.Sprintf("job is not finished (state %s)", this.State)
}

type ErrorJobFailed struct {
	State int
}

func (this *ErrorJobFailed) Error() string {
	return fmt.Sprintf("job failed (state %s)", this.State)
}

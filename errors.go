package grid

import "errors"

// TODO: create custom error struct to include extra data
var ERR_INVALID_JOB_ID = errors.New("invalid job id")
var ERR_WORKER_OUT_OF_SYNC = errors.New("worker is out of sync")
var ERR_TASK_NOT_RUNNING = errors.New("task is not running")
var ERR_WRONG_JOB_STATE = errors.New("job is in wrong state for requested action")

type ErrorWrongJobState struct {
	State int
}

func (this *ErrorWrongJobState) Error() string {
	return "wrong job state for requested action: " + JOB_STATES[this.State]
}

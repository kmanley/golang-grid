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
	State       int
	PctComplete float32
}

func (this *JobNotFinished) Error() string {
	return fmt.Sprintf("job is not finished (state %s, %.2f%% complete)",
		JOB_STATES[this.State], this.PctComplete)
}

type ErrorJobFailed struct {
	State  int
	Reason string
}

func (this *ErrorJobFailed) Error() string {
	return fmt.Sprintf("job failed (state %s): %s", JOB_STATES[this.State], this.Reason)
}

type ErrorJobTimedOut struct {
	Timeout float64
	Elapsed float64
}

func (this *ErrorJobTimedOut) Error() string {
	return fmt.Sprintf("job timed out after %.1f secs (actual=%.1f secs)", this.Timeout, this.Elapsed)
}

type ErrorTaskTimedOut struct {
	Timeout float64
	Elapsed float64
}

func (this *ErrorTaskTimedOut) Error() string {
	return fmt.Sprintf("task timed out after %.1f secs (actual=%.1f secs)", this.Timeout, this.Elapsed)
}

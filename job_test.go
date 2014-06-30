package grid

import (
	"errors"
	_ "fmt"
	"runtime"
	"testing"
)

func assertTrue(t *testing.T, cond bool, fmt string, items ...interface{}) {
	if !cond {
		if fmt == "" {
			fmt = "expected True, got False"
		}
		buf := make([]byte, 32768)
		runtime.Stack(buf, false)
		fmt = fmt + "\n" + string(buf)
		t.Errorf(fmt, items...)
	}
}

func assertJobState(t *testing.T, job *Job, state int) {
	assertTrue(t, job.State() == state, "expected state %s, got %s", JOB_STATES[state], job.StateString())
}

func TestJobStructOK(t *testing.T) {
	job := NewJob("123", "foobar.exe", "test job", []interface{}{1, 2}, nil, nil)
	assertJobState(t, job, JOB_WAITING)
	w1 := NewWorker("worker1")
	w2 := NewWorker("worker2")
	t1 := job.allocateTask(w1)
	assertJobState(t, job, JOB_RUNNING)
	t2 := job.allocateTask(w2)
	assertJobState(t, job, JOB_RUNNING)
	assertTrue(t, t1 == job.getRunningTask(0), "")
	assertTrue(t, t2 == job.getRunningTask(1), "")
	job.setTaskDone(w1, t1, 10, "", "", nil)
	assertJobState(t, job, JOB_RUNNING)
	job.setTaskDone(w2, t2, 20, "", "", nil)
	assertJobState(t, job, JOB_DONE_OK)
}

func TestJobStructLastErr(t *testing.T) {
	job := NewJob("123", "foobar.exe", "test job", []interface{}{1, 2}, nil, nil)
	assertJobState(t, job, JOB_WAITING)
	w1 := NewWorker("worker1")
	w2 := NewWorker("worker2")
	t1 := job.allocateTask(w1)
	assertJobState(t, job, JOB_RUNNING)
	t2 := job.allocateTask(w2)
	assertJobState(t, job, JOB_RUNNING)
	assertTrue(t, t1 == job.getRunningTask(0), "")
	assertTrue(t, t2 == job.getRunningTask(1), "")
	job.setTaskDone(w1, t1, 10, "", "", nil)
	assertJobState(t, job, JOB_RUNNING)
	job.setTaskDone(w2, t2, 20, "", "", errors.New("something went wrong"))
	assertJobState(t, job, JOB_DONE_ERR)
}

func TestJobStructFirstErrNoContinue(t *testing.T) {
	job := NewJob("123", "foobar.exe", "test job", []interface{}{1, 2}, nil, nil)
	assertJobState(t, job, JOB_WAITING)
	w1 := NewWorker("worker1")
	w2 := NewWorker("worker2")
	t1 := job.allocateTask(w1)
	assertJobState(t, job, JOB_RUNNING)
	t2 := job.allocateTask(w2)
	assertJobState(t, job, JOB_RUNNING)
	assertTrue(t, t1 == job.getRunningTask(0), "")
	assertTrue(t, t2 == job.getRunningTask(1), "")
	job.setTaskDone(w1, t1, 10, "", "", errors.New("something went wrong"))
	assertJobState(t, job, JOB_DONE_ERR)
	job.setTaskDone(w2, t2, 20, "", "", nil)
	assertJobState(t, job, JOB_DONE_ERR)
}

func TestJobStructFirstErrContinue(t *testing.T) {
	ctrl := JobControl{ContinueJobOnTaskError: true}
	job := NewJob("123", "foobar.exe", "test job", []interface{}{1, 2}, nil, &ctrl)
	assertJobState(t, job, JOB_WAITING)
	w1 := NewWorker("worker1")
	w2 := NewWorker("worker2")
	t1 := job.allocateTask(w1)
	assertJobState(t, job, JOB_RUNNING)
	t2 := job.allocateTask(w2)
	assertJobState(t, job, JOB_RUNNING)
	assertTrue(t, t1 == job.getRunningTask(0), "")
	assertTrue(t, t2 == job.getRunningTask(1), "")
	job.setTaskDone(w1, t1, 10, "", "", errors.New("something went wrong"))
	assertJobState(t, job, JOB_RUNNING)
	job.setTaskDone(w2, t2, 20, "", "", nil)
	assertJobState(t, job, JOB_DONE_ERR)
}

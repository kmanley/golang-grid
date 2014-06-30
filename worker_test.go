package grid

import (
	_ "fmt"
	"testing"
)

func TestWorkerStruct(t *testing.T) {
	task := NewTask("job1", 0, 100)
	worker := NewWorker("worker1")
	assertTrue(t, worker.IsWorking() == false, "")

	worker.assignTask(task)
	assertTrue(t, worker.IsWorking() == true, "")
	assertTrue(t, worker.CurrJob == task.Job, "")
	assertTrue(t, worker.CurrTask == task.Seq, "")

	worker.reset()
	assertTrue(t, worker.IsWorking() == false, "")
	assertTrue(t, worker.CurrJob == "", "")
	assertTrue(t, worker.CurrTask == 0, "")
}

package grid

import (
	"fmt"
	"testing"
)

func TestTaskStruct(t *testing.T) {
	task := NewTask("job1", 0, 100)
	assertTrue(t, task.Started.IsZero(), "")

	w := NewWorker("worker1")
	task.start(w)
	assertTrue(t, !task.Started.IsZero(), "")
	assertTrue(t, task.Finished.IsZero(), "")

	task.finish(1000, "stdout", "stderr", nil)
	assertTrue(t, !task.Started.IsZero(), "")
	assertTrue(t, !task.Finished.IsZero(), "")
	assertTrue(t, task.Stdout == "stdout", "")
	assertTrue(t, task.Stderr == "stderr", "")
	assertTrue(t, task.Error == nil, "")
	assertTrue(t, task.hasError() == false, "")

	task.reset()
	assertTrue(t, task.Started.IsZero(), "")
	assertTrue(t, task.Finished.IsZero(), "")
	assertTrue(t, task.Stdout == "", "")
	assertTrue(t, task.Stderr == "", "")
	assertTrue(t, task.Error == nil, "")
	assertTrue(t, task.hasError() == false, "")

	task.start(w)
	task.finish(1000, "stdout", "stderr", fmt.Errorf("something went wrong"))
	assertTrue(t, task.Error != nil, "")
	assertTrue(t, task.hasError() == true, "")

}

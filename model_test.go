package grid

/* test case TODO




*/

import (
	"fmt"
	//	"github.com/kmanley/golang-grid"
	"testing"
)

func TestCreateJobs(t *testing.T) {
	resetModel()
	data := []interface{}{1, 3, 5, 7, 9}
	ctx := &Context{"foo": "bar"}
	ctrl := &JobControl{MaxConcurrency: 20}
	const COUNT = 5
	for i := 0; i < COUNT; i++ {
		CreateJob(&JobDefinition{Cmd: "python.exe doit.py", Data: data,
			Description: fmt.Sprintf("job %d", i), Ctx: ctx,
			Ctrl: ctrl})
	}
	PrintStats()
	//sanityCheck()
	for i := 0; i < COUNT; i++ {

	}
}

func TestSimple(t *testing.T) {
	resetModel()
	jobID, _ := CreateJob(&JobDefinition{Cmd: "python.exe doit.py", Data: []interface{}{1, 3, 5, 7, 9},
		Description: "my first job", Ctx: &Context{"foo": "bar"},
		Ctrl: &JobControl{MaxConcurrency: 20}})
	PrintStats()
	t1 := GetWorkerTask("worker1")
	t2 := GetWorkerTask("worker2")
	PrintStats()
	//fmt.Println(t1, t2)
	t3 := GetWorkerTask("worker3")
	t4 := GetWorkerTask("worker4")
	PrintStats()
	t5 := GetWorkerTask("worker5")
	PrintStats()

	//fmt.Println(t1, t2, t3, t4, t5)

	SetTaskDone("worker1", t1.Job, t1.Seq, 10, "", "", nil)
	SetTaskDone("worker2", t2.Job, t2.Seq, 30, "", "", nil)
	SetTaskDone("worker3", t3.Job, t3.Seq, 50, "", "", nil)
	SetTaskDone("worker4", t4.Job, t4.Seq, 70, "", "", nil)
	SetTaskDone("worker5", t5.Job, t5.Seq, 90, "", "", nil)

	PrintStats()

	res, _ := GetJobResult(jobID)
	fmt.Println("RESULT: ", res)
}

package main

import (
	_ "fmt"
	"github.com/kmanley/golang-grid"
	"testing"
)

func TestModel(t *testing.T) {
	grid.CreateJob(&grid.JobDefinition{Cmd: "python.exe doit.py", Data: []interface{}{1, 3, 5, 7, 9},
		Description: "my first job", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 20}})
	grid.PrintStats()
	t1 := grid.GetTaskForWorker("worker1")
	t2 := grid.GetTaskForWorker("worker2")
	grid.PrintStats()
	//fmt.Println(t1, t2)
	t3 := grid.GetTaskForWorker("worker3")
	t4 := grid.GetTaskForWorker("worker4")
	grid.PrintStats()
	t5 := grid.GetTaskForWorker("worker5")
	grid.PrintStats()

	//fmt.Println(t1, t2, t3, t4, t5)

	grid.SetTaskDone("worker1", t1.Job, t1.Seq, 20, "", "")
	grid.SetTaskDone("worker2", t2.Job, t2.Seq, 20, "", "")
	grid.SetTaskDone("worker3", t3.Job, t3.Seq, 20, "", "")
	grid.SetTaskDone("worker4", t4.Job, t4.Seq, 20, "", "")
	grid.SetTaskDone("worker5", t5.Job, t5.Seq, 20, "", "")

	grid.PrintStats()
}

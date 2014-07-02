package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
	"sync"
	"time"
)

var wg sync.WaitGroup

func client() {
	defer wg.Done()
	fmt.Println("client:start")

	jobID, _ := grid.CreateJob(&grid.JobDefinition{Cmd: "times10", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "my first job", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 3}})
	for {
		res, err := grid.GetJobResult(jobID)
		if err == nil {
			fmt.Println("client: job finished", res)
			break
		} else {
			if _, notFinished := err.(*grid.JobNotFinished); notFinished {
				fmt.Println("client: job working")
			} else {
				fmt.Println("clent: job failed")
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println("client:end")
}

func distributor() {

}

func worker(name string) {
	fmt.Println(name, "start")
	defer wg.Done()
START:
	for {
		task := grid.GetTaskForWorker(name)
		if task == nil {
			fmt.Println(name, "no tasks available")
			time.Sleep(3 * time.Second)
			continue
		}
		fmt.Println(name, "got task", task.Seq)

		for i := 0; i < 2; i++ {
			time.Sleep(50 * time.Millisecond)
			err := grid.CheckJobStatus(name, task.Job, task.Seq)
			if err != nil {
				fmt.Println(name, "got error", err)
				goto START
			}
		}

		grid.SetTaskDone(name, task.Job, task.Seq, task.Indata.(int)*10, "", "", nil)
	}
}

func main() {
	wg.Add(1)
	go client()

	NUM_WORKERS := 5
	for i := 0; i < NUM_WORKERS; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("worker%d", i))
	}

	wg.Wait()
}

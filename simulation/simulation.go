package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
	"runtime"
	"sync"
	"time"
)

var wg sync.WaitGroup
var quit = make(chan bool)

func simulate(numWorkers int, jobIDs []grid.JobID) {
	runWorkers(numWorkers)
	fmt.Println("client:start")

	doneMap := make(map[grid.JobID]bool)
	for i := range jobIDs {
		doneMap[jobIDs[i]] = false
	}

	for {
		if len(doneMap) < 1 {
			// no more jobs to check
			break
		}
		for jobID, _ := range doneMap {
			res, err := grid.GetJobResult(jobID)
			if err == nil {
				fmt.Println("client: job", jobID, "finished:", res)
				delete(doneMap, jobID)
			} else {
				if e, notFinished := err.(*grid.JobNotFinished); notFinished {
					fmt.Println("client: job", jobID, e.Error())
				} else {
					fmt.Println("client: job", jobID, "failed", err.Error())
					delete(doneMap, jobID)
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println("client end")
	for i := 0; i < numWorkers; i++ {
		quit <- true
	}
}

func worker(name string) {
	fmt.Println(name, "start")
	defer wg.Done()
	defer fmt.Println(name, "end")
START:
	for {
		task := grid.GetTaskForWorker(name)
		if task == nil {
			fmt.Println(name, "no tasks available")
			select {
			case <-quit:
				return
			default:
			}
			time.Sleep(3 * time.Second)
			continue
		}
		fmt.Println(name, "got task", task.Job, task.Seq)

		// simulate the task taking some time to compute, during which
		// time we occasionally check status
		for i := 0; i < 3; i++ {
			time.Sleep(150 * time.Millisecond)
			err := grid.CheckJobStatus(name, task.Job, task.Seq)
			if err != nil {
				fmt.Println(name, "got check status error", err)
				goto START
			}
		}

		fmt.Println(name, "setting task", task.Job, task.Seq, "done")
		grid.SetTaskDone(name, task.Job, task.Seq, task.Indata.(int)*10, "", "", nil)
	}
}

func runWorkers(count int) {
	for i := 0; i < count; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("worker%d", i))
	}
}

func testBasic() {
	jobID, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "my first job", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	simulate(3, []grid.JobID{jobID})
}

func test2JobsBasic() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	job2, _ := grid.CreateJob(&grid.JobDefinition{ID: "job2", Cmd: "", Data: []interface{}{-1, -2, -3, -4, -5},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	simulate(3, []grid.JobID{job1, job2})
}

func test2JobsWithPriority() {
	// when looking at output the tasks for job2 should all be processed first; however the jobs
	// may complete in any order
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	job2, _ := grid.CreateJob(&grid.JobDefinition{ID: "job2", Cmd: "", Data: []interface{}{-1, -2, -3, -4, -5},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0, JobPriority: 100}})
	simulate(3, []grid.JobID{job1, job2})
}

func test2JobsWithCancel() {
	// when looking at output the tasks for job2 should all be processed first; however the jobs
	// may complete in any order
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	job2, _ := grid.CreateJob(&grid.JobDefinition{ID: "job2", Cmd: "", Data: []interface{}{-1, -2, -3, -4, -5},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0, JobPriority: 100}})
	time.AfterFunc(100*time.Millisecond, func() { fmt.Println("cancelling job ", job1); grid.CancelJob(job1) })
	simulate(3, []grid.JobID{job1, job2})
}

func testJobWithGracefulSuspendResume() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	time.AfterFunc(300*time.Millisecond, func() { fmt.Println("suspending job", job1); grid.SuspendJob(job1, true) })
	time.AfterFunc(3*time.Second, func() { fmt.Println("resuming job", job1); grid.RetryJob(job1) })
	simulate(3, []grid.JobID{job1})
}

func main() {
	numcpu := runtime.NumCPU()
	fmt.Println("setting GOMAXPROCS to", numcpu)
	runtime.GOMAXPROCS(numcpu)

	//testBasic()
	//test2JobsBasic()
	//test2JobsWithPriority()
	//test2JobsWithCancel()
	testJobWithGracefulSuspendResume()

	wg.Wait()
}

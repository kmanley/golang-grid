/* other scenarios to test2JobsBasic()
- abandoned client timeout
- worker killed
- job timeout
- task timeout
- worker can't handle task and asks for realloc - ensure not assigned to same worker(s) again
- worker name regex
- check other Job struct fields for ideas

*/

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
var newjob = make(chan grid.JobID)

func simulate(numWorkers int, jobIDs []grid.JobID) {
	runWorkers(numWorkers)
	fmt.Println("client:start")

	jobMap := make(map[grid.JobID]bool)
	for i := range jobIDs {
		jobMap[jobIDs[i]] = false
	}

	for {
		var newJobID grid.JobID
		select {
		case newJobID = <-newjob:
			fmt.Println("client: got new job", newJobID)
			jobMap[newJobID] = false
		default:
		}

		if len(jobMap) < 1 {
			// no more jobs to check
			break
		}
		for jobID, _ := range jobMap {
			res, err := grid.GetJobResult(jobID)
			if err == nil {
				fmt.Println("client: job", jobID, "finished:", res)
				delete(jobMap, jobID)
			} else {
				if e, notFinished := err.(*grid.JobNotFinished); notFinished {
					fmt.Println("client: job", jobID, e.Error())
				} else {
					fmt.Println("client: job", jobID, "failed", err.Error())
					delete(jobMap, jobID)
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
		select {
		case <-quit:
			return
		default:
		}
		task := grid.GetTaskForWorker(name)
		if task == nil {
			fmt.Println(name, "no tasks available")
			time.Sleep(3 * time.Second)
			continue
		}
		fmt.Println(name, "got task", task.Job, task.Seq)

		// simulate the task taking some time to compute, during which
		// time we occasionally check status
		for i := 0; i < 3; i++ {
			time.Sleep(300 * time.Millisecond)
			err := grid.CheckJobStatus(name, task.Job, task.Seq)
			if err != nil {
				fmt.Println(name, "working on", task.Job, task.Seq, "got check status error", err)
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

func testJobWithGracelessSuspendResume() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	time.AfterFunc(300*time.Millisecond, func() { fmt.Println("suspending job", job1); grid.SuspendJob(job1, false) })
	time.AfterFunc(3*time.Second, func() { fmt.Println("resuming job", job1); grid.RetryJob(job1) })
	simulate(7, []grid.JobID{job1})
}

func testPreemptConcurrency() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	time.AfterFunc(500*time.Millisecond, func() { fmt.Println("changing concurrency to 2", job1); grid.SetJobMaxConcurrency(job1, 2) })
	simulate(5, []grid.JobID{job1})
}

func testPreemptPriority() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{MaxConcurrency: 0}})
	time.AfterFunc(1*time.Second, func() {
		fmt.Println("adding new higher priority job job2")
		job2, _ := grid.CreateJob(&grid.JobDefinition{ID: "job2", Cmd: "", Data: []interface{}{-1, -2, -3, -4, -5, -6},
			Description: "", Ctx: &grid.Context{"foo": "bar"},
			Ctrl: &grid.JobControl{JobPriority: 100}})
		newjob <- job2
	})
	simulate(5, []grid.JobID{job1})
}

func testJobFutureStartTime() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{StartTime: time.Now().Add(2 * time.Second), MaxConcurrency: 0}})
	simulate(5, []grid.JobID{job1})
}

func testJobWithWorkerNameRegex() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "", Data: []interface{}{1, 3, 5, 7, 9, 11, 13, 15, 17},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{WorkerNameRegex: "worker0|worker3", MaxConcurrency: 0}})
	simulate(5, []grid.JobID{job1})
}

func main() {
	numcpu := runtime.NumCPU()
	fmt.Println("setting GOMAXPROCS to", numcpu)
	runtime.GOMAXPROCS(numcpu)

	//testBasic()
	//test2JobsBasic()
	//test2JobsWithPriority()
	//test2JobsWithCancel()
	//testJobWithGracefulSuspendResume()
	//testJobWithGracelessSuspendResume()
	//testChangeConcurrency()
	//testPreemptPriority()
	//testJobFutureStartTime()
	testJobWithWorkerNameRegex()

	wg.Wait()
}

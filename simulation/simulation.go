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
	"errors"
	"fmt"
	"github.com/kmanley/golang-grid"
	"runtime"
	"sync"
	"time"
)

var wg sync.WaitGroup
var distquit = make(chan bool)
var workerquit = make(chan bool)
var newjob = make(chan grid.JobID)

func simulate(numWorkers int, jobIDs []grid.JobID) {
	runWorkers(numWorkers)
	runDistributor()
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
	// make distributor quit; it will take down the workers
	distquit <- true
	wg.Wait()
}

func distributor() {
	fmt.Println("distributor start")
	defer wg.Done()
	defer fmt.Println("distributor end")
	grid.Model.HUNG_WORKER_TIMEOUT = 5 * time.Second
	for {
		select {
		case <-distquit:
			goto END
		default:
			grid.ReallocateHungTasks()
			time.Sleep(2 * time.Second)
		}
	}
END:
	// send a quit signal to all known live workers; this is necessary
	// because some tests simulate a worker crashing
	for i := 0; i < len(grid.Model.Workers); i++ {
		workerquit <- true
	}

}

func worker(name string) {
	fmt.Println(name, "start")
	defer wg.Done()
	defer fmt.Println(name, "end")
START:
	for {
		select {
		case <-workerquit:
			return
		default:
		}
		wtask := grid.GetWorkerTask(name)
		if wtask == nil {
			fmt.Println(name, "no tasks available")
			time.Sleep(3 * time.Second)
			continue
		}
		fmt.Println(name, "got task", wtask.Job, wtask.Seq, "data:", wtask.Data)

		// simulate the task taking some time to compute, during which
		// time we occasionally check status
		for i := 0; i < 5; i++ {
			// make worker1 take longer than the others; this is to help simulate
			// timeouts for the timeout tests
			if name == "worker1" {
				time.Sleep(500 * time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			err := grid.CheckJobStatus(name, wtask.Job, wtask.Seq)
			if err != nil {
				fmt.Println(name, "working on", wtask.Job, wtask.Seq, "got check status error", err)
				switch err.(type) {
				case *grid.ErrorTaskTimedOut, *grid.ErrorJobTimedOut:
					fmt.Println(name, "setting task", wtask.Job, wtask.Seq, "done", nil, err)
					grid.SetTaskDone(name, wtask.Job, wtask.Seq, nil, "stdout data", "stderr data", err)
					goto START
				default:
					goto START
				}
			}
		}

		var res interface{}
		var stderr string
		var err error
		switch wtask.Cmd {
		case "taskerror1":
			if name == "worker1" {
				res = interface{}(nil)
				stderr = "stderr data"
				err = errors.New("task failed!")
			} else {
				res = wtask.Data.(int) * 5
				stderr = "stderr data"
				err = error(nil)
			}
		case "tasksleep1":
			res = wtask.Data.(int) * 3
			stderr = "stderr data"
			err = error(nil)
			if name == "worker1" {
				time.Sleep(5 * time.Second)
			}
		case "testhang":
			res = wtask.Data.(int) * -1
			stderr = "stderr data"
			err = error(nil)
			if name == "worker1" {
				fmt.Println(name, "crashing!")
				return
			}
		default:
			// by default, multiply by 10
			res = wtask.Data.(int) * 10
			stderr = ""
			err = error(nil)
		}
		var serr string
		if err == nil {
			serr = ""
		} else {
			serr = "(" + err.Error() + ")"
		}
		fmt.Println(name, "setting task", wtask.Job, wtask.Seq, "done", res, serr)
		grid.SetTaskDone(name, wtask.Job, wtask.Seq, res, "stdout data", stderr, err)
	}
}

func runWorkers(count int) {
	for i := 0; i < count; i++ {
		wg.Add(1)
		go worker(fmt.Sprintf("worker%d", i))
	}
}

func runDistributor() {
	wg.Add(1)
	go distributor()
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
	time.AfterFunc(100*time.Millisecond, func() { fmt.Println("cancelling job ", job1); grid.CancelJob(job1, "testing") })
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

func testJobWithTaskErrorContinue() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "taskerror1", Data: []interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{ContinueJobOnTaskError: true}})
	simulate(5, []grid.JobID{job1})
}

func testJobWithTaskErrorNoContinue() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "taskerror1", Data: []interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{ContinueJobOnTaskError: false}})
	simulate(5, []grid.JobID{job1})
}

func testJobWithTimeout() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "tasksleep1", Data: []interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{JobTimeout: 1.5}})
	simulate(5, []grid.JobID{job1})
}

func testJobWithTaskTimeout() {
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "tasksleep1", Data: []interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{TaskTimeout: 1.5}})
	simulate(5, []grid.JobID{job1})
}

func testHungWorker() {
	grid.Model.HUNG_WORKER_TIMEOUT = 10 * time.Millisecond
	job1, _ := grid.CreateJob(&grid.JobDefinition{ID: "job1", Cmd: "testhang", Data: []interface{}{1, 2, 3, 4, 5, 6, 7, 8},
		Description: "", Ctx: &grid.Context{"foo": "bar"},
		Ctrl: &grid.JobControl{}})
	simulate(5, []grid.JobID{job1})
}

func main() {
	numcpu := runtime.NumCPU()
	fmt.Println("setting GOMAXPROCS to", numcpu)
	runtime.GOMAXPROCS(numcpu)

	/* TODO: make simulations selectable from command line
	fmt.Println(`
	1. basic job with 1 job
	2. basic test with 2 jobs
	3. two jobs with priority
	`)
	*/

	//testBasic()
	//test2JobsBasic()
	//test2JobsWithPriority()
	//test2JobsWithCancel()
	//testJobWithGracefulSuspendResume()
	//testJobWithGracelessSuspendResume()
	//testChangeConcurrency()
	//testPreemptPriority()
	//testJobFutureStartTime()
	//testJobWithWorkerNameRegex()
	//testJobWithTaskErrorContinue()
	//testJobWithTaskErrorNoContinue()
	//testJobWithTimeout()
	testJobWithTaskTimeout()
	//testHungWorker()
}

package grid

import (
	"container/heap"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"time"
)

type model struct {
	Mutex   sync.Mutex
	Fifo    bool
	JobMap  map[JobID]*Job
	JobHeap JobHeap
}

var Model = &model{}
var prng = rand.New(rand.NewSource(time.Now().Unix()))

func InsertJob(jobDef JobDefinition) (jobID JobID, err error) {
	jobDef.Ctrl.CompiledWorkerNameRegex, err = regexp.Compile(jobDef.Ctrl.WorkerNameRegex)
	if err != nil {
		return 0, err // TODO: wrap error
	}

	jobID = JobID(100) // TODO:
	newJob := &Job{ID: jobID, Cmd: jobDef.Cmd, Description: jobDef.Description, Ctx: *jobDef.Ctx,
		Ctrl: *jobDef.Ctrl, Created: time.Now()}

	data := (jobDef.Data).([]interface{}) // TODO: use reflection to verify correct time, return err
	newJob.IdleTasks = make(TaskList, len(data))
	for i, taskData := range data {
		newJob.IdleTasks[i] = &Task{Seq: uint32(i), Indata: taskData}
	}

	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	Model.JobMap[jobID] = newJob
	heap.Push(&(Model.JobHeap), newJob)
	fmt.Println("created job", newJob.ID)
	return newJob.ID, nil
}

func FindJobForWorker(worker string) (job *Job) {
	now := time.Now()
	Model.Mutex.Lock()
	defer Model.Mutex.Unlock()
	candidates := make([]*Job, 0, Model.JobHeap.Len())
	jobs := Model.JobHeap.Copy()
	for jobs.Len() > 0 {
		// note: this pops in priority + createdtime order
		job = (heap.Pop(jobs)).(*Job)
		if (job.Ctrl.StartTime.Before(now)) &&
			(len(job.IdleTasks) > 0) &&
			(uint32(len(job.RunningTasks)) < job.Ctrl.MaxConcurrency) &&
			(job.Ctrl.CompiledWorkerNameRegex == nil || job.Ctrl.CompiledWorkerNameRegex.MatchString(worker)) &&
			(len(candidates) == 0 || job.Ctrl.JobPriority == candidates[0].Ctrl.JobPriority) {
			candidates = append(candidates, job)
		}
	}
	if len(candidates) < 1 {
		return nil
	}
	if Model.Fifo {
		return candidates[0]
	} else {
		return candidates[prng.Intn(len(candidates))]
	}
}

/*
  jobs = [job for job in self.jobs.itervalues() if job.idletasks \
                    and job.context_dict.get("grid.jobStartTime", now) <= now \
                    and len(job.runningtasks) < job.maxconcurrency \
                    and ((job.regex is None and worker.machine not in self.hiddenWorkers) or \
                         (job.regex and re.match(job.regex, worker.machine)))]
            log.debug("candidate jobs for %s: %s" % (worker, repr(jobs))) */

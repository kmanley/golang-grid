package main

import (
	"container/heap"
	"fmt"
	"github.com/kmanley/golang-grid"
	"testing"
	"time"
)

func TestJobHeap(t *testing.T) {
	jobs := &grid.JobHeap{}
	heap.Init(jobs)

	job1 := grid.Job{Description: "job 1", Created: time.Now()}
	job2 := grid.Job{Description: "job 2", Created: time.Now()} //, Ctrl: grid.JobControl{JobPriority: 100}}
	job3 := grid.Job{Description: "job 3", Created: time.Now()} //, Ctrl: grid.JobControl{JobPriority: 104}}

	heap.Push(jobs, &job2)
	heap.Push(jobs, &job3)
	heap.Push(jobs, &job1)

	for _, job := range *jobs {
		fmt.Println(job)
	}

	fmt.Println("===")
	fmt.Println(jobs.Peek())

	fmt.Println("---")
	ret1 := heap.Pop(jobs)
	ret2 := heap.Pop(jobs)
	ret3 := heap.Pop(jobs)

	fmt.Println(ret1)
	fmt.Println(ret2)
	fmt.Println(ret3)

}

func TestCopy(t *testing.T) {
	job1 := grid.Job{Description: "job 1", Created: time.Now()}
	job2 := grid.Job{Description: "job 2", Created: time.Now()} //, Ctrl: grid.JobControl{JobPriority: 100}}
	job3 := grid.Job{Description: "job 3", Created: time.Now()} //, Ctrl: grid.JobControl{JobPriority: 104}}
	jobs := &grid.JobHeap{}
	heap.Init(jobs)
	heap.Push(jobs, &job1)
	heap.Push(jobs, &job2)
	heap.Push(jobs, &job3)
	fmt.Println(jobs)

	jobsCopy := jobs.Copy()
	fmt.Println(jobsCopy)
	fmt.Println("-------------")
	jobsCopy.Pop()
	jobsCopy.Pop()

	fmt.Println(jobs)
	fmt.Println(jobsCopy)

}

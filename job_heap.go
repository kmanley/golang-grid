package grid

import (
	"container/heap"
	_ "fmt"
)

// A JobHeap is a heap of Jobs ordered by priority then creation time
type JobHeap []*Job

func (this JobHeap) Len() int { return len(this) }

func (this JobHeap) Less(i, j int) bool {
	lhs, rhs := this[i], this[j]
	if lhs.Ctrl.JobPriority > rhs.Ctrl.JobPriority {
		// lhs has higher priority, so is "less" in terms of heap ordering
		return true
	}
	if lhs.Ctrl.JobPriority == rhs.Ctrl.JobPriority {
		if lhs.Created.Before(rhs.Created) {
			return true
		}
	}
	return false
}

func (this JobHeap) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
	this[i].HeapIndex, this[j].HeapIndex = i, j
}

func (this *JobHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	n := this.Len()
	item := x.(*Job)
	item.HeapIndex = n
	*this = append(*this, item)
}

func (this *JobHeap) Pop() interface{} {
	old := *this
	idx := len(old) - 1
	job := old[idx]
	job.HeapIndex = -1 // for safety
	*this = old[:idx]
	return job
}

func (this *JobHeap) Copy() *JobHeap {
	newHeap := make(JobHeap, this.Len())
	copy(newHeap, *this)
	return &newHeap
}

func (this *JobHeap) Peek() *Job {
	return (*this)[0]
}

// update modifies the priority and value of an Item in the queue.
func (this *JobHeap) ChangePriority(job *Job, priority int8) {
	job.Ctrl.JobPriority = priority
	heap.Fix(this, job.HeapIndex)
}

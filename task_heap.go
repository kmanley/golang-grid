package grid

import (
	_ "fmt"
)

// A TaskHeap is a heap of Tasks ordered by sequence number
type TaskHeap []*Task

func (this TaskHeap) Len() int { return len(this) }

func (this TaskHeap) Less(i, j int) bool {
	return this[i].Seq < this[j].Seq
}

func (this TaskHeap) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this *TaskHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	item := x.(*Task)
	*this = append(*this, item)
}

func (this *TaskHeap) Pop() interface{} {
	old := *this
	idx := len(old) - 1
	job := old[idx]
	*this = old[:idx]
	return job
}

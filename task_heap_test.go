package grid

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestTaskHeap(t *testing.T) {
	tasks := &TaskHeap{}
	heap.Init(tasks)

	for i := 20; i >= 0; i-- {
		t := Task{Seq: i}
		heap.Push(tasks, &t)
	}

	for i := 0; i <= 20; i++ {
		fmt.Println(heap.Pop(tasks))
	}
	fmt.Println(tasks.Len())
}

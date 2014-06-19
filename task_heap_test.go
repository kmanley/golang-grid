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
		task := Task{Seq: i}
		heap.Push(tasks, &task)
	}

	for i := 0; i <= 20; i++ {
		task := heap.Pop(tasks).(*Task)
		if task.Seq != i {
			t.Fail()
		}
		fmt.Println(task)
	}
	fmt.Println(tasks.Len())
}

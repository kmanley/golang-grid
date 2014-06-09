package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
)

func test1() {
	g, err := grid.NewClient("127.0.0.1:9999")
	if err != nil {
		fmt.Println("NewClient error", err)
		return
	}
	jobID := g.CreateJob("hello.exe", []int{1, 2, 3})
	fmt.Println("jobID", jobID)
}

func main() {
	test1()
}

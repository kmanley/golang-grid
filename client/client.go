package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
	"net/rpc"
)

func client() {
	c, err := rpc.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Println(err)
		return
	}
	var result grid.JobID
	jobDef := &grid.JobDefinition{Cmd: "hello.exe", Description: "my first grid job"}

	err = c.Call("ClientApi.CreateJob", jobDef, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("success!")
	}
}

func main() {
	client()
}

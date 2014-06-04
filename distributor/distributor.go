package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
	"net"
	"net/rpc"
)

func server() {
	rpc.Register(new(grid.ClientApi))
	ln, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(c)
	}
}

func main() {
	server()
}

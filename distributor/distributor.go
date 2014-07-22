package main

import (
	"fmt"
	"github.com/kmanley/golang-grid"
	//"net"
	//"net/rpc"
	"flag"
	"github.com/golang/glog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var quitchan = make(chan os.Signal, 1)

/*
func server() {
	rpc.Register(new(grid.ClientApi))
	ln, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		c, err := ln.Accept()
		fmt.Println("got Accept") // TODO:
		if err != nil {
			continue
		}
		go rpc.ServeConn(c)
	}
}

func main() {
	server()
}
*/

func dummy() {
	for {
		select {
		case <-quitchan:
			grid.ShutdownClientApi()
			goto END
		default:
			glog.Info("dummy loop")
			time.Sleep(2.0 * time.Second)
		}
	}
END:
	fmt.Println("dummy goroutine exiting")
}

func main() {
	flag.Parse()
	signal.Notify(quitchan, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGTERM)
	go dummy()
	grid.StartClientApi()
	glog.Flush()
}

package grid

import (
	"fmt"
	"net/rpc"
	_ "net/rpc/jsonrpc"
)

type Client struct {
	conn *rpc.Client
}

func NewClient(addr string) (*Client, error) {
	conn, err := rpc.Dial("tcp", addr) // TODO: DialHTTP, jsonrpc.DialHTTP?
	if err != nil {
		fmt.Println("Dial error", err) // TODO:
		return nil, err
	}

	return &Client{conn}, nil
}

func (this *Client) CreateJob(cmd string, data interface{}) JobID {
	return this.CreateJobEx(cmd, data, "", &Context{}, &JobControl{})
}

func (this *Client) CreateJobEx(cmd string, data interface{}, description string,
	ctx *Context, ctrl *JobControl) (jobID JobID) {
	jobDef := &JobDefinition{cmd, data, description, ctx, ctrl}

	err := this.conn.Call("ClientApi.CreateJob", jobDef, &jobID)
	if err != nil {
		fmt.Println("Call error", err)
	} else {
		fmt.Println("success!", jobID)
	}
	return
}

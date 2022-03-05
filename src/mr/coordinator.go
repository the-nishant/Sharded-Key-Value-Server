package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files              []string
	mapTasks           chan int
	successMapTasks    []bool
	successMapCount    int
	reduceTasks        chan int
	successReduceTasks []bool
	successReduceCount int
	nReduce            int
	mu                 sync.Mutex
	done               bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	task, ok := <-c.mapTasks
	if !ok {
		task, ok := <-c.reduceTasks
		if !ok {
			reply.Type = -1
			return nil
		}
		reply.Index = task
		reply.Other = len(c.files)
		reply.Type = 1
		go c.WaitSuccess(reply)
		return nil
	}
	reply.Index = task
	reply.Other = c.nReduce
	reply.Type = 0
	reply.Filename = c.files[task]
	go c.WaitSuccess(reply)
	return nil
}

func (c *Coordinator) WaitSuccess(reply *TaskReply) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if reply.Type == 0 && !c.successMapTasks[reply.Index] {
		c.mapTasks <- reply.Index
	} else if reply.Type == 1 && !c.successReduceTasks[reply.Index] {
		c.reduceTasks <- reply.Index
	}
}

func (c *Coordinator) Success(args *SuccessArgs, reply *SuccessReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Type == 0 && !c.successMapTasks[args.Index] {
		c.successMapTasks[args.Index] = true
		c.successMapCount += 1
		if c.successMapCount == len(c.files) {
			close(c.mapTasks)
		}
	} else if args.Type == 1 && !c.successReduceTasks[args.Index] {
		c.successReduceTasks[args.Index] = true
		c.successReduceCount += 1
		if c.successReduceCount == c.nReduce {
			close(c.reduceTasks)
			c.done = true
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.mapTasks = make(chan int)
	c.successMapTasks = make([]bool, len(files))
	c.reduceTasks = make(chan int)
	c.successReduceTasks = make([]bool, nReduce)
	c.nReduce = nReduce
	c.successMapCount = 0
	c.successReduceCount = 0
	c.done = false
	c.server()
	for i := 0; i < len(files); i++ {
		c.mapTasks <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
	}
	return &c
}

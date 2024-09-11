package mr

import (
	// "fmt"
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

	// 任务列表
	mapTask    []Task
	reduceTask []Task

	path     string   // 服务器运行路径
	fileName []string // 输入的文件名

	// 待办任务数量
	mapRemain    int
	reduceRemain int

	// 保护元数据
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskMapTask(args *AskMapArgs, reply *AskMapReply) error {
	curTime := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapRemain == 0 {
		reply.Finished = true
		reply.TaskId = -1
		return nil
	}
	for index, task := range c.mapTask {
		duration := curTime.Sub(c.mapTask[index].StartTime)
		if task.State == UNASSIGNED || (task.State == PROGRESS && duration > 10.0) {
			c.mapTask[index].State = PROGRESS
			c.mapTask[index].StartTime = curTime
			c.mapTask[index].WorkerMachine = Machine{args.WorkerMachine.Path}
			reply.TaskId = index
			reply.FileName = c.fileName[index]
			reply.Path = c.path
			reply.NReduce = len(c.reduceTask)
			reply.Finished = false
			return nil
		}
	}
	reply.Finished = false
	reply.TaskId = -1
	return nil
}

func (c *Coordinator) AskReduceTask(args *AskReduceArgs, reply *AskReduceReply) error {
	curTime := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceRemain == 0 {
		reply.TaskId = -1
		reply.Finished = true
		return nil
	}
	for index, task := range c.reduceTask {
		duration := curTime.Sub(c.reduceTask[index].StartTime)
		if task.State == UNASSIGNED || (task.State == PROGRESS && duration > 10.0) {
			c.reduceTask[index].State = PROGRESS
			c.reduceTask[index].StartTime = curTime
			for _, task := range c.mapTask {
				reply.MiddleMachine = append(reply.MiddleMachine, task.WorkerMachine)
			}
			c.reduceTask[index].WorkerMachine = Machine{args.WorkerMachine.Path}
			reply.TaskId = index
			reply.Finished = false
			return nil
		}
	}
	reply.Finished = false
	reply.TaskId = -1
	return nil
}

func (c *Coordinator) MapFinish(args *TaskFinishedArgs, _ *TaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := args.TaskId
	c.mapTask[i].State = COMPLETE
	c.mapRemain--
	// fmt.Printf("map task %v finished\n", i)
	return nil
}

func (c *Coordinator) ReduceFinish(args *TaskFinishedArgs, _ *TaskFinishedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := args.TaskId
	c.reduceTask[i].State = COMPLETE
	c.reduceRemain--
	// fmt.Printf("reduce task %v finished\n", i)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// RPC函数示例
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
// 运行服务器
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.reduceRemain==0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	c.mapTask = make([]Task, nMap)
	c.reduceTask = make([]Task, nReduce)
	path, err := os.Getwd()
	if err != nil {
		log.Fatal("Getwd error")
	}
	c.path = path
	c.fileName = make([]string, nMap) // map任务的文件名
	c.reduceRemain = nReduce
	c.mapRemain = nMap
	for i, filename := range files {
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		c.fileName[i] = filename
	}

	c.server()
	return &c
}

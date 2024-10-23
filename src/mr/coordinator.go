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

// coordinator服务器
type Coordinator struct {
	// Your definitions here.
	// 可以记录worker的健康状态map
	
	mu           sync.Mutex // 互斥锁在并发时保护服务器数据
	Path         string     // 服务器运行地址
	FileNames    []string   //输入的所有文件
	MapTask      []Task
	ReduceTask   []Task
	MapRemain    int // 剩余map任务数量
	ReduceRemain int // 剩余reduce任务数量
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// rpc方法，args由发送方填充，reply由接收方填充，返回err
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 采用时间戳判断worker崩溃或执行任务超时
// coordinator周期性检查任务是否超时，如果超时10s，重新分发任务
func (c *Coordinator) AskMapTask(args *AskMapArgs, reply *AskMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapRemain == 0 {
		reply.TaskId = -1
		reply.Message=ALL_FINISH
		reply.AllFinish = true
		return nil
	}
	curTime := time.Now()
	// 分配所有任务
	for i, task := range c.MapTask {
		duration := curTime.Sub(c.MapTask[i].StartTime)
		// 任务未分配或执行超时
		if task.State == UNASSIGNED || (task.State == PROGRESS && duration.Seconds() > 10.0) {
			c.MapTask[i].State = PROGRESS
			c.MapTask[i].Worker = MachinePath{Path: args.Worker.Path}
			c.MapTask[i].StartTime = curTime
			reply.Path = c.Path
			reply.FileName = c.FileNames[i]
			reply.TaskId = i
			reply.NReduce = len(c.ReduceTask)
			reply.Message=CONTINUE
			reply.AllFinish = false
			return nil
		}
	}
	reply.TaskId = -1 // 无空闲worker
	reply.Message=BUSY
	reply.AllFinish = false
	return nil
}

func (c *Coordinator) MapFinish(args *TaskFinishArgs, _ *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := args.TaskId
	c.MapTask[i].State = FINISH
	c.MapRemain--
	return nil
}

func (c *Coordinator) AskReduceTask(args *AskReduceArgs, reply *AskReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReduceRemain == 0 {
		reply.TaskId = -1
		reply.Message=ALL_FINISH
		reply.AllFinish = true
		return nil
	}
	curTime := time.Now()
	for i, task := range c.ReduceTask {
		duration := curTime.Sub(c.ReduceTask[i].StartTime)
		// 任务未分配或执行超时
		if task.State == UNASSIGNED || (task.State == PROGRESS && duration.Seconds() > 10.0) {
			c.ReduceTask[i].State = PROGRESS
			c.ReduceTask[i].Worker = MachinePath{Path: args.Worker.Path}
			c.ReduceTask[i].StartTime = curTime
			// 返回正在执行map处理intermediate files的workers
			for _, mapTask := range c.MapTask {
				reply.IntermediateWorkers = append(reply.IntermediateWorkers, mapTask.Worker)
			}
			reply.TaskId = i
			reply.Message=CONTINUE
			reply.AllFinish = false
			return nil
		}
	}
	reply.TaskId = -1 // 无空闲worker
	reply.AllFinish = false
	reply.Message=BUSY
	return nil
}

func (c *Coordinator) ReduceFinish(args *TaskFinishArgs, _ *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := args.TaskId
	c.ReduceTask[i].State = FINISH
	// 执行完一个reduce任务
	c.ReduceRemain--
	return nil
}

// start a thread that listens for RPCs from worker.go
// rpc用于服务器通信，coordinator接受请求
func (c *Coordinator) server() {
	// 将coordinator方法注册到本地服务器，使rpc发送方worker可以调用
	rpc.Register(c)
	// 本地服务器可以处理http request
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	// 获取socket, 并调用http.serve监听其他服务器请求
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	// 所有任务全部完成
	return c.ReduceRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 需要执行map的文件
	nMap := len(files)
	c.MapTask = make([]Task, nMap)
	c.ReduceTask = make([]Task, nReduce)
	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("coordinator getwd error")
	}
	c.Path = path
	c.FileNames = make([]string, nMap)
	c.MapRemain = nMap
	c.ReduceRemain = nReduce
	for i, file := range files {
		if err != nil {
			log.Fatalf("cannot open %v file", file)
		}
		c.FileNames[i] = file
	}
	// coordinator注册到rpc
	c.server()
	return &c
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	FAILED = -1 //异常错误
	// 单个任务状态
	UNASSIGNED = 0 //空闲
	PROGRESS   = 1 //执行中
	FINISH     = 2 //任务完成
	// 任务执行信息
	ALL_FINISH = "ALL_FINISH" //所有任务完成
	BUSY       = "BUSY"       //无空闲worker
	CONTINUE   = "CONTINUE"   //分配任务或超时重发
)

// worker服务器地址
type MachinePath struct {
	Path string
}

// 使用时间戳和超时机制, 让coordinator周期性检查任务，判断worker是否崩溃
type Task struct {
	State     int
	Worker    MachinePath
	StartTime time.Time
}

// worker请求coordinator分配map任务
type AskMapArgs struct {
	Worker MachinePath
}

// coordinator返回map情况
type AskMapReply struct {
	TaskId    int    // map任务编号
	Path      string //服务器运行地址
	NReduce   int    // reduce任务数量
	FileName  string //input file
	AllFinish bool   // 所有map任务完成
	Message   string //任务执行信息
}

// worker请求coordinator分配reduce任务
type AskReduceArgs struct {
	Worker MachinePath
}

// coordinator返回reduce情况
type AskReduceReply struct {
	TaskId              int           // reduce任务编号
	IntermediateWorkers []MachinePath // 处理map任务和intermediate files的workers
	AllFinish           bool          // 所有reduce任务完成
	Message             string        //任务执行信息
}

type TaskFinishArgs struct {
	TaskId int
}

type TaskFinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

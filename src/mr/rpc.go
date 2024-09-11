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

// 记录任务状态
const (
	UNASSIGNED = 0
	PROGRESS   = 1
	COMPLETE   = 2
)

// 记录服务器的地址
type Machine struct {
	Path string
}

// 任务信息
type Task struct {
	State         int
	StartTime     time.Time
	WorkerMachine Machine
}

type AskMapArgs struct {
	WorkerMachine Machine
}

type AskMapReply struct {
	FileName string
	Path     string
	TaskId   int
	NReduce  int
	Finished bool
}

type AskReduceArgs struct {
	WorkerMachine Machine
}

type AskReduceReply struct {
	TaskId        int
	Finished      bool
	MiddleMachine []Machine // 中间服务器
}

type TaskFinishedArgs struct {
	TaskId int
	Finished bool
}

type TaskFinishedReply struct {
	TaskId int
	Finished bool
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

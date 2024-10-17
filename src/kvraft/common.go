package kvraft

import "log"

const (
	Debug          = true
	NONE           = -1
	GET            = 0
	PUT            = 1
	APPEND         = 2
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrServerDead  = "ErrServerDead"
	ErrTimeout     = "ErrTimeout"
)

func DPrintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if Debug {
		if debug {
			log.Printf(format, a...)
		}
	}
	return
}

// mark operation whether success
type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// mark client's operation
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}

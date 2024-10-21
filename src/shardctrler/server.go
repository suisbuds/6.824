package shardctrler

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JOIN      = 0
	LEAVE     = 1
	MOVE      = 2
	QUERY     = 3
	ASKSHARDS = 4
)

const Debug = false

func DPrintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if Debug {
		if debug {
			log.Printf(format, a...)
		}
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs            []Config         // indexed by config num
	clientSequenceNums map[int64]int64  // 去重
	queryBuffer        map[int64]Config // client执行query时应该返回的config
}

// 包含operation里出现的所有字段
type Op struct {
	// Your data here.
	Type        int
	Serevrs     map[int][]string
	GIDs        []int
	Shard       int
	GID         int
	Num         int // desired config number
	ClientId    int64
	SequenceNum int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, _, isLeader := sc.rf.Start(Op{
		Type:        JOIN,
		Serevrs:     args.Servers,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum},
	)
	if !isLeader {
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	for {
		if atomic.LoadInt32(&timeout) == 1 {
			return
		}
		sc.mu.Lock()
		if sc.clientSequenceNums[args.ClientId] >= args.SequenceNum {
			reply.Done = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, _, isLeader := sc.rf.Start(Op{
		Type:        LEAVE,
		GIDs:        args.GIDs,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum},
	)
	if !isLeader {
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	for {
		if atomic.LoadInt32(&timeout) == 1 {
			return
		}
		sc.mu.Lock()
		if sc.clientSequenceNums[args.ClientId] >= args.SequenceNum {
			reply.Done = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, _, isLeader := sc.rf.Start(Op{
		Type:        JOIN,
		Shard:       args.Shard,
		GID:         args.GID,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum},
	)
	if !isLeader {
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	for {
		if atomic.LoadInt32(&timeout) == 1 {
			return
		}
		sc.mu.Lock()
		if sc.clientSequenceNums[args.ClientId] >= args.SequenceNum {
			reply.Done = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	_, _, isLeader := sc.rf.Start(Op{
		Type:        JOIN,
		Num:         args.Num,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum},
	)
	if !isLeader {
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	for {
		if atomic.LoadInt32(&timeout) == 1 {
			return
		}
		sc.mu.Lock()
		if sc.clientSequenceNums[args.ClientId] >= args.SequenceNum {
			// client 串行访问shard controller，所以可以用clientId保存读取结果
			reply.Config = sc.queryBuffer[args.ClientId]
			reply.Done = true
			sc.mu.Unlock()
			return
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) joinTask(op Op) {}

func (sc *ShardCtrler) leaveTask(op Op) {}

func (sc *ShardCtrler) moveTask(op Op) {}

func (sc *ShardCtrler) queryTask(op Op, configsSize int) {
	// 缓存，防止config返回client前被修改
	if op.Num == -1 || op.Num >= configsSize {
		// 返回最新配置
		sc.queryBuffer[op.ClientId] = sc.configs[configsSize-1]
	} else {
		sc.queryBuffer[op.ClientId] = sc.configs[op.Num]
	}
}

func (sc *ShardCtrler) doOperation(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.clientSequenceNums[op.ClientId] >= op.SequenceNum {
		return
	}
	sc.clientSequenceNums[op.ClientId] = op.SequenceNum
	configsSize := len(sc.configs)
	switch op.Type {
	case JOIN:
		sc.joinTask(op)
	case LEAVE:
		sc.leaveTask(op)
	case MOVE:
		sc.moveTask(op)
	case QUERY:
		sc.queryTask(op, configsSize)
	}
}

func (sc *ShardCtrler) receiveMsg() {
	for msg := range sc.applyCh {
		op := msg.Command.(Op)
		sc.doOperation(op)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSequenceNums = make(map[int64]int64)
	sc.queryBuffer = make(map[int64]Config)

	go sc.receiveMsg()
	return sc
}

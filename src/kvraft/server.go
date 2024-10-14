package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType int
	// Operation parameters
	Key string
	Val string
	// Client parameters
	ClientId       int64
	SequenceNumber int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// persistent data
	data               map[string]string // record key-value pairs
	maxSequenceNumbers map[int64]int64   // record the max sequence number of each client, avoid duplicate operation
	applyIndex         int               // client apply max command index, to compact log

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrServerDead
		return
	}
	// 将客户端发送的操作以Command形式发送给Raft
	_, _, isLeader := kv.rf.Start(Op{
		OperationType:  GET,
		Key:            args.Key,
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf(false, "server-[%d] Get Command, ClientId = %d, Seq = %d, Key = %s", kv.me, args.ClientId, args.SequenceNumber, args.Key)
	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()
	// 轮询等待Command提交
	for {
		// 设置超时阀值，防止日志提交后丢失导致死循环
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrRaftTimeout
			return
		}
		kv.mu.Lock()
		// 利用操作序列单调递增的特性，判断Command是否提交和执行
		if kv.maxSequenceNumbers[args.ClientId] >= args.SequenceNumber {
			reply.Value = kv.data[args.Key]
			kv.mu.Unlock()
			DPrintf(false, "server-[%d] Get Command %s", kv.me, OK)
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrServerDead
		return
	}
	var operationType int
	if args.Op == "Put" {
		operationType = PUT
	} else if args.Op == "Append" {
		operationType = APPEND
	}
	_, _, isLeader := kv.rf.Start(Op{
		OperationType:  operationType,
		Key:            args.Key,
		Val:            args.Value,
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf(false, "server-[%d] PutAppend Command, ClientId = %d, Seq = %d, Key = %s, Val = %s, Op = %s", kv.me, args.ClientId, args.SequenceNumber, args.Key, args.Value, args.Op)
	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()
	for {
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrRaftTimeout
			return
		}
		kv.mu.Lock()
		if kv.maxSequenceNumbers[args.ClientId] >= args.SequenceNumber {
			kv.mu.Unlock()
			DPrintf(false, "server-[%d] PutAppend Command %s", kv.me, OK)
			return
		}
		kv.mu.Unlock()	
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.maxSequenceNumbers = make(map[int64]int64)

	// You may need initialization code here.
	var data map[string]string
	var maxSequenceNumbers map[int64]int64
	var applyIndex int
	// use persister to restore previous server state
	snapshot := persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	// Don't find error
	if d.Decode(&data) == nil && d.Decode(&maxSequenceNumbers) == nil && d.Decode(&applyIndex) == nil {
		kv.mu.Lock()
		kv.data = data
		kv.maxSequenceNumbers = maxSequenceNumbers
		kv.applyIndex = applyIndex
		kv.mu.Unlock()
	}
	go kv.receiveMsg()
	go kv.trySnapshot()
	return kv
}

// receive commited logs from kv.applyCh
func (kv *KVServer) receiveMsg() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		DPrintf(false, "server-[%d], msg receive = %v, raftStateSize = %d", kv.me, msg, kv.rf.RaftStateSize())
		// Command log
		if msg.CommandValid {
			op := msg.Command.(Op) // type assertion
			kv.mu.Lock()
			kv.doOperation(op)
			kv.applyIndex = msg.CommandIndex
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// Snapshot command
			var data map[string]string
			var maxSequenceNumbers map[int64]int64
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			if d.Decode(&data) == nil && d.Decode(&maxSequenceNumbers) == nil {
				kv.mu.Lock()
				// replace local server state with snapshot data
				kv.data = data
				kv.maxSequenceNumbers = maxSequenceNumbers
				kv.applyIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// check raft's logs current size periodically, and compact logs if hit the threshold
func (kv *KVServer) trySnapshot() {}

// according to the operation type, do the corresponding operation
func (kv *KVServer) doOperation(op Op) {
	// 客户端的操作序号单调递增，说明此操作执行过了
	if kv.maxSequenceNumbers[op.ClientId] >= op.SequenceNumber {
		return
	}
	// 更新操作序号并执行操作
	kv.maxSequenceNumbers[op.ClientId] = op.SequenceNumber
	if op.OperationType == GET {
		return
	} else if op.OperationType == PUT {
		kv.data[op.Key] = op.Val
	} else if op.OperationType == APPEND {
		kv.data[op.Key] += op.Val
	}
}

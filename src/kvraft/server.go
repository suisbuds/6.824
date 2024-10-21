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
	ClientId    int64
	SequenceNum int64
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
	data            map[string]string // record key-value pairs
	clientSequenceNums map[int64]int64   // record the max sequence number of each client, avoid duplicate operation
	applyIndex      int               // client apply max command index, to compact log

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrServerDead
		return
	}
	// 将客户端发送的操作以Command形式发送给Raft
	op := Op{
		OperationType: GET,
		Key:           args.Key,
		ClientId:      args.ClientId,
		SequenceNum:   args.SequenceNum,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf(false, "server-[%d] Get Command, ClientId = %d, Seq = %d, Key = %s", kv.me, args.ClientId, args.SequenceNum, args.Key)

	var timeout int32
	atomic.StoreInt32(&timeout, 0)
	go func() {
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	// 轮询等待Command提交
	for {
		// 设置超时阀值，防止日志提交后丢失导致死循环
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrTimeout
			return
		}
		kv.mu.Lock()
		// 利用操作序列单调递增的特性，判断Command是否提交和执行
		if kv.clientSequenceNums[args.ClientId] >= args.SequenceNum {
			DPrintf(false, "server-[%d] Get Command %s", kv.me, OK)
			reply.Value = kv.data[args.Key]
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

// Except handling Put and Append operations, the main logic is similar to Get function
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrServerDead
		return
	}

	var operationType int
	switch args.Op {
	case "Put":
		operationType = PUT
	case "Append":
		operationType = APPEND
	}

	_, _, isLeader := kv.rf.Start(Op{
		OperationType: operationType,
		Key:           args.Key,
		Val:           args.Value,
		ClientId:      args.ClientId,
		SequenceNum:   args.SequenceNum,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf(false, "server-[%d] PutAppend Command, ClientId = %d, Seq = %d, Key = %s, Val = %s, Op = %s", kv.me, args.ClientId, args.SequenceNum, args.Key, args.Value, args.Op)

	var timeout int32
	atomic.StoreInt32(&timeout, 0)
	go func() {
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()

	for {
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrTimeout
			return
		}
		kv.mu.Lock()
		if kv.clientSequenceNums[args.ClientId] >= args.SequenceNum {
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

// receive commited logs from kv.applyCh
func (kv *KVServer) receiveMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf(false, "server-[%d], msg receive = %v, raftStateSize = %d", kv.me, msg, kv.rf.RaftStateSize())
		if msg.CommandValid {
			// log Command
			kv.mu.Lock()
			op := msg.Command.(Op) // type assertion
			kv.applyIndex = msg.CommandIndex
			kv.doOperation(op)
			kv.mu.Unlock()
			// NOTE:修改这里
			// if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate*8/10 {
			// 	kv.snapshot()
			// }
		} else if msg.SnapshotValid {
			// Snapshot command

			var data map[string]string
			var clientSequenceNums map[int64]int64
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)

			if d.Decode(&data) == nil && d.Decode(&clientSequenceNums) == nil {
				// replace local server state with snapshot data
				kv.mu.Lock()
				kv.data = data
				kv.clientSequenceNums = clientSequenceNums
				kv.applyIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
			// NOTE:修改这里
			// if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate*8/10 {
			// 	kv.snapshot()
			// }
		}
	}
}

// check raft's logs current size periodically, and compact logs if hit the threshold
// UGLY: 平衡trySnapshot的频率，必须让goroutine休眠，否则会一直占用资源
func (kv *KVServer) trySnapshot() {

	for !kv.killed() {
		// 如果raft日志长度大于阀值，利用snapshot压缩日志
		// NOTE:修改这里
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate*8/10 {
			kv.snapshot()
		}
		time.Sleep(time.Millisecond)
	}
}

// according to the operation type, do the corresponding operation
func (kv *KVServer) doOperation(op Op) {
	// 客户端的操作序号单调递增，说明此操作执行过了
	if kv.clientSequenceNums[op.ClientId] >= op.SequenceNum {
		return
	}
	// 更新操作序号并执行操作
	kv.clientSequenceNums[op.ClientId] = op.SequenceNum
	switch op.OperationType {
	case GET:
		return
	case PUT:
		kv.data[op.Key] = op.Val
	case APPEND:
		kv.data[op.Key] += op.Val
	}
}

func (kv *KVServer) snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.clientSequenceNums)
	e.Encode(kv.applyIndex)

	applyIndex := kv.applyIndex // 当前已运行的最大操作序号
	snapshot := w.Bytes()       // sever run state
	kv.rf.Snapshot(applyIndex, snapshot)
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

	kv.data = map[string]string{}
	kv.clientSequenceNums = map[int64]int64{}

	// You may need initialization code here.

	var data map[string]string
	var clientSequenceNums map[int64]int64
	var applyIndex int
	snapshot := persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	// Don't find error, restore server state
	if d.Decode(&data) == nil && d.Decode(&clientSequenceNums) == nil && d.Decode(&applyIndex) == nil {
		kv.mu.Lock()
		kv.data = data
		kv.clientSequenceNums = clientSequenceNums
		kv.applyIndex = applyIndex
		kv.mu.Unlock()
	}

	// keep running goroutine for receiving Msg and trying Snapshot
	go kv.receiveMsg()
	go kv.trySnapshot()
	return kv
}


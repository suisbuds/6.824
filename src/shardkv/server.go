package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	Debug     = false
	GET       = 0
	PUT       = 1
	APPEND    = 2
	PUTSHARD  = 3
	MOVESHARD = 4
)

func DPrintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if Debug && debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   int
	Key    string
	Val    string
	Config int

	Shard         int
	ShardData     map[string]string
	ShardSequence map[int64]int64
	ShardBuffer   map[int64]string

	Servers     []string
	ClientId    int64
	SequenceNum int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm                 *shardctrler.Clerk
	config             shardctrler.Config
	data               []map[string]string // key/value
	clientSequenceNums []map[int64]int64
	applyIndex         int

	curShards          []bool            // kv server's current shards
	requiredShards     []bool            // kv server's required shards
	tasks              []mvShard         // shards 转移任务
	serverSequenceNums []map[int64]int64 // 记录shards转移操作

	NShards    int
	initialize bool // 将分片分配至group时，记录server是否观察到config的初始化
}

type mvShard struct {
}

type SnapshotData struct {
	data               []map[string]string
	clientSequenceNums []map[int64]int64
	serverSequenceNums []map[int64]int64
	curShards          []bool
	tasks              []mvShard
	initialize         bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// 确认shard是否在group的管理内
	if kv.config.Shards[args.Shard] != kv.gid || !kv.curShards[args.Shard] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(Op{
		Type:        GET,
		Key:         args.Key,
		Shard:       args.Shard,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()
	for {
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrTimeout
			return
		}
		kv.mu.Lock()
		if kv.clientSequenceNums[args.Shard][args.ClientId] >= args.SequenceNum {
			reply.Value = kv.data[args.Shard][args.Key]
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// 确认shard是否在group的管理内
	if kv.config.Shards[args.Shard] != kv.gid || !kv.curShards[args.Shard] {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opType int
	switch args.Op {
	case "Put":
		opType = PUT
	case "Append":
		opType = APPEND
	}
	
	_, _, isLeader := kv.rf.Start(Op{
		Type:        opType,
		Key:         args.Key,
		Val: 	   args.Value,
		Shard:       args.Shard,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var timeout int32 = 0
	go func() {
		time.Sleep(1000 * time.Millisecond)
		atomic.StoreInt32(&timeout, 1)
	}()
	for {
		if atomic.LoadInt32(&timeout) == 1 {
			reply.Err = ErrTimeout
			return
		}
		kv.mu.Lock()
		if kv.clientSequenceNums[args.Shard][args.ClientId] >= args.SequenceNum {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) updateConfig() {}

func (kv *ShardKV) receiveMsg() {}

func (kv *ShardKV) trySnapshot() {}

func (kv *ShardKV) moveShard() {}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.NShards = len(kv.config.Shards)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.curShards = make([]bool, kv.NShards)
	kv.requiredShards = make([]bool, kv.NShards)
	kv.data = make([]map[string]string, kv.NShards)
	kv.clientSequenceNums = make([]map[int64]int64, kv.NShards)
	kv.serverSequenceNums = make([]map[int64]int64, kv.NShards)
	for i := 0; i < kv.NShards; i++ {
		kv.data[i] = make(map[string]string)
		kv.clientSequenceNums[i] = make(map[int64]int64)
		kv.serverSequenceNums[i] = make(map[int64]int64)
	}

	// 读取快照，服务器运行状态的字段都要保存
	var snapshotData SnapshotData
	snapshot := persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&snapshotData) == nil {
		kv.mu.Lock()
		kv.data = snapshotData.data
		kv.clientSequenceNums = snapshotData.clientSequenceNums
		kv.serverSequenceNums = snapshotData.serverSequenceNums
		kv.curShards = snapshotData.curShards
		kv.tasks = snapshotData.tasks
		kv.initialize = snapshotData.initialize
		kv.mu.Unlock()
	}

	go kv.updateConfig() // 轮询更新config
	go kv.receiveMsg()   // 接收Raft提交的Command
	go kv.trySnapshot()
	go kv.moveShard() // shard转移

	return kv
}

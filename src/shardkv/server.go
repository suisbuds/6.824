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

	curShards          []bool            // kv server 当前持有的 shards
	requiredShards     []bool            // kv server's 需要的 shards
	tasks              []MvShard         // shards 转移任务
	serverSequenceNums []map[int64]int64 // 记录shards转移操作

	NShards     int
	checkConfig bool // 当 shards 再分配时，记录server是否观察到config的变化
}

type MvShard struct {
	Config        int
	Servers       []string
	Shard         int
	ShardData     map[string]string
	ShardSequence map[int64]int64
	ShardBuffer   map[int64]string
}

type SnapshotData struct {
	Data               []map[string]string
	ClientSequenceNums []map[int64]int64
	ServerSequenceNums []map[int64]int64
	CurShards          []bool
	Tasks              []MvShard
	CheckConfig        bool
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
		Val:         args.Value,
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

// PutShard RPC
func (kv *ShardKV) PutShard(args *PutShardArgs, reply *PutShardReply) {
	_, _, isLeader := kv.rf.Start(Op{
		Type:          PUTSHARD,
		Shard:         args.Shard,
		ShardData:     args.Data,
		ShardSequence: args.ClientSequenceNums,
		ClientId:      args.ClientId,
		SequenceNum:   args.SequenceNum,
	})
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
		kv.mu.Lock()
		if kv.clientSequenceNums[args.Shard][args.ClientId] >= args.SequenceNum {
			reply.Done = true
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

func (kv *ShardKV) updateConfig() {
	for {
		kv.mu.Lock()
		kv.rf.Start(Op{})

		// server 定期从 sm 获取最新 config，发现 config 变化时更新本地 config 和 requiredShards
		config := kv.sm.Query(-1)
		if config.Num != kv.config.Num && config.Num != 0 {
			// 检查 config 1's GID == group's GID
			if !kv.checkConfig {
				// 通过 config 1 完成 shards 再分配
				firstConfig := kv.sm.Query(1)
				for i := range kv.config.Shards {
					if firstConfig.Shards[i] == kv.gid {
						kv.curShards[i] = true
					}
				}
				kv.checkConfig = true // snapshot 保存，防止 shards 重复分配
			}
			requiredShards := make([]bool, kv.NShards)
			for i := range config.Shards {
				if config.Shards[i] == kv.gid {
					requiredShards[i] = true
				}
				kv.requiredShards = requiredShards
				kv.config = config
			}
		}
		for i := range kv.curShards {
			// 检查 group 不需要的 shards，执行 MOVESHARD, 并在 Raft 集群达成一致
			if kv.curShards[i] && !kv.requiredShards[i] {
				_, _, ok := kv.rf.Start(Op{
					Type:        MOVESHARD,
					Config:      kv.config.Num,
					Servers:     kv.config.Groups[kv.config.Shards[i]],
					Shard:       i,
					ClientId:    int64(kv.gid),
					SequenceNum: int64(kv.config.Num),
				})
				if ok {
					DPrintf(false, "[group: %d]-[server: %d] Start MoveShard", kv.gid, kv.me)
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// 抄Lab3
func (kv *ShardKV) receiveMsg() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			kv.doOperation(op)
			kv.applyIndex = msg.CommandIndex
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.readPersist(msg.Snapshot)
		}
	}
}

func (kv *ShardKV) trySnapshot() {
	for {
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate*8/10 {
			kv.snapshot()
		}
		time.Sleep(5*time.Millisecond)
	}
}

// 轮询检查 mvShard 任务
func (kv *ShardKV) tryMoveShard() {
	for {
		kv.mu.Lock()
		if len(kv.tasks) != 0 {
			task := kv.tasks[0]
			// kv.tasks = append(kv.tasks[:0], kv.tasks[1:]...)
			newTasks := make([]MvShard, len(kv.tasks)-1)
			copy(newTasks, kv.tasks[1:])
			kv.tasks = newTasks
			kv.moveShard(task)
		}
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// MoveShard Call PutShard RPC
func (kv *ShardKV) moveShard(task MvShard) {
	args := PutShardArgs{}
	args.Shard = task.Shard
	args.Data = task.ShardData
	args.ClientId = int64(kv.gid)
	args.SequenceNum = int64(task.Config)
	args.ClientSequenceNums = task.ShardSequence
	args.Buffer = task.ShardBuffer
	servers := task.Servers
	kv.mu.Unlock()
	for {
		// 参考 client
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply PutShardReply
			ok := srv.Call("ShardKV.PutShard", &args, &reply)
			if ok && reply.Done {
				kv.mu.Lock()
				return
			}
		}
	}
}

func (kv *ShardKV) readPersist(snapshot []byte) {
	// 读取快照，服务器运行状态的字段都要保存
	var snapshotData SnapshotData
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&snapshotData) == nil {
		kv.mu.Lock()
		kv.data = snapshotData.Data
		kv.clientSequenceNums = snapshotData.ClientSequenceNums
		kv.serverSequenceNums = snapshotData.ServerSequenceNums
		kv.curShards = snapshotData.CurShards
		kv.tasks = snapshotData.Tasks
		kv.checkConfig = snapshotData.CheckConfig
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(SnapshotData{
		Data:               kv.data,
		ClientSequenceNums: kv.clientSequenceNums,
		ServerSequenceNums: kv.serverSequenceNums,
		CurShards:          kv.curShards,
		Tasks:              kv.tasks,
		CheckConfig:        kv.checkConfig,
	})
	applyIndex := kv.applyIndex
	data := w.Bytes()
	kv.rf.Snapshot(applyIndex, data)
}

func (kv *ShardKV) doOperation(op Op) {
	switch op.Type {
	case GET:
		return
	case PUT:
		kv.doPutAppend(op)
	case APPEND:
		kv.doPutAppend(op)
	case PUTSHARD:
		kv.doPutShard(op)
	case MOVESHARD:
		kv.doMoveShard(op)
	}
}

func (kv *ShardKV) doPutAppend(op Op) {
	if kv.serverSequenceNums[op.Shard][op.ClientId] >= op.SequenceNum {
		return
	}
	kv.serverSequenceNums[op.Shard][op.ClientId] = op.SequenceNum
	if op.Type == PUT {
		kv.data[op.Shard][op.Key] = op.Val
	} else if op.Type == APPEND {
		kv.data[op.Shard][op.Key] += op.Val
	}
}

func (kv *ShardKV) doPutShard(op Op) {
	if kv.serverSequenceNums[op.Shard][op.ClientId] >= op.SequenceNum {
		return
	}
	kv.serverSequenceNums[op.Shard][op.ClientId] = op.SequenceNum
	kv.curShards[op.Shard] = true
	kv.data[op.Shard] = map[string]string{}
	kv.clientSequenceNums[op.Shard] = map[int64]int64{}
	for k, v := range op.ShardData {
		kv.data[op.Shard][k] = v
	}
	for k, v := range op.ShardSequence {
		kv.clientSequenceNums[op.Shard][k] = v
	}
}

func (kv *ShardKV) doMoveShard(op Op) {
	if kv.serverSequenceNums[op.Shard][op.ClientId] >= op.SequenceNum {
		return
	}
	kv.serverSequenceNums[op.Shard][op.ClientId] = op.SequenceNum
	kv.curShards[op.Shard] = false
	task := MvShard{
		Config:        op.Config,
		Servers:       op.Servers,
		Shard:         op.Shard,
		ShardData:     make(map[string]string),
		ShardSequence: make(map[int64]int64),
		ShardBuffer:   make(map[int64]string),
	}
	for k, v := range kv.data[op.Shard] {
		task.ShardData[k] = v
	}
	for k, v := range kv.clientSequenceNums[op.Shard] {
		task.ShardSequence[k] = v
	}
	kv.data[op.Shard] = map[string]string{}
	kv.clientSequenceNums[op.Shard] = map[int64]int64{}
	kv.tasks = append(kv.tasks, task)

}

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
	// 需要labgob编解码自定义类型时，需要注册
	labgob.Register(SnapshotData{})
	labgob.Register(MvShard{})

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

	
	// // 读取快照，服务器运行状态的字段都要保存
	// var snapshotData SnapshotData
	// snapshot := persister.ReadSnapshot()
	// r := bytes.NewBuffer(snapshot)
	// d := labgob.NewDecoder(r)
	// if d.Decode(&snapshotData) == nil {
	// 	kv.mu.Lock()
	// 	kv.data = snapshotData.Data
	// 	kv.clientSequenceNums = snapshotData.ClientSequenceNums
	// 	kv.serverSequenceNums = snapshotData.ServerSequenceNums
	// 	kv.curShards = snapshotData.CurShards
	// 	kv.tasks = snapshotData.Tasks
	// 	kv.checkConfig = snapshotData.CheckConfig
	// 	kv.mu.Unlock()
	// }
	snapshot := persister.ReadSnapshot()
	kv.readPersist(snapshot)

	go kv.updateConfig() // 轮询更新config
	go kv.receiveMsg()   // 接收Raft提交的Command
	go kv.trySnapshot()
	go kv.tryMoveShard() // shard转移

	return kv
}

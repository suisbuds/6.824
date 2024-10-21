package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Gid         int
	Shard       int
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Gid         int
	Shard       int
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PutShardArgs struct {
	Shard              int
	Data               map[string]string
	Config             int
	ClientId           int64
	SequenceNum        int64
	ClientSequenceNums map[int64]int64
	Buffer             map[int64]string
}

type PutShardReply struct {
	Config int
	Done   bool
}

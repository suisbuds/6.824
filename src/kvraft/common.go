package kvraft


const (
	None           = -1
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)



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
	ClientId	int64
	SequenceNumber	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// mark client's operation
	ClientId int64
	SequenceNumber int64
}

type GetReply struct {
	Err   Err
	Value string
}

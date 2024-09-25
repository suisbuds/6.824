package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D: 日志压缩
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FAILED               = -1
	LEADER               = 1
	CANDIDATE            = 2
	FOLLOWER             = 3
	BROADCASTTIME        = 100  // 限制为每秒十次心跳
	ELECTIONTIMEOUTBASE  = 1000 // broadcastTime ≪ electionTimeout ≪ MTBF
	ELECTIONTIMEOUTRANGE = 1000
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int // 日志条目的索引
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applych   chan ApplyMsg       // channel,用于发送提交的日志
	cond      *sync.Cond          // 唤醒睡眠的候选者
	// quickCheck rune
	// name string
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	BroadcastTime   int // 心跳间隔
	ElectionTimeout int // 随机选举超时

	Log         []LogEntry
	CurrentTerm int
	VotedFor    int
	CommitIndex int   // 已提交的日志
	LastApplied int   // 已应用的日志
	NextIndex   []int // 即将发送到server的日志
	MatchIndex  []int // 已经复制到server的日志

	State int // 角色

	LastIncludedIndex int // 快照压缩替换掉的之前的索引
	LastIncludedTerm  int // 快照压缩替换掉的索引的term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	if rf.killed() {
		return FAILED, false
	}
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.State == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的term
	CandidateId  int
	LastLogIndex int // candidate的LogIndex
	LastLogTerm  int // candidate的LogTerm
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前term
	VoteGranted bool // candidate是否获得投票
}

// 自定义rpc的struct

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 用于优化重传
	XTerm  int
	XIndex int
	XLen   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// DPrintf("[%d] Get RequestVote from %d", rf.me, args.CandidateId)
	defer rf.mu.Unlock()
	// 填充reply
	reply.Term = rf.CurrentTerm
	LastIndex := rf.LastLogEntry().Index
	LastTerm := rf.LastLogEntry().Term
	// 如果本地term>candidate的term, 拒绝投票
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm < args.Term {
		// 如果本地term<candidate的term, 则转换为follower
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term // 变为candidate的term
		rf.VotedFor = FAILED       // 取消投票权
		rf.persist()
	}

	if rf.VotedFor == -1 && (LastTerm < args.LastLogTerm) {
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 客户端向Raft服务器发送命令，创建日志条目并插入本地日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// 请求失败
	_, isLeader := rf.GetState()
	if !isLeader || rf.killed() {
		rf.DPrintf("[%d] Start() Fail isleader = %t, isKilled = %t", rf.me, isLeader, rf.killed())
		return -1, -1, false
	}

	// Your code here (2B).

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// 判断goroutines是否kill,需要lock
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 无限期执行raft集群的任务，直到server被杀死
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// 更新已应用的日志
		rf.UpdateLastApplied()
		// 在任务函数中处理完元数据 / 耗时操作 需要解锁，防止死锁
		switch rf.State {
		case LEADER:
			rf.mu.Unlock()
			rf.DoLeaderTask()
		case CANDIDATE:
			rf.DoCandidateTask()
		case FOLLOWER:
			// 选举超时，转换为candidate
			if !rf.DoFollowerTask() {
				continue
			}
		default:
			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.mu.Lock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applych = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.DPrintf("[%d] is Making, len(peers) = %d\n", me, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.BroadcastTime = BROADCASTTIME
	rf.ElectionTimeout = GetElectionTimeout() // 随机选举超时
	rf.State = FOLLOWER                       // 初始化为follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1 // 未投票
	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// 自定义函数

func (rf *Raft) DoLeaderTask() {
	
}

func (rf *Raft) DoFollowerTask() bool {
	rf.DPrintf("[%d] 's ElectionTimeout = %d\n", rf.me, rf.ElectionTimeout)
	// 超时转换为candidate
	if rf.ElectionTimeout < rf.BroadcastTime {
		rf.State = CANDIDATE
		rf.DPrintf("[%d] is ElectionTimeout, convert to CANDIDATE\n", rf.me)
		rf.mu.Unlock()
		return false
	}
	rf.ElectionTimeout -= rf.BroadcastTime // 未超时，继续等待
	rf.mu.Unlock()
	time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond) //
	return true
}

func (rf *Raft) DoCandidateTask() {
	rf.CurrentTerm++
	voteGranted := 1 // 得票数初始化为1
	rf.VotedFor = rf.me
	rf.persist()
	rf.ElectionTimeout = GetElectionTimeout()
	lastLogIndex := rf.LastLogEntry().Index
	lastLogTerm := rf.LastLogEntry().Term
	rf.DPrintf("[%d] start election, term = %d\n", rf.me, rf.CurrentTerm)
	// 处理完元数据, 在发送rpc时不得持有锁
	rf.mu.Unlock()

}

// 应用已提交的日志, 同时更新LastApplied
func (rf *Raft) UpdateLastApplied() {

}

func (rf *Raft) UpdateCommitIndex() {}

func (rf *Raft) TrySendEntries() {}

func (rf *Raft) SendEntries() {}

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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
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

const (
	FAILED                 = -1
	LEADER                 = 1
	CANDIDATE              = 2
	FOLLOWER               = 3
	BROADCAST_TIME         = 100 // 限制为每秒十次心跳
	ELECTION_TIMEOUT_BASE  = 200 // broadcastTime < electionTimeout ≪ MTBF
	ELECTION_TIMEOUT_RANGE = 200
	MAXRAFTSTATE           = 1000
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For Lab2D
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg // 用于发送提交日志的channel
	cond    *sync.Cond    // 唤醒线程

	// Lab3：Client 发送的 operation 要在三分之一心跳间隔内提交
	// 为了快速提交日志，leader需要在 Start() 时立即向follower发送日志，并在发送后即刻更新 CommitIndex，从而加速CommitIndex的更新
	quickCommitCheck int32

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// initialize in Make()
	BroadcastTime   int // 心跳间隔
	ElectionTimeout int // 随机选举超时
	Role            int
	CurrentTerm     int
	VotedFor        int
	Log             []LogEntry
	NextIndex       []int // leader 即将发送到对应 follower 的日志
	MatchIndex      []int // leader 已经发送给对应 follower 的日志，即 follower 当前拥有的所有日志

	// lazy init
	CommitIndex       int // 需要提交的日志
	LastApplied       int // 已提交并应用的日志
	LastIncludedIndex int // 快照压缩替换掉的之前的索引
	LastIncludedTerm  int // 快照压缩替换掉的索引的term
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	if rf.killed() {
		return FAILED, false
	}
	// Your code here (2A).
	rf.mu.Lock()
	var term int
	var isLeader bool
	term = rf.CurrentTerm
	isLeader = rf.Role == LEADER
	rf.mu.Unlock()
	return term, isLeader
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
	// persist CurrentTerm, VotedFor, Log, LastIncludedIndex, LastIncludedTerm

	// Don't need to add lock, because it's called by other functions with lock
	// RaftState 包含的data，其中rf.Log会随运行时间膨胀，所以要压缩
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.SaveRaftState(raftstate)
	rf.DPrintf(false, "persist() raftstate size = %d", rf.RaftStateSize())
	rf.DPrintf(false, "rf-[%d] call persist(), CurrentTerm = %d, VotedFor = %d, LogLength = %d, LastIncludedIndex = %d, LastIncludedTerm = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log), rf.LastIncludedIndex, rf.LastIncludedTerm)
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

	// Need to add lock, because it's called by other functions without lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var validateDefaultVaule func() bool
	validateDefaultVaule = func() bool {
		if d.Decode(&currentTerm) != nil ||
			d.Decode(&votedFor) != nil ||
			d.Decode(&log) != nil ||
			d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil {
			return false
		}
		return true
	}
	if !validateDefaultVaule() {
		// panic("ReadPersist(): Decode Error because of indefault value")
		return
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.DPrintf(false, "rf-[%d] call readPersist(), CurrentTerm = %d, VotedFor = %d, LogLength = %d, LastIncludedIndex = %d, LastIncludedTerm = %d", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log), lastIncludedIndex, lastIncludedTerm)
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// Not need to implement

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// HACK: 问题是日志压缩不够，而且很明显rf.Log = log添加很多多余的日志
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// delete log entries whose index <= arg's index
	rf.mu.Lock()

	// 1. Not need to snapshot
	if index <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	// 2. log compaction
	rf.DPrintf(false, "rf-[%d] Call Snapshot(), index = %d", rf.me, index)
	var log []LogEntry
	// BUG:check the boundary , it may lead to index out of range😄
	for i := index + 1; i <= rf.GetLastLogEntry().Index; i++ {
		log = append(log, rf.GetLogEntry(i))
	}
	rf.LastIncludedTerm = rf.GetLogEntry(index).Term // 要在替换日志前操作，否则会越界
	rf.LastIncludedIndex = index                     // update lastIncludedIndex
	rf.Log = log

	// 3. persist current state and snapshot, raftstate is lab2, snapshot is lab3
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftstate, snapshot)
	rf.DPrintf(false, "snapshot() raftstate size = %d", rf.RaftStateSize())
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate request vote
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// replier's msg
	Term        int
	VoteGranted bool // candidate是否获得投票
}

// appendEntries RPC
type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int // index of log entry immediately preceding new ones
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with XTerm (if any)
	XLen    int // length of the conflicting entry

}

// NOTE: snapshot RPC, raftstate's parameters, not snapshots
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
	// Not need to implement offset mechanism for splitting up the snapshot
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
// Invoked by candidates to gather votes
/*
* 1. Reply false if term < currentTerm
* 2. If votedFor is null or candidateId, and candidate’s log is at
* least as up-to-date as receiver’s log, grant vote
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf(false, "rf-[%d] Get RequestVote from rf-[%d]", rf.me, args.CandidateId)
	reply.Term = rf.CurrentTerm
	lastIndex := rf.GetLastLogEntry().Index
	lastTerm := rf.GetLastLogEntry().Term
	// currentTerm > candidate's term, reject to vote
	// currentTerm < candidate's term, convert to follower
	// if rf.currentTerm == args.Term, directly return
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm < args.Term {
		rf.Role = FOLLOWER
		rf.CurrentTerm = args.Term // candidate's term
		rf.VotedFor = -1           // 重置选票至过渡状态
		rf.persist()
	} else {
		return
	}
	// Check candidate's log is at least as up-to-date as receiver's log
	var validateCandidateLog func() bool
	validateCandidateLog = func() bool {
		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
			return true
		}
		return false
	}
	// 检查选票是否在过渡状态
	if rf.VotedFor == -1 && validateCandidateLog() {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		// 投票成功，重置选举超时，防止不符合的candidate阻塞潜在leader
		rf.persist()
		rf.ElectionTimeout = GetElectionTimeout()
		rf.DPrintf(false, "rf-[%d] Role = %d, VoteFor = %d, Term = %d", rf.me, rf.Role, args.CandidateId, rf.CurrentTerm)
	}
}

/*
* RPC handler
* Invoked by leader to replicate log entries; also used as heartbeat
* 1. Reply false if term < currentTerm
* 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
* 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
* 4. Append any new entries not already in the log
* 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// State 1: reset election timeout, prevent from election while having leader
	rf.DPrintf(false, "rf-[%d], Role = %d, Get AppendEntries from leader-[%d], LastIncludeIndex = %d, CurrentTerm = %d, PrevLogIndex = %d, PrevLogTerm = %d, LeaderTerm = %d",
		rf.me, rf.Role, args.LeaderId, rf.LastIncludedIndex, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Term)

	reply.Term = rf.CurrentTerm
	rf.ElectionTimeout = GetElectionTimeout()
	// State 2: 发送方的term小于接收方的term / 发送方prevLogIndex处的日志条目已经被接收方压缩删除
	// 如果是后者，XLen=0，sendEntries中NextIndex将被设定为Max(0,1)即1，并在下次TrySendEntries中发送快照
	if args.Term < rf.CurrentTerm || rf.LastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		return
	}
	// State 3: reply find potential leader, update term and convert to follower
	if rf.CurrentTerm < args.Term || rf.Role == CANDIDATE {
		reply.Success = true
		rf.Role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1 // reset vote, dont vote anymore
		rf.persist()
		rf.cond.Broadcast() // 唤醒election thread
	}
	// State 4: replier log inconsistency, fill in XTerm, XIndex, XLen and retry
	if rf.GetLastLogEntry().Index < args.PrevLogIndex || rf.GetLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.XLen = rf.GetLastLogEntry().Index
		if rf.GetLastLogEntry().Index >= args.PrevLogIndex {
			reply.XTerm = rf.GetLogEntry(args.PrevLogIndex).Term
			reply.XIndex = args.PrevLogIndex
			// 向前搜索寻找!=Xterm的日志条目
			for reply.XIndex > rf.LastIncludedIndex && rf.GetLogEntry(reply.XIndex).Term == reply.XTerm {
				reply.XIndex--
			}
			// 找到第一个不匹配的日志索引
			reply.XIndex++
		}
		rf.DPrintf(false, "rf-[%d], Role = %d, AppendEntries fail because of inconsistency, XLen = %d, XTerm = %d, XIndex = %d", rf.me, rf.Role, reply.XLen, reply.XTerm, reply.XIndex)
		return
	}
	// State 5: pass log consistency check, merge sender's log with local log
	// 1. 发送方日志为本地日志子集，不作处理
	// 2. 不重合时，将本地日志置为发送方日志
	// 3. 特殊情况：发送方连续发送不同长度的日志，且短日志更晚到达，此时不能将本地日志置为发送方日志，会使本地日志回退（日志只能递增）

	reply.Success = true
	for index, entry := range args.Entries {
		// 日志不重合
		if rf.GetLastLogEntry().Index < entry.Index || rf.GetLogEntry(entry.Index).Term != entry.Term {
			var log []LogEntry
			for i := rf.LastIncludedIndex + 1; i < entry.Index; i++ {
				log = append(log, rf.GetLogEntry(i))
			}
			log = append(log, args.Entries[index:]...)
			rf.Log = log
			rf.persist()
			rf.DPrintf(false, "rf-[%d], Role = %d, Append new log = %v", rf.me, rf.Role, rf.Log)
		}
	}
	// State 6: update commitIndex
	if args.LeaderCommitIndex > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommitIndex, rf.GetLastLogEntry().Index)
		rf.DPrintf(false, "rf-[%d] Role = %d, CommitIndex update to %d", rf.me, rf.Role, rf.CommitIndex)
	}
}

/*
* Receiver implementation:
* 1. Reply immediately if term < currentTerm
* 2. Create new snapshot file if first chunk (offset is 0)
* 3. Write data into snapshot file at given offset
* 4. Reply and wait for more data chunks if done is false
* 5. Save snapshot file, discard any existing or partial snapshot
* with a smaller index
* 6. If existing log entry has same index and term as snapshot’s
* last included entry, retain log entries following it and reply
* 7. Discard the entire log
* 8. Reset state machine using snapshot contents (and load
* snapshot’s cluster configuration)
 */

// Not need to split snapshot into chunks
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	// Need to reset election timeout, because may be another election
	rf.ElectionTimeout = GetElectionTimeout()
	// 1. Check  whether to InstallSnapshot
	// rf.Term need to be equal to leader's term
	if rf.CurrentTerm > args.Term || args.LastIncludedIndex < rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	if rf.CurrentTerm < args.Term || rf.Role == CANDIDATE {
		rf.Role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
		rf.cond.Broadcast()
	}
	// 2. saveStateAndSnapshot to persister
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftstate, args.Snapshot)
	// rf.DPrintf(true,"InstallSnapshot() raftstate size = %d", rf.RaftStateSize())

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	lastLogEntry := rf.GetLastLogEntry()

	// 3. If  agrs.LastIncludedIndex < local LastIncludedIndex, delete log entries whose index <= args.LastIncludedIndex
	// And use ApplyMsg to send snapshot to applyCh in order to apply snapshot in state machine
	if args.LastIncludedIndex < lastLogEntry.Index {
		entry := rf.GetLogEntry(args.LastIncludedIndex)
		if entry.Term == args.LastIncludedTerm {
			var log []LogEntry
			// NOTE:check the boundary 😄
			for i := entry.Index + 1; i <= lastLogEntry.Index; i++ {
				log = append(log, rf.GetLogEntry(i))
			}
			rf.Log = log
			rf.persist()
			rf.DPrintf(false, "rf-[%d] InstallSnapShot success, LastIncludeIndex = %d, condition: delete logs whose index <= args.LastIncludedIndex", rf.me, rf.LastIncludedIndex)
			rf.mu.Unlock()
			// Can't use channel with lock,
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Snapshot,
				SnapshotIndex: args.LastIncludedIndex,
				SnapshotTerm:  args.LastIncludedTerm,
			}
			rf.DPrintf(false, "rf-[%d] apply snapshot", rf.me)
			return
		}
	}
	// 4. If  args.LastIncludedIndex >= local LastIncludedIndex, discard all the local logs
	// And use ApplyMsg to send snapshot to applyCh
	rf.Log = make([]LogEntry, 0)
	rf.persist()
	rf.DPrintf(false, "rf-[%d] InstallSnapShot Success, LastIncludeIndex = %d, condition: discard all the local logs", rf.me, rf.LastIncludedIndex)
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.DPrintf(false, "rf-[%d] apply snapshot", rf.me)
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// Client向Raft Server发送命令command

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).

	_, isLeader := rf.GetState()
	if !isLeader || rf.killed() {
		rf.DPrintf(false, "rf-[%d] Start() Fail, isLeader = %t, isKilled = %t", rf.me, isLeader, rf.killed())
		return -1, -1, false
	}
	// leader创建日志条目并通过心跳通知所有follower写入日志
	rf.mu.Lock()
	logEntry := LogEntry{
		Command: command,
		Term:    rf.CurrentTerm,
		Index:   rf.GetLastLogEntry().Index + 1,
	}
	rf.Log = append(rf.Log, logEntry)
	rf.persist()
	rf.mu.Unlock()
	rf.DPrintf(false, "rf-[%d] Start(), Index = %d, Term = %d, Command = %d", rf.me, logEntry.Index, logEntry.Term, logEntry.Command)

	// Lab3: leader一开始要快速提交Client发送的Operation，以便通过速度测试
	atomic.StoreInt32(&rf.quickCommitCheck, 5)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendEntries(i, true)
		}
	}
	return logEntry.Index, logEntry.Term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use ki	lled() to
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
// heartsBeats recently.
func (rf *Raft) ticker() {
	// 循环执行raft集群任务
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.updateLastApplied()
		// role可能会被多个Goroutine读写访问，需要加锁
		// NOTE: atomic.StoreInt32(&role,int32(rf.Role)) 仅能保证赋值操作是原子的，不能保证rf.Role的读取是原子的
		rf.mu.Lock()
		role := rf.Role
		rf.mu.Unlock()
		// 在任务函数中处理完元数据 / 在耗时操作前 记得解锁，防止死锁
		switch role {
		case LEADER:
			rf.doLeaderTask()
		case CANDIDATE:
			rf.doCandidateTask() // candidate to follower or leader or election timeout
		case FOLLOWER:
			if rf.doFollowerTask() {
				continue // follower to candidate
			}
		default:
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
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	rf.DPrintf(false, "rf-[%d] is Making, Len(peers) = %d", me, len(peers))
	rf.BroadcastTime = BROADCAST_TIME
	rf.ElectionTimeout = GetElectionTimeout() // 初始化，随机选举超时
	rf.Role = FOLLOWER                        // 初始化为follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1 // 未投票状态
	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))

	rf.mu.Unlock()

	// initialize from state persisted after  crash
	rf.readPersist(persister.ReadRaftState())

	// keep running ticker goroutine for  elections
	go rf.ticker()

	return rf
}

/*
* atomic operation, don't need to add lock
* 1. Lab3：初始阶段快速提交
* 	一开始leader需要更快速地提交Client发送的Operations，休眠时间更短，以便通过速度测试
* 2. Lab2：稳定阶段正常提交
* 	当日志被大多数follower复制时，leader恢复到正常心跳频率
* 3. quickCommitCheck只能递减，因为只有初期快速提交才需要
 */
func (rf *Raft) doLeaderTask() {
	// Lab3
	if atomic.LoadInt32(&rf.quickCommitCheck) > 0 {
		rf.updateCommitIndex()
		time.Sleep(time.Millisecond) // 快速提交阶段，强制睡眠
		atomic.AddInt32(&rf.quickCommitCheck, -1)
	} else {
		// Lab2
		rf.trySendEntries(false) // false代表是否为leader第一次发送日志
		rf.updateCommitIndex()
		time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond) // 强制睡眠，实现心跳间隔
	}
}

func (rf *Raft) doFollowerTask() bool {
	rf.mu.Lock()
	rf.DPrintf(false, "rf-[%d] 's ElectionTimeout = %d", rf.me, rf.ElectionTimeout)
	// 检查是否即将超时，electionTimeout<100ms
	if rf.ElectionTimeout < rf.BroadcastTime {
		rf.Role = CANDIDATE
		rf.DPrintf(false, "rf-[%d] is ElectionTimeout, convert to CANDIDATE", rf.me)
		rf.mu.Unlock()
		// change to candidate, continue loop
		return true
	}
	// 未超时，继续等待
	rf.ElectionTimeout -= rf.BroadcastTime
	rf.mu.Unlock()
	time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond) // 心跳间隔
	return false
}

/*
* To begin an election, a follower increments its current
* term and transitions to candidate state. It then votes for
* itself and issues RequestVote RPCs in parallel to each of
* the other servers in the cluster. A candidate continues in
* this state until one of three things happens: (a) it wins the
* election, (b) another server establishes itself as leader, or
* (c) a period of time goes by with no winner. These out-
* comes are discussed separately in the paragraphs below.
* Three goroutine
* 1. RequestVote RPC
* 2. timeout goroutine
* 3. main goroutine to check election state
* Use cond to notify main goroutine
 */
func (rf *Raft) doCandidateTask() {
	// prepare for election
	rf.mu.Lock()
	votesGet := 1 // 得票数
	rf.CurrentTerm++
	rf.VotedFor = rf.me // 投票给自己
	rf.persist()
	rf.ElectionTimeout = GetElectionTimeout() // 重置选举超时
	term := rf.CurrentTerm
	electionTimeout := rf.ElectionTimeout
	lastLogIndex := rf.GetLastLogEntry().Index
	lastLogTerm := rf.GetLastLogEntry().Term
	rf.mu.Unlock()
	rf.DPrintf(false, "rf-[%d] start election, term = %d", rf.me, term)
	// State1：无锁并发RequestVote RPC
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				rf.DPrintf(false, "rf-[%d] send RequestVote to server [%d]", rf.me, server)
				if !rf.sendRequestVote(server, &args, &reply) {
					// 发送失败，广播唤醒主线程
					rf.cond.Broadcast()
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock() // only lock in goroutine
				// check 选票
				if reply.VoteGranted {
					votesGet++
				}
				// 立刻转换为follower
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.persist()
					rf.Role = FOLLOWER
					rf.ElectionTimeout = GetElectionTimeout()
				}
				rf.cond.Broadcast()
			}(i)
		}
	}
	// State2：在主线程中运行超时goroutine，检测candidate是否运行超时，将timeout原子置1记录，并唤醒主线程提醒超时
	var timeout int32
	go func(electionTimeout int, timeout *int32) {
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		atomic.StoreInt32(timeout, 1)
		rf.cond.Broadcast()
	}(electionTimeout, &timeout)

	// State3：由主线程判断选举是否结束

	// check election state：currentTerm, State, isTimeout
	var validateElectionState func() bool
	validateElectionState = func() bool {
		if rf.CurrentTerm == term &&
			rf.Role == CANDIDATE &&
			atomic.LoadInt32(&timeout) == 0 {
			return true
		}
		return false
	}
	for {
		rf.mu.Lock()
		// 选举尚未结束
		if votesGet <= len(rf.peers)/2 && validateElectionState() {
			rf.cond.Wait() // 主线程等待被唤醒
		}
		if !validateElectionState() {
			rf.mu.Unlock()
			return
		}
		if votesGet > len(rf.peers)/2 {
			rf.DPrintf(false, "rf-[%d] is voted as Leader, term = [%d]", rf.me, rf.CurrentTerm)
			rf.Role = LEADER
			// Reinitialize leader's log state
			rf.CommitIndex = 0
			for i := 0; i < len(rf.peers); i++ {
				rf.MatchIndex[i] = 0                             // optimistical initialization
				rf.NextIndex[i] = rf.GetLastLogEntry().Index + 1 // pessimistical initialization
			}
			rf.mu.Unlock()
			rf.trySendEntries(true) // first time to send log entries
			break
		}
		rf.mu.Unlock()
	}
	rf.DPrintf(false, "Candidate rf-[%d] finishes election", rf.me)
}

/*
* Commit Restriction
* If there exists an N such that N > commitIndex, a majority
* of matchIndex[i] ≥ N, and log[N].term == currentTerm:
* set commitIndex = N
 */
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommitIndex := rf.CommitIndex
	for N := rf.CommitIndex + 1; N <= rf.GetLastLogEntry().Index; N++ {
		if N > rf.LastIncludedIndex && rf.GetLogEntry(N).Term == rf.CurrentTerm {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				// 已经复制过的日志
				if rf.MatchIndex[i] >= N {
					count++
					// 超过半数即可
					if count > len(rf.peers)/2 {
						newCommitIndex = N
						break
					}
				}
			}
		}
	}
	// 新的提交日志
	rf.CommitIndex = newCommitIndex
	rf.DPrintf(false, "[%d] update CommitIndex, term = %d, NextIndex is %v, MatchIndex is %v, CommitIndex is %d", rf.me, rf.CurrentTerm, rf.NextIndex, rf.MatchIndex, rf.CommitIndex)
}

/*
* If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
* 1. 检查LastApplied<CommitIndex
* 2. 以ApplyMsg形式发送到rf.applych
* 3.应用log[LastApplied]到state machine
* 4. update LastApplied
 */
func (rf *Raft) updateLastApplied() {
	rf.mu.Lock()
	rf.LastApplied = Max(rf.LastApplied, rf.LastIncludedIndex)
	// 防止发送被日志压缩删除的条目：LastApplied >= LastIncludedIndex
	for rf.LastApplied < rf.CommitIndex && rf.LastApplied < rf.GetLastLogEntry().Index && rf.LastApplied >= rf.LastIncludedIndex {
		// 应用LastApplied+1的日志
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.GetLogEntry(rf.LastApplied + 1).Command,
			CommandIndex: rf.LastApplied + 1,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.LastApplied++
		rf.DPrintf(false, "[%d] apply msg [%d] success", rf.me, rf.LastApplied+1)
	}
	rf.mu.Unlock()
}

// 尝试执行心跳rpc / 日志复制 / 快照复制
// firstSendEntries: 成为leader后初次调用
func (rf *Raft) trySendEntries(firstSendEntries bool) {
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		nextIndex := rf.NextIndex[i]
		firstLogIndex := rf.GetFirstLogEntry().Index
		lastLogIndex := rf.GetLastLogEntry().Index
		if i != rf.me {
			// lastLogIndex >= nextIndex，说明leader可能有新日志需要复制 || 成为leader后首次调用
			if lastLogIndex >= nextIndex || firstSendEntries {
				// SendEntries更新目标节点日志
				if firstLogIndex <= nextIndex {
					go rf.sendEntries(i, true)
				} else {
					// SendSnapshot更新落后的目标节点
					go rf.sendSnapshot(i)
				}
			} else {
				// SendHeartbeat
				go rf.sendEntries(i, false)
			}
			// if firstLogIndex >= nextIndex {
			// 	rf.mu.Unlock()
			// 	go rf.sendSnapshot(i)
			// }else{
			// 	rf.mu.Unlock()
			// 	go rf.sendEntries(i, true)
			// }
		}
	}
}

/*
* The consistency check acts as an induction
* step: the initial empty state of the logs satisfies the Log
* Matching Property, and the consistency check preserves
* the Log Matching Property whenever logs are extended.
* As a result, whenever AppendEntries returns successfully,
* the leader knows that the follower’s log is identical to its
* own log up through the new entries.
*
* In Lab2, heartbeat is similar to sendEntries, but not need to fill in entries and wait for reply.success
* In fact, it is a special case of sendEntries, so we can use sendEntries to implement heartbeat
* newEntriesFlag: true means sendEntries, false means heartbeat
 */
func (rf *Raft) sendEntries(server int, newEntries bool) {
	// NOTE: 想象滑动窗口
	finish := false
	// sendEntries loop or send one heartbeat
	for !finish {
		rf.mu.Lock()
		// 1. 判断当前是否仍为leader
		if rf.Role != LEADER {
			rf.mu.Unlock()
			return
		}
		// 2. 无新日志
		if rf.NextIndex[server] <= rf.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.CurrentTerm
		leaderCommit := rf.CommitIndex
		prevLogIndex := rf.NextIndex[server] - 1
		prevLogTerm := rf.GetLogEntry(prevLogIndex).Term
		entries := rf.Log[prevLogIndex-rf.LastIncludedIndex:]
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		if newEntries {
			// SendEntries
			args = AppendEntriesArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
				Entries:           entries,
				LeaderCommitIndex: leaderCommit}
			reply = AppendEntriesReply{}
			rf.DPrintf(false, "rf-[%d] send entries to server [%d], prevLogIndex = [%d], prevLogTerm = [%d], lastIncludeIndex = [%d]",
				rf.me, server, prevLogIndex, prevLogTerm, rf.LastIncludedIndex)
		} else {
			// SendHeartbeat
			args = AppendEntriesArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
				LeaderCommitIndex: leaderCommit}
			reply = AppendEntriesReply{}
			rf.DPrintf(false, "rf-[%d] send heartBeat to server [%d], prevLogIndex = [%d], prevLogTerm = [%d], lastIncludeIndex = [%d]",
				rf.me, server, prevLogIndex, prevLogTerm, rf.LastIncludedIndex)
		}
		// 3. AppendEntries RPC
		finish = true
		rf.mu.Unlock()
		if !rf.sendAppendEntries(server, &args, &reply) {
			return
		}
		rf.mu.Lock()
		// 4: RPC-Result: found a larger peer's term
		// current peer convert to follower and update term equal to candidate's term
		if reply.Term > rf.CurrentTerm {
			rf.Role = FOLLOWER
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1 // reset vote
			rf.persist()
			rf.ElectionTimeout = GetElectionTimeout()
			rf.mu.Unlock()
			return
		}
		// 5: RPC-Result: Log Conflict
		// If AppendEntries fails because of log confilct: decrement nextIndex and retry
		// Case 1: leader doesn't have XTerm:
		//				 nextIndex = XIndex
		// Case 2: leader has XTerm:
		// 				nextIndex = leader's last entry for XTerm
		// Case 3: follower's log is too short:
		//				nextIndex = XLen
		if !reply.Success {
			// Case 3
			if reply.XLen < prevLogIndex {
				rf.NextIndex[server] = Max(reply.XLen, 1) // prevent nextIndex < 0
			} else {
				newNextIndex := prevLogIndex
				// 向前搜索匹配的日志条目
				for newNextIndex > rf.LastIncludedIndex && rf.GetLogEntry(newNextIndex).Term > reply.XTerm {
					newNextIndex--
				}
				// Case 2
				if rf.GetLogEntry(newNextIndex).Term == reply.XTerm {
					// 防止回退到snapshot之前的日志
					rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex+1)
				} else {
					// Case 1
					// help leader can quick find follower expected log
					rf.NextIndex[server] = reply.XIndex
				}
			}
			rf.DPrintf(false, "rf-[%d] send entires, rf.NextIndex[%d] update to %d", rf.me, server, rf.NextIndex[server])
			finish = false // rpc failed, retry
			// 6. Heartbeat don't need to wait for reply.success, return directly
			if !newEntries {
				rf.mu.Unlock()
				return
			}
		} else {
			// 7: RPC-Result: AppendEntries Success
			// 发送方连续发送不同长度日志的AppendEntries，且短日志更晚到达，
			// 利用Max使得NextIndex及MatchIndex单调增长，同时忽略短日志
			if !newEntries {
				rf.mu.Unlock()
				return
			}
			rf.NextIndex[server] = Max(rf.NextIndex[server], prevLogIndex+len(entries)+1)
			rf.MatchIndex[server] = Max(rf.MatchIndex[server], prevLogIndex+len(entries))
			rf.DPrintf(false, "rf-[%d] AppendEntries success, NextIndex = %v, MatchIndex = %v", rf.me, rf.NextIndex, rf.MatchIndex)
		}
		rf.mu.Unlock()
	}
}

// leader send snapshot to follower
func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Snapshot:          rf.persister.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.DPrintf(false, "rf-[%d] sendSnapshot to rf-[%d], LastIncludedIndex = %d", rf.me, server, rf.LastIncludedIndex)
	rf.mu.Unlock()
	if !rf.sendInstallSnapshot(server, &args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check replier's term to determine whether convert to follower
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.Role = FOLLOWER
		rf.VotedFor = -1
		rf.persist()
		rf.ElectionTimeout = GetElectionTimeout() // each time convert Role, reset election timeout
	}
	// Because of sending snapshots ( before rf.LastIncludedIndex's log ), need to update nextIndex and matchIndex
	rf.NextIndex[server] = rf.LastIncludedIndex + 1 // nextIndex use optismistic strategy
	rf.MatchIndex[server] = rf.LastIncludedIndex    // matchIndex use pessimistic strategy
}

// use in lab3
func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

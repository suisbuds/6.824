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
	FAILED                 = -1
	LEADER                 = 1
	CANDIDATE              = 2
	FOLLOWER               = 3
	BROADCAST_TIME         = 100 // 限制为每秒十次心跳
	ELECTION_TIMEOUT_BASE  = 250 // broadcastTime ≪ electionTimeout ≪ MTBF
	ELECTION_TIMEOUT_RANGE = 250
)

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
	// 当leader开始处理日志时，快速检查是否有新日志需要提交，加速CommitIndex的更新，提高日志提交的及时性
	quickCommitCheck int32
	// name string
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// initialize in Make()
	BroadcastTime   int // 心跳间隔
	ElectionTimeout int // 随机选举超时
	Log             []LogEntry
	Role            int // 角色
	CurrentTerm     int
	VotedFor        int
	NextIndex       []int // 即将发送到server的日志
	MatchIndex      []int // 已经复制到server的日志

	// lazy init
	CommitIndex       int // 已提交的日志
	LastApplied       int // 已应用的日志
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// conflict entry
	XTerm  int // term in the conflicting entry
	XIndex int // index of first entry with XTerm
	XLen   int // length of the conflicting entry
}

// snapshot RPC

type InstallSnapshotArgs struct{}

type InstallSnapshotReply struct{}

// example RequestVote RPC handler.
/*
1. Reply false if term < currentTerm
2. If votedFor is null or candidateId, and candidate’s log is at
	least as up-to-date as receiver’s log, grant vote
*/
// Invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	LastIndex := rf.GetLastLogEntry().Index
	LastTerm := rf.GetLastLogEntry().Term
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
	var ValidateLeader func() bool
	ValidateLeader = func() bool {
		if LastTerm < args.LastLogTerm || (LastTerm == args.LastLogTerm && LastIndex <= args.LastLogIndex) {
			return true
		}
		return false
	}
	// 检查选票是否在过渡状态
	if rf.VotedFor == -1 && ValidateLeader() {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		// 投票成功，重置选举超时，防止不符合的candidate阻塞潜在leader
		rf.ElectionTimeout = GetElectionTimeout()
		rf.persist()
		rf.DPrintf("[%d %d] vote for %d, term = %d", rf.me, rf.Role, args.CandidateId, rf.CurrentTerm)
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
	rf.DPrintf("[%d-%d] Get AppendEntries from leader [%d], LastIncludeIndex = %d, CurrentTerm = %d, PrevLogIndex = %d, PrevLogTerm = %d, LeaderTerm = %d)\n",
		rf.me, rf.Role, args.LeaderId, rf.LastIncludedIndex, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Term)
	reply.Term = rf.CurrentTerm
	rf.ElectionTimeout = GetElectionTimeout()
	// State 2: 发送方的term小于接收方的term；发送方prevLogIndex处的日志条目已经被接收方压缩删除
	// 如果是后者，XLen=0，sendEntries中NextIndex将被设定为Max(0,1)即1，并在下次TrySendEntries中发送快照
	if args.Term < rf.CurrentTerm || rf.LastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		return
	}
	// State 3: reply find potential leader, update term and convert to follower
	if rf.CurrentTerm < args.Term || rf.Role != CANDIDATE {
		reply.Success = true
		rf.Role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1    // reset vote, dont vote anymore
		rf.cond.Broadcast() // 唤醒election thread
		rf.persist()
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
			reply.XIndex += 1
		}
		rf.DPrintf("[%d-%d] AppendEntries fail because of inconsistency, XLen = %d, XTerm = %d, XIndex = %d", rf.me, rf.Role, reply.XLen, reply.XTerm, reply.XIndex)
		return
	}
	// State 5: pass log consistency check, merge sender's log with local log
	// 1. 发送方日志为本地日志子集，不作处理
	// 2. 不重合时，将本地日志置为发送方日志
	// 3. 特殊情况：发送方连续发送不同长度的日志，且短日志更晚到达，此时不能将本地日志置为发送方日志，会使本地日志回退（日志只能递增）
	reply.Success = true
	for index, entry := range args.Entries {
		// 日志不重合
		if rf.GetLastLogEntry().Index < entry.Index ||
		entry.Term != rf.GetLogEntry(entry.Index).Term {
			var log []LogEntry
			for i := rf.LastIncludedIndex + 1; i < entry.Index; i++ {
				log = append(log, rf.GetLogEntry(i))
			}
			log = append(log, args.Entries[index:]...)
			rf.Log = log
			rf.persist()
			rf.DPrintf("[%d-%d] Append new log %v", rf.me, rf.Role, rf.Log)
		}
	}
	// State 6: update commitIndex
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommit, rf.GetLastLogEntry().Index)
		rf.DPrintf("[%d-%d] CommitIndex update to %d\n", rf.me, rf.Role, rf.CommitIndex)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {}

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
// 客户端向Raft服务器发送命令command
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	_, isLeader := rf.GetState()
	// 不是leader无法Start
	if !isLeader || rf.killed() {
		rf.DPrintf("[%d] Start() Fail isLeader = %t, isKilled = %t", rf.me, isLeader, rf.killed())
		return -1, -1, false
	}

	// Your code here (2B).
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
	rf.DPrintf("[%d] Start(), index [%d], term [%d], command is %d", rf.me, logEntry.Index, logEntry.Term, logEntry.Command)
	// leader一开始要更频繁地发送日志条目 / 心跳
	atomic.StoreInt32(&rf.quickCommitCheck, 20)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.SendEntries(i, true)
		}
	}
	return logEntry.Index, logEntry.Term, isLeader
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
// heartsBeats recently.
func (rf *Raft) ticker() {
	// 无限执行raft集群任务
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.UpdateLastApplied()
		// role需要加锁，可能会被多个Goroutine访问
		rf.mu.Lock()
		role := rf.Role
		rf.mu.Unlock()
		// 在任务函数中处理完元数据 / 在耗时操作前 记得解锁，防止死锁
		switch role {
		case LEADER:
			rf.DoLeaderTask()
		case CANDIDATE:
			rf.DoCandidateTask() // candidate to follower or leader or election timeout
		case FOLLOWER:
			if rf.DoFollowerTask() {
				continue // follower to candidate
			}
		default:
			// rf.mu.Unlock()
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
	rf.DPrintf("[%d] is Making, len(peers) = %d\n", me, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.BroadcastTime = BROADCAST_TIME
	rf.ElectionTimeout = GetElectionTimeout() // 初始化，随机选举超时
	rf.Role = FOLLOWER                        // 初始化为follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1 // 未投票状态
	rf.MatchIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

/*
* Customized Functions
*
 */

func (rf *Raft) DoLeaderTask() {
	// atomic operation, don't need to add lock
	/*
		1. 初始阶段快速提交
			一开始leader需要更频繁地发送日志条目 / 心跳，确保日志尽快复制到大多数followers
		2. 稳定阶段正常提交
			当日志被大多数follower复制时，leader恢复到正常心跳频率
		3. quickCommitCheck只能递减，因为只有初期才需要
	*/
	if atomic.LoadInt32(&rf.quickCommitCheck) > 0 {
		rf.UpdateCommitIndex()
		atomic.AddInt32(&rf.quickCommitCheck, -1)
		time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond)
	} else {
		rf.TrySendEntries(false) // false代表是否为leader第一次发送日志
		rf.UpdateCommitIndex()
		time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond) // 强制睡眠，实现心跳间隔
	}
}

func (rf *Raft) DoFollowerTask() bool {
	rf.mu.Lock()
	rf.DPrintf("[%d] 's ElectionTimeout = %d\n", rf.me, rf.ElectionTimeout)
	// 检查是否即将超时，electionTimeout<100ms
	if rf.ElectionTimeout < rf.BroadcastTime {
		rf.Role = CANDIDATE
		rf.DPrintf("[%d] is ElectionTimeout, convert to CANDIDATE\n", rf.me)
		rf.mu.Unlock()
		// change to candidate, continue loop
		return true
	}
	// 未超时，继续等待
	rf.ElectionTimeout -= rf.BroadcastTime
	rf.mu.Unlock()
	time.Sleep(time.Duration(rf.BroadcastTime) * time.Millisecond) // 心跳间隔睡眠
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
 */
func (rf *Raft) DoCandidateTask() {
	// prepare for election
	rf.mu.Lock()
	rf.CurrentTerm++
	votesGet := 1       // 得票数
	rf.VotedFor = rf.me // 投票给自己
	rf.persist()
	rf.ElectionTimeout = GetElectionTimeout() // 重置选举超时
	term := rf.CurrentTerm
	electionTimeout := rf.ElectionTimeout
	lastLogIndex := rf.GetLastLogEntry().Index
	lastLogTerm := rf.GetLastLogEntry().Term
	rf.mu.Unlock()
	rf.DPrintf("[%d] start election, term = %d\n", rf.me, term)
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
				rf.DPrintf("[%d] send RequestVote to server [%d]\n", rf.me, server)
				if !rf.sendRequestVote(server, &args, &reply) {
					// 发送失败，终止对应协程，cond唤醒主线程
					rf.cond.Broadcast()
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check 是否获得选票
				if reply.VoteGranted {
					votesGet++
				}
				if reply.Term > rf.CurrentTerm {
					// 立刻转换为follower并重置超时
					rf.CurrentTerm = reply.Term
					rf.Role = FOLLOWER
					rf.ElectionTimeout = GetElectionTimeout()
					rf.persist()
				}
				rf.cond.Broadcast()
			}(i)
		}
	}
	// State2：超时结束goroutine，并唤醒主线程提醒超时
	var timeout rune
	go func(electionTimeout int, timeout *rune) {
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		// atomic operation
		atomic.StoreInt32(timeout, 1)
		rf.cond.Broadcast()
	}(electionTimeout, &timeout)

	// State3：主线程循环判断选举是否结束

	// 判断选举状态：currentTerm, State, isTimeout
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
			rf.cond.Wait() // 主线程等待
		}
		if !validateElectionState() {
			rf.mu.Unlock()
			break
		}
		if votesGet > len(rf.peers)/2 {
			rf.DPrintf("[%d] is voted as Leader, term is [%d]\n", rf.me, rf.CurrentTerm)
			rf.Role = LEADER
			// Reinitialize leader's log state
			rf.CommitIndex = 0
			for i := 0; i < len(rf.peers); i++ {
				rf.MatchIndex[i] = 0
				rf.NextIndex[i] = rf.GetLastLogEntry().Index + 1
			}
			rf.mu.Unlock()
			rf.TrySendEntries(true) // first time to send log entries
			break
		}
		rf.mu.Unlock()
	}
	rf.DPrintf("Candidate [%d] finishes election", rf.me)
}

/*
If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
1. 检查LastApplied<CommitIndex
2. 以ApplyMsg形式发送到rf.applych
3.应用log[LastApplied]到state machine
4. update LastApplied
*/
func (rf *Raft) UpdateLastApplied() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		rf.DPrintf("[%d] apply msg [%d] success", rf.me, rf.LastApplied+1)
		rf.LastApplied++
	}
}

/*
* If there exists an N such that N > commitIndex, a majority
* of matchIndex[i] ≥ N, and log[N].term == currentTerm:
* set commitIndex = N
 */
func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newCommitIndex := rf.CommitIndex
	for N := rf.CommitIndex + 1; N < rf.GetLastLogEntry().Index; N++ {
		if N > rf.LastIncludedIndex && rf.Log[N].Term == rf.CurrentTerm {
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
	rf.DPrintf("[%d] update CommitIndex, term = %d, NextIndex is %v, MatchIndex is %v, CommitIndex is %d", rf.me, rf.CurrentTerm, rf.NextIndex, rf.MatchIndex, rf.CommitIndex)
}

// 尝试执行心跳rpc / 日志复制 / 快照复制
// initialize: 成为leader后初次调用
func (rf *Raft) TrySendEntries(initialize bool) {
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		nextIndex := rf.NextIndex[i]
		firstLogIndex := rf.GetFirstLogEntry().Index
		lastLogIndex := rf.GetLastLogEntry().Index
		rf.mu.Unlock()
		if i != rf.me {
			// 成为leader后首次调用
			// lastLogIndex >= nextIndex，说明本地有新日志需要复制到目标节点
			if lastLogIndex >= nextIndex || initialize {
				// SendEntries更新目标节点
				if firstLogIndex <= nextIndex {
					go rf.SendEntries(i, true)
				} else {
					// 目标节点的日志远远落后，需要SendSnapshot更新
					go rf.SendSnapshot(i)
				}
			} else {
				// heartbeat保活
				go rf.SendEntries(i, false)
			}
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
func (rf *Raft) SendEntries(server int, newEntriesFlag bool) {
	finish := false
	// sendEntries loop or send one heartbeat
	for !finish {
		rf.mu.Lock()
		// 判断当前是否仍为leader
		if rf.Role != LEADER {
			rf.mu.Unlock()
			return
		}
		// 无需要发送的新日志
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
		if newEntriesFlag {
			args = AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit}
			reply = AppendEntriesReply{}
			rf.DPrintf("[%d] send entries to server [%d], prevLogIndex = [%d], prevLogTerm = [%d], lastIncludeIndex = [%d]\n",
				rf.me, server, prevLogIndex, prevLogTerm, rf.LastIncludedIndex)
		} else {
			// 无需发送日志，所以不用填充entry
			args = AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit}
			reply = AppendEntriesReply{}
			rf.DPrintf("[%d] send heartBeat to server [%d]\n", rf.me, server)
		}
		finish = true // start appendEntries rpc
		rf.mu.Unlock()
		//  try appendEntries rpc
		if !rf.sendAppendEntries(server, &args, &reply) {
			return
		}
		rf.mu.Lock()
		// RPC STATE 1: found a larger term
		// current peer convert to follower and update term equal to candidate's term
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Role = FOLLOWER
			rf.VotedFor = -1 // reset vote
			rf.ElectionTimeout = GetElectionTimeout()
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// RPC STATE 2: log inconsistency
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		if !reply.Success {
			// case 1: follower's log is too short
			if reply.XLen < prevLogIndex {
				rf.NextIndex[server] = Max(reply.XLen, 1) // prevent nextIndex < 0
			} else {
				newNextIndex := prevLogIndex
				// 向前搜索匹配的日志条目
				for newNextIndex > rf.LastIncludedIndex &&
					rf.GetLogEntry(newNextIndex).Term > reply.XTerm {
					newNextIndex--
				}
				//  case 2: leader has xTerm
				if rf.GetLogEntry(newNextIndex).Term == reply.XTerm {
					// 防止回退到snapshot之前的日志
					rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex+1)
				} else {
					// case 3: leader don't have xTerm
					// help leader can quick find follower expected log
					rf.NextIndex[server] = reply.XIndex
				}
			}
			rf.DPrintf("[%d] send entires, rf.NextIndex[%d] update to %d", rf.me, server, rf.NextIndex[server])
			finish = false // rpc failed, retry
			if !newEntriesFlag {
				// Heartbeat don't need to wait for reply.success, break directly
				rf.mu.Unlock()
				break
			}
		} else {
			// RPC STATE 3: appendEntries success
			// 发送方连续发送不同长度日志的AppendEntries，且短日志更晚到达，
			// 利用Max使得NextIndex及MatchIndex单调增长，同时忽略短日志
			rf.NextIndex[server] = Max(rf.NextIndex[server], prevLogIndex+len(entries)+1)
			rf.MatchIndex[server] = Max(rf.MatchIndex[server], prevLogIndex+len(entries))
			rf.DPrintf("[%d] appendEntries success, NextIndex is %v, MatchIndex is %v", rf.me, rf.NextIndex, rf.MatchIndex)
		}
		rf.mu.Unlock()
	}
}

// 快照复制
func (rf *Raft) SendSnapshot(server int) {}

/*
* @suisbuds
* Deprecated function
--------------------------------

func (rf *Raft) SendHeartbeat(server int) {
	rf.mu.Lock()
	if rf.Role != LEADER {
	rf.mu.Unlock()
	return
	}
	if rf.NextIndex[server] <= rf.LastIncludedIndex {
	rf.mu.Unlock()
	return
	}
	currentTerm := rf.CurrentTerm
	leaderCommit := rf.CommitIndex
	prevLogIndex := rf.NextIndex[server] - 1
	prevLogTerm := rf.GetLogEntry(prevLogIndex).Term
	// 无需发送日志，所以不用填充entry
	args := AppendEntriesArgs{
	Term:         currentTerm,
	LeaderId:     rf.me,
	PrevLogIndex: prevLogIndex,
	PrevLogTerm:  prevLogTerm,
	LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	rf.DPrintf("[%d] send heartBeat to server [%d]\n", rf.me, server)
	if !rf.sendAppendEntries(server, &args, &reply) {
	return
	}
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
	rf.CurrentTerm = reply.Term
	rf.Role = FOLLOWER
	rf.VotedFor = -1 //  reset vote
	rf.ElectionTimeout = GetElectionTimeout()
	rf.persist()
	rf.mu.Unlock()
	return
	}
	// Dont need to wait for reply.success
	if !reply.Success {
		if reply.XLen < prevLogIndex {
			rf.NextIndex[server] = Max(reply.XLen, 1)
		} else {
			newNextIndex := prevLogIndex
			for newNextIndex > rf.LastIncludedIndex &&
				rf.GetLogEntry(newNextIndex).Term > reply.XTerm {
				newNextIndex--
			}
			if rf.GetLogEntry(newNextIndex).Term == reply.XTerm {
				rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex+1)
			} else {
				rf.NextIndex[server] = reply.XIndex
			}
		}
		rf.DPrintf("[%d] send heartbeat to server [%d], rf.NextIndex[%d] update to %d", rf.me, server, server, rf.NextIndex[server])
	}
	rf.mu.Unlock()
}

func (rf *Raft) SendEntries(server int) {
	finish := false
	// sendEntries loop
	for !finish {
		rf.mu.Lock()
		// 判断当前是否仍为leader
		if rf.Role != LEADER {
			rf.mu.Unlock()
			return
		}
		// 无需要发送的新日志
		if rf.NextIndex[server] <= rf.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.CurrentTerm
		leaderCommit := rf.CommitIndex
		prevLogIndex := rf.NextIndex[server] - 1
		prevLogTerm := rf.GetLogEntry(prevLogIndex).Term
		// 需要发送的日志
		entries := rf.Log[prevLogIndex-rf.LastIncludedIndex:]
		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit}
		reply := AppendEntriesReply{}
		finish = true // start appendEntries rpc
		rf.mu.Unlock()
		rf.DPrintf("[%d] send entries to server [%d], prevLogIndex = [%d], prevLogTerm = [%d], lastIncludeIndex = [%d]\n",
			rf.me, server, prevLogIndex, prevLogTerm, rf.LastIncludedIndex)
		//  try appendEntries rpc
		if !rf.sendAppendEntries(server, &args, &reply) {
			return
		}
		rf.mu.Lock()
		// RPC STATE 1: found a larger term
		// current peer convert to follower and update term equal to candidate's term
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Role = FOLLOWER
			rf.ElectionTimeout = GetElectionTimeout()
			rf.VotedFor = -1 // reset vote
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// RPC STATE 2: log inconsistency
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		if !reply.Success {
			// case 1: follower's log is too short
			if reply.XLen < prevLogIndex {
				rf.NextIndex[server] = Max(reply.XLen, 1) // prevent nextIndex < 0
			} else {
				newNextIndex := prevLogIndex
				// 向前搜索匹配的日志条目
				for newNextIndex > rf.LastIncludedIndex &&
					rf.GetLogEntry(newNextIndex).Term > reply.XTerm {
					newNextIndex--
				}
				//  case 2: leader has xTerm
				if rf.GetLogEntry(newNextIndex).Term == reply.XTerm {
					// 防止回退到snapshot之前的日志
					rf.NextIndex[server] = Max(newNextIndex, rf.LastIncludedIndex+1)
				} else {
					// case 3: leader dont have xTerm
					// help leader can quick find follower expected log
					rf.NextIndex[server] = reply.XIndex
				}
			}
			rf.DPrintf("[%d] send entires, rf.NextIndex[%d] update to %d", rf.me, server, rf.NextIndex[server])
			finish = false // rpc failed, retry
		} else {
			// RPC STATE 3: appendEntries success
			// 发送方连续发送不同长度日志的AppendEntries，且短日志更晚到达，
			// 利用Max使得NextIndex及MatchIndex单调增长，同时忽略短日志
			rf.NextIndex[server] = Max(rf.NextIndex[server], prevLogIndex+len(entries)+1)
			rf.MatchIndex[server] = Max(rf.MatchIndex[server], prevLogIndex+len(entries))
			rf.DPrintf("[%d] appendEntries success, NextIndex is %v, MatchIndex is %v", rf.me, rf.NextIndex, rf.MatchIndex)
		}
		rf.mu.Unlock()
	}
}

--------------------------------
*
*/

package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func (rf *Raft) DPrintf(debug bool, format string, a ...interface{}) (n int, err error) {
	if Debug {
		if debug {
			log.Printf(format, a...)
		}
	}
	return
}

/*
* Raft uses randomized election timeouts to ensure that
* split votes are rare and that they are resolved quickly. To
* prevent split votes in the first place, election timeouts are
* chosen randomly from a fixed interval (e.g., 150–300ms).
* This spreads out the servers so that in most cases only a
* single server will time out; it wins the election and sends
* heartbeats before any other servers time out. The same
* mechanism is used to handle split votes. Each candidate
* restarts its randomized election timeout at the start of an
* election, and it waits for that timeout to elapse before
* starting the next election; this reduces the likelihood of
* another split vote in the new election.
 */

func GetElectionTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return ELECTION_TIMEOUT_BASE + int(rand.Int31n(ELECTION_TIMEOUT_RANGE))
}

func (rf *Raft) GetLogEntry(index int) LogEntry {
	// 要考虑被日志压缩的条目
	if index == 0 {
		return LogEntry{Term: -1, Index: 0}
	} else if index == rf.LastIncludedIndex {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	} else {
		return rf.Log[index-rf.LastIncludedIndex-1]
	}
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	// 如果日志为空，返回快照的最后一个日志条目
	if len(rf.Log) == 0 {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}
	return rf.Log[len(rf.Log)-1]
}

func (rf *Raft) GetFirstLogEntry() LogEntry {
	if len(rf.Log) == 0 {
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}
	return rf.Log[0]
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

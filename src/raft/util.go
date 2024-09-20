package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 随机选举超时
func GerElectionTimeout() int {
	rand.Seed(time.Now().UnixNano())
	return ELECTIONTIMEOUTBASE + int(rand.Int31n(ELECTIONTIMEOUTRANGE))
}

// 获取最后一个日志条目
func (rf *Raft) LastLogEntry() LogEntry {
	// 如果日志为空，返回快照的最后一个日志条目
	if len(rf.Log)==0{
		return LogEntry{Term: rf.LastIncludedTerm, Index: rf.LastIncludedIndex}
	}
	return rf.Log[len(rf.Log)-1]
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

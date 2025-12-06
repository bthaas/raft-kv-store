package raft

import "keyvalue/internal/wal"

// RequestVoteArgs invoked by candidates to gather votes.
type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the response to RequestVoteArgs.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs invoked by leader to replicate log entries; also used as heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []wal.LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the response to AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

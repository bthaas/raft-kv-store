package raft

import (
	"keyvalue/internal/wal"
	"testing"
)

func TestElectionTimer(t *testing.T) {
	// This test just ensures NewRaftNode starts a timer and doesn't panic
	applyCh := make(chan wal.LogEntry)
	node := NewRaftNode("node1", []string{}, applyCh)
	defer node.Stop()
}

func TestAppendEntries(t *testing.T) {
	applyCh := make(chan wal.LogEntry)
	node := NewRaftNode("node1", []string{"node2"}, applyCh)
	defer node.Stop()

	// 1. Heartbeat from new leader
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}
	reply := &AppendEntriesReply{}

	if err := node.AppendEntries(args, reply); err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	if !reply.Success {
		t.Errorf("Expected heartbeat success, got false")
	}
	if node.CurrentTerm != 1 {
		t.Errorf("Expected CurrentTerm=1, got %d", node.CurrentTerm)
	}

	// 2. Append new entry
	entries := []wal.LogEntry{{Term: 1, Command: "CMD1"}}
	args = &AppendEntriesArgs{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
	}
	reply = &AppendEntriesReply{}

	if err := node.AppendEntries(args, reply); err != nil {
		t.Fatalf("AppendEntries Append failed: %v", err)
	}

	if !reply.Success {
		t.Errorf("Expected append success, got false")
	}
	if len(node.Log) != 2 { // Dummy(0) + CMD1
		t.Errorf("Expected Log len=2, got %d", len(node.Log))
	}
	if node.Log[1].Command != "CMD1" {
		t.Errorf("Expected log[1]=CMD1, got %s", node.Log[1].Command)
	}

	// 3. Consistency Check Fail (PrevLogIndex too high)
	args.PrevLogIndex = 10
	reply = &AppendEntriesReply{}
	node.AppendEntries(args, reply)
	if reply.Success {
		t.Errorf("Expected consistency failure (index too high), got success")
	}

	// 4. Conflict Replaced
	// Append a conflicting entry at index 1
	node.mu.Lock()
	node.Log[1] = wal.LogEntry{Term: 2, Command: "CONFLICT"}
	node.mu.Unlock()

	// Leader sends original CMD1 (Term 1) at index 1
	args.PrevLogIndex = 0
	args.PrevLogTerm = 0
	args.Entries = []wal.LogEntry{{Term: 1, Command: "CMD1"}}
	reply = &AppendEntriesReply{}

	node.AppendEntries(args, reply)
	if !reply.Success {
		t.Errorf("Expected overwrite success, got failure")
	}
	if node.Log[1].Command != "CMD1" {
		t.Errorf("Expected log[1] overwritten to CMD1, got %s", node.Log[1].Command)
	}
}

func TestStart(t *testing.T) {
	applyCh := make(chan wal.LogEntry)
	node := NewRaftNode("leader", []string{}, applyCh)
	defer node.Stop()

	// Become leader
	node.becomeLeader()

	// Start command
	idx, term, isLeader := node.Start("cmd1", "k", "v")
	if !isLeader {
		t.Fatalf("Expected isLeader=true")
	}
	if idx != 1 { // 0 is dummy
		t.Fatalf("Expected index=1, got %d", idx)
	}
	if term != node.CurrentTerm {
		t.Fatalf("Expected term=%d, got %d", node.CurrentTerm, term)
	}

	if len(node.Log) != 2 || node.Log[1].Command != "cmd1" {
		t.Errorf("Log not updated correctly")
	}
}

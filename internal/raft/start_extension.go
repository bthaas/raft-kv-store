package raft

import (
	"fmt"
	"keyvalue/internal/wal"
)

// Start appends a command to the log. Returns true if this node is the leader.
func (rn *RaftNode) Start(command string, key string, value string) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.State != Leader {
		return -1, -1, false
	}

	entry := wal.LogEntry{
		Command: command,
		Key:     key,
		Value:   value,
		Term:    rn.CurrentTerm,
	}
	rn.Log = append(rn.Log, entry)
	index := len(rn.Log) - 1
	rn.MatchIndex[rn.id] = index
	rn.NextIndex[rn.id] = index + 1

	fmt.Printf("[%s] Start: command=%s, key=%s, term=%d, index=%d\n", rn.id, command, key, rn.CurrentTerm, index)

	return index, rn.CurrentTerm, true
}

func (rn *RaftNode) StartSet(key, value string) (int, int, bool) {
	return rn.Start("SET", key, value)
}

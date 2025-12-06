package raft

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"keyvalue/internal/wal"
)

// State represents the current state of the Raft node.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftNode represents a single node in the Raft cluster.
type RaftNode struct {
	mu sync.RWMutex

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    string
	Log         []wal.LogEntry

	// Volatile state on all servers
	CommitIndex int
	LastApplied int
	State       State

	// Volatile state on leaders
	NextIndex  map[string]int
	MatchIndex map[string]int

	// Timers
	ElectionTimer *time.Timer

	// Channels
	stopCh  chan struct{}
	ApplyCh chan wal.LogEntry

	// Apply trigger
	applyCond *sync.Cond

	// Node Info
	id    string
	peers []string // List of peer addresses
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(id string, peers []string, applyCh chan wal.LogEntry) *RaftNode {
	rn := &RaftNode{
		id:       id,
		peers:    peers,
		State:    Follower,
		VotedFor: "",
		stopCh:   make(chan struct{}),
		ApplyCh:  applyCh,
		// Dummy entry at index 0 to make math easier (1-based indexing)
		Log: []wal.LogEntry{{Term: 0}},
	}
	rn.applyCond = sync.NewCond(&rn.mu)

	// Initialize with a random election timeout to minimize split votes
	rn.resetElectionTimer()
	go rn.runElectionTimer()
	go rn.applyLogs()
	return rn
}

// Stop stops the Raft node.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-rn.ElectionTimer.C:
			rn.mu.Lock()
			state := rn.State
			rn.mu.Unlock()

			if state == Leader {
				rn.sendAppendEntries()
				rn.resetElectionTimer()
			} else {
				rn.startElection()
			}
		case <-rn.stopCh:
			return
		}
	}
}

func (rn *RaftNode) resetElectionTimer() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.resetElectionTimerLocked()
}

func (rn *RaftNode) resetElectionTimerLocked() {
	var duration time.Duration
	if rn.State == Leader {
		duration = 100 * time.Millisecond // Heartbeat interval
	} else {
		// Random timeout between 150-300ms for election
		duration = time.Duration(150+rand.Intn(150)) * time.Millisecond
	}

	if rn.ElectionTimer == nil {
		rn.ElectionTimer = time.NewTimer(duration)
	} else {
		if !rn.ElectionTimer.Stop() {
			select {
			case <-rn.ElectionTimer.C:
			default:
			}
		}
		rn.ElectionTimer.Reset(duration)
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.State = Candidate
	rn.CurrentTerm++
	rn.VotedFor = rn.id
	fmt.Printf("[%s] Starting election for term %d\n", rn.id, rn.CurrentTerm)
	rn.mu.Unlock()

	rn.resetElectionTimer()

	// Mock sending RequestVote RPCs to peers
	go rn.broadcastRequestVote()
}

// broadcastRequestVote sends RequestVote RPCs to all peers in parallel.
func (rn *RaftNode) broadcastRequestVote() {
	rn.mu.RLock()
	term := rn.CurrentTerm
	id := rn.id
	lastLogIndex := len(rn.Log) - 1
	lastLogTerm := rn.Log[lastLogIndex].Term
	rn.mu.RUnlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1
	votesNeeded := (len(rn.peers)+1)/2 + 1
	var voteMu sync.Mutex

	for _, peer := range rn.peers {
		go func(p string) {
			var reply RequestVoteReply
			if ok := rn.sendRequestVote(p, &args, &reply); ok {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if rn.State != Candidate || rn.CurrentTerm != term {
					return
				}

				if reply.Term > term {
					rn.CurrentTerm = reply.Term
					rn.State = Follower
					rn.VotedFor = ""
					rn.resetElectionTimerLocked()
					return
				}

				if reply.VoteGranted {
					voteMu.Lock()
					votesReceived++
					if votesReceived >= votesNeeded {
						rn.becomeLeader()
					}
					voteMu.Unlock()
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) sendRequestVote(peer string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		fmt.Printf("[%s] Dialing %s failed: %v\n", rn.id, peer, err)
		return false
	}
	defer client.Close()
	err = client.Call("RaftNode.RequestVote", args, reply)
	if err != nil {
		fmt.Printf("[%s] RequestVote RPC to %s failed: %v\n", rn.id, peer, err)
		return false
	}
	return true
}

func (rn *RaftNode) becomeLeader() {
	// Already leader?
	if rn.State == Leader {
		return
	}

	rn.State = Leader
	rn.NextIndex = make(map[string]int)
	rn.MatchIndex = make(map[string]int)

	lastLogIndex := len(rn.Log) - 1
	for _, peer := range rn.peers {
		rn.NextIndex[peer] = lastLogIndex + 1
		rn.MatchIndex[peer] = 0
	}
	rn.MatchIndex[rn.id] = lastLogIndex

	fmt.Printf("[%s] Became Leader for term %d\n", rn.id, rn.CurrentTerm)

	// Must release lock before sending heartbeats to avoid deadlock if sendAppendEntries needs it (it does for reading state)
	// But `rn.mu` is held by caller `broadcastRequestVote` -> `rn.mu.Lock` inside check.
	// WAIT. `broadcastRequestVote` takes lock inside the closure.
	// Yes, `rn.mu.Lock()` is held when calling `rn.becomeLeader()`.
	// So we are safe.
	// However, `sendAppendEntries` acquires RLock.
	// RLock cannot be acquired if Lock is held.
	// So we must NOT call `sendAppendEntries` synchronously while holding Lock.
	// We should just reset timer (to immediate heartbeat) or run it in goroutine.

	// DO NOT call sendAppendEntries directly while holding Lock.
	// Instead, force the heartbeat timer to fire immediately?
	// Or launch go routine.

	go rn.sendAppendEntries()
	rn.resetElectionTimerLocked()
}

func (rn *RaftNode) sendAppendEntries() {
	rn.mu.RLock()
	if rn.State != Leader {
		rn.mu.RUnlock()
		return
	}

	// Copy necessary state to avoid holding lock during RPCs
	term := rn.CurrentTerm
	id := rn.id
	commitIndex := rn.CommitIndex
	peers := make([]string, len(rn.peers))
	copy(peers, rn.peers)

	// Map iteration needs lock? Yes for NextIndex
	// Create a copy of NextIndex and MatchIndex for this round of RPCs
	nextIndices := make(map[string]int)
	for k, v := range rn.NextIndex {
		nextIndices[k] = v
	}
	rn.mu.RUnlock()

	for _, peer := range peers {
		go func(p string) {
			rn.mu.RLock()
			nextIdx := rn.NextIndex[p] // Use the current NextIndex for this peer
			prevLogIndex := nextIdx - 1
			if prevLogIndex < 0 {
				prevLogIndex = 0
			}

			var prevLogTerm int
			if prevLogIndex < len(rn.Log) {
				prevLogTerm = rn.Log[prevLogIndex].Term
			}

			entries := make([]wal.LogEntry, 0)
			if nextIdx < len(rn.Log) {
				entries = append(entries, rn.Log[nextIdx:]...)
			}
			rn.mu.RUnlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			fmt.Printf("[%s] Sending AE to %s: PLI=%d, Entries=%d, Commit=%d\n", id, p, prevLogIndex, len(entries), commitIndex)
			var reply AppendEntriesReply

			if ok := rn.callAppendEntries(p, &args, &reply); ok {
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if reply.Term > rn.CurrentTerm {
					rn.CurrentTerm = reply.Term
					rn.State = Follower
					rn.VotedFor = ""
					rn.resetElectionTimerLocked()
					return
				}

				if rn.State != Leader {
					return
				}

				if reply.Success {
					rn.MatchIndex[p] = prevLogIndex + len(entries)
					rn.NextIndex[p] = rn.MatchIndex[p] + 1
					fmt.Printf("[%s] AE Success from %s. Match=%d\n", rn.id, p, rn.MatchIndex[p])
					rn.updateCommitIndex()
				} else {
					// Backtrack
					rn.NextIndex[p]--
					if rn.NextIndex[p] < 1 {
						rn.NextIndex[p] = 1
					}
				}
			}
			// fmt.Printf("[%s] Sending AppendEntries to %s: PrevLogIndex=%d, EntriesLen=%d, Args=%+v\n",
			// 	rn.id, p, prevLogIndex, len(entries), args) // Removed this line as it's now handled by RPC success/failure
		}(peer)
	}
}

func (rn *RaftNode) callAppendEntries(peer string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		// fmt.Printf("[%s] Dialing %s failed (AppendEntries): %v\n", rn.id, peer, err)
		return false
	}
	defer client.Close()
	if err := client.Call("RaftNode.AppendEntries", args, reply); err != nil {
		fmt.Printf("[%s] AppendEntries to %s failed: %v\n", rn.id, peer, err)
		return false
	}
	return true
}

func (rn *RaftNode) updateCommitIndex() {
	// This function must be called with Lock held
	matchIndices := make([]int, 0, len(rn.peers)+1)
	matchIndices = append(matchIndices, len(rn.Log)-1) // Include self's log length as its match index
	for _, peer := range rn.peers {
		matchIndices = append(matchIndices, rn.MatchIndex[peer])
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndices)))

	// Majority index
	// If we have N nodes, majority is (N/2) + 1.
	// If sorted descending, the element at index (N-1)/2 is the smallest index that a majority has matched.
	// Example: 5 nodes. Majority 3. Indices: [10, 8, 7, 5, 3]. (5-1)/2 = 2. matchIndices[2] = 7.
	// 3 nodes (10, 8, 7) are >= 7. Correct.
	// Example: 3 nodes. Majority 2. Indices: [5, 3, 1]. (3-1)/2 = 1. matchIndices[1] = 3.
	// 2 nodes (5, 3) are >= 3. Correct.
	N := matchIndices[len(matchIndices)/2]

	if N > rn.CommitIndex && rn.Log[N].Term == rn.CurrentTerm {
		rn.CommitIndex = N
		fmt.Printf("[%s] CommitIndex updated to %d\n", rn.id, rn.CommitIndex)
		rn.applyCond.Signal()
	}
}

// AppendEntries RPC handler.
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Success = false
	reply.Term = rn.CurrentTerm

	if args.Term < rn.CurrentTerm {
		return nil
	}

	if args.Term > rn.CurrentTerm {
		rn.CurrentTerm = args.Term
		rn.State = Follower
		rn.VotedFor = ""
	}

	rn.resetElectionTimerLocked() // Heartbeat received

	// Consistency Check
	if args.PrevLogIndex >= len(rn.Log) {
		return nil // Log doesn't contain prev entry
	}
	if rn.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return nil // Term mismatch
	}

	// Conflict Resolution and Append
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index >= len(rn.Log) {
			rn.Log = append(rn.Log, args.Entries[i:]...)
			break
		}
		if rn.Log[index].Term != entry.Term {
			rn.Log = rn.Log[:index]
			rn.Log = append(rn.Log, args.Entries[i:]...)
			break
		}
	}

	reply.Success = true

	// Follower Commit Logic
	if args.LeaderCommit > rn.CommitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		// commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit < lastNewIndex {
			rn.CommitIndex = args.LeaderCommit
		} else {
			rn.CommitIndex = lastNewIndex
		}
		fmt.Printf("[%s] Follower CommitIndex updated to %d\n", rn.id, rn.CommitIndex)
		rn.applyCond.Signal()
	}

	return nil
}

// RequestVote RPC handler.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term > rn.CurrentTerm {
		rn.CurrentTerm = args.Term
		rn.State = Follower
		rn.VotedFor = ""
	}

	reply.Term = rn.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rn.CurrentTerm {
		return nil
	}

	// Check if candidate's log is at least as up-to-date as follower's log
	lastLogIndex := len(rn.Log) - 1
	lastLogTerm := rn.Log[lastLogIndex].Term
	
	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	if (rn.VotedFor == "" || rn.VotedFor == args.CandidateId) && logUpToDate {
		rn.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rn.resetElectionTimerLocked()
	}

	return nil
}

func (rn *RaftNode) applyLogs() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for {
		for rn.CommitIndex <= rn.LastApplied {
			// Wait until commitIndex updates
			rn.applyCond.Wait()
		}

		select {
		case <-rn.stopCh:
			return
		default:
		}

		rn.LastApplied++
		entry := rn.Log[rn.LastApplied]
		fmt.Printf("[%s] Applying entry at index %d: %s\n", rn.id, rn.LastApplied, entry.Command)

		rn.mu.Unlock() // Unlock while sending to channel to avoid deadlock
		rn.ApplyCh <- entry
		rn.mu.Lock()
	}
}

package raft

import (
	"cloud-storage/internal/metadata/raft/models"
	"cloud-storage/internal/metadata/raft/persistence"
	"cloud-storage/internal/metadata/snapshot"
	statemachine "cloud-storage/internal/metadata/state_machine"
	"fmt"
	"sync"
	"time"
)

type RaftNode struct {
	mu sync.Mutex
	id string
	address string
	peers []string

	state RaftNodeState

	currentTerm int
	votedFor *string

	log []models.LogEntry

	commitIndex int
	lastApplied int
	
	lastIncludedIndex int
	lastIncludedTerm int
	
	nextIndex map[string]int
	matchIndex map[string]int

	electionResetEvent time.Time
	voteCount int
	stateMachine *statemachine.StateMachine
	storage persistence.Storage
}


func NewRaftNode(id string, address string, peers []string, storage persistence.Storage) *RaftNode {
	node := &RaftNode{
		id: id,
		address: address,
		peers: peers,
		state: Follower,
		log: []models.LogEntry{},
		nextIndex: make(map[string]int),
		matchIndex: make(map[string]int),
		stateMachine: statemachine.NewStateMachine(),
		storage: storage,
	}

	node.recoverState()

	snap, err := node.storage.LoadSnapshot()
	if snap != nil && err == nil {
		node.lastIncludedIndex = snap.LastIncludedIndex
		node.lastIncludedTerm = snap.LastIncludedTerm

		node.stateMachine.Restore(snap.Data)
		node.commitIndex = snap.LastIncludedIndex
		node.lastApplied = snap.LastIncludedIndex
	}

	return node
}

func (n *RaftNode) Debug() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return fmt.Sprintf("Node=%s State=%v Term=%d",
		n.id, n.state, n.currentTerm)
}


func(n * RaftNode) stepDown(term int) {
	n.currentTerm = term
	n.votedFor = nil
	n.state = Follower
	n.storage.SaveState(n.currentTerm, n.votedFor)

	n.electionResetEvent = time.Now()
	// n.persist();
}

func (n *RaftNode) logState(msg string) {
    fmt.Printf("[%s] state=%v term=%d | %s\n", n.id, n.state, n.currentTerm, msg)
}


func (n *RaftNode) lastLogIndex() int {
	if len(n.log) == 0 {
		return n.lastIncludedIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode) lastLogTerm() int {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNode) isLogUpToDate(lastIndex int, lastTerm int) bool {
	if lastTerm > n.lastLogTerm() { return true }
	if lastTerm == n.lastLogTerm() && lastIndex >= n.lastLogIndex() {
		return true
	}
	return false
}

func (n *RaftNode) recoverState() {
	term, votedFor, err := n.storage.LoadState()
	if err == nil {
		n.currentTerm = term
		n.votedFor = votedFor
	}

	logs, err := n.storage.LoadLog()
	if err == nil {
		n.log = logs
	}
}

func (n *RaftNode) maybeSnapshot() {
	if n.commitIndex - n.lastIncludedIndex < 5  {
		return
	}

	data := n.stateMachine.Snapshot();

	snap := snapshot.Snapshot {
		LastIncludedIndex: n.commitIndex,
		LastIncludedTerm: n.getLogTerm(n.commitIndex),
		Data: data,
	}

	n.storage.SaveSnapshot(snap)

	// truncate log
	offset := n.commitIndex - n.lastIncludedIndex
	n.log = n.log[offset:]

	n.lastIncludedIndex = snap.LastIncludedIndex
	n.lastIncludedTerm = snap.LastIncludedTerm
}

func (n *RaftNode) getLogTerm(index int) int {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	
	if index < n.lastIncludedIndex || index > n.lastLogIndex() {
		return 0
	}

	return n.log[index - n.lastIncludedIndex - 1].Term
}

func (n *RaftNode) canSnapshot() bool {
	for _, peer := range n.peers {
		if n.matchIndex[peer] < n.commitIndex {
			return false
		}
	}
	return true
}
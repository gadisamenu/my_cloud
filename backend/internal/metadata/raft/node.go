package raft

import (
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

	log []LogEntry

	commitIndex int
	lastApplied int
	
	nextIndex map[string]int
	matchIndex map[string]int

	electionResetEvent time.Time
	voteCount int
	stateMachine *statemachine.StateMachine
}


func NewRaftNode(id string, address string, peers []string) *RaftNode {
	return &RaftNode{
		id: id,
		address: address,
		peers: peers,
		state: Follower,
		log: []LogEntry{},
		nextIndex: make(map[string]int),
		matchIndex: make(map[string]int),
		stateMachine: statemachine.NewStateMachine(),
	}
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
	n.persist();
}

func (n *RaftNode) logState(msg string) {
    fmt.Printf("[%s] state=%v term=%d | %s\n", n.id, n.state, n.currentTerm, msg)
}
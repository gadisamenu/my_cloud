package raft

import (
	statemachine "cloud-storage/internal/metadata/state_machine"
	"sync"
	"time"
)

type RaftNode struct {
	mu sync.Mutex
	id string
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


func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id: id,
		peers: peers,
		state: Follower,
		log: []LogEntry{},
		nextIndex: make(map[string]int),
		matchIndex: make(map[string]int),
		stateMachine: statemachine.NewStateMachine(),
	}
}


func(n * RaftNode) stepDown(term int) {
	n.currentTerm = term
	n.votedFor = nil
	n.state = Follower
	n.persist();
}
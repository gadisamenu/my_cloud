package raft

import (
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
}



func(n * RaftNode) stepDown(term int) {
	n.currentTerm = term
	n.votedFor = nil
	n.state = Follower
	n.persist();
}
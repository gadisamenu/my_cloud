package raft

// raft node state
type RaftNodeState int

const (
	Follower RaftNodeState = iota
	Candidate
	Leader
)

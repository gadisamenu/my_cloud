package raft

import "cloud-storage/internal/metadata/raft/models"

type RequestVoteRequest struct {
	Term int
	CandidateID string
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteResponse struct {
	Term int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term int
	LeaderID string
	PrevLogIndex int
	PrevLogTerm int
	Entries []models.LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term int
	Success bool
	ConflictIndex int
	// ConflictTerm int
}
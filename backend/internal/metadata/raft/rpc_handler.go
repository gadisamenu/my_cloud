package raft

import "time"


func (n *RaftNode) HandleRequestVote (req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	//reject old term
	if req.Term < n.currentTerm {
		return RequestVoteResponse {
			VoteGranted: false,
			Term: n.currentTerm,
		}
	}

	// new term -> step down
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	if n.votedFor != nil && *n.votedFor != req.CandidateId {
		return RequestVoteResponse {
			Term: n.currentTerm,
			VoteGranted: false,
		}
	}

	if !n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		return RequestVoteResponse {
			Term: n.currentTerm,
			VoteGranted: false,
		}
	}

	n.votedFor = &req.CandidateId
	n.electionResetEvent = time.Now()
	n.persist()

	return RequestVoteResponse {
		Term: n.currentTerm,
		VoteGranted: true,
	}
}

func (n * RaftNode) HandleAppendEntries() bool {
	n.electionResetEvent = time.Now()
	return true;
}
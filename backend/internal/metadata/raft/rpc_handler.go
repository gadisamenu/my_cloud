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

	if n.votedFor != nil && *n.votedFor != req.CandidateID {
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

	n.votedFor = &req.CandidateID
	n.electionResetEvent = time.Now()
	n.storage.SaveState(n.currentTerm, n.votedFor)
	n.persist()

	return RequestVoteResponse {
		Term: n.currentTerm,
		VoteGranted: true,
	}
}

func (n * RaftNode) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{ Term: n.currentTerm, Success:  false}
	}
	// become follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = nil
		n.state = Follower
		// n.persist()
	}

	n.electionResetEvent = time.Now()

	// check log consistency
	if req.PrevLogIndex > len(n.log) {
		return AppendEntriesResponse{ Term: n.currentTerm, Success:  false}
	}
	
	if req.PrevLogIndex > 0 && 
		n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		 return AppendEntriesResponse{ Term: n.currentTerm, Success:  false}
	}

	// if req.PrevLogIndex > 0 {
	// 	if len(n.log) < req.PrevLogIndex {
	// 		return fail
	// 	}

	// 	if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
	// 		// ❗ conflict detected
	// 		n.log = n.log[:req.PrevLogIndex-1]
	// 		return fail
	// 	}
	// }

	// delete conflicting entries
	n.log = n.log[:req.PrevLogIndex]

	// append new entries
	n.log = append(n.log, req.Entries...)

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, len(n.log))
	}

	return AppendEntriesResponse{ Term: n.currentTerm, Success: true } 
}
package raft

import (
	"time"
)


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
	// n.persist()

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
		n.storage.SaveState(n.currentTerm, n.votedFor)
		// n.persist()
	}

	n.electionResetEvent = time.Now()

	if req.PrevLogIndex < n.lastIncludedIndex {
		// leader is too far behind
		return AppendEntriesResponse{Success: false}
	}

	// check log consistency
	if req.PrevLogIndex > len(n.log) {
		return AppendEntriesResponse{ Term: n.currentTerm, Success:  false}
	}
	
	if req.PrevLogIndex > 0 && 
		n.getLogTerm(req.PrevLogIndex) != req.PrevLogTerm {
		 return AppendEntriesResponse{ Term: n.currentTerm, Success:  false}
	}

	// find first mismatch
	i := 0
	for ; i < len(req.Entries); i++ {
		idx := req.PrevLogIndex + 1 + i

		if idx > len(n.log) {
			break
		}

		if n.getLogTerm(idx) != req.Entries[i].Term {
			break
		}
	}

	// truncate conflicting suffix
	truncateIndex := req.PrevLogIndex + i
	if truncateIndex < len(n.log) {
		n.log = n.log[:truncateIndex]
		n.storage.RewriteLog(n.log)
	}

	// append only NEW entries
	newEntries := req.Entries[i:]
	if len(newEntries) > 0 {
		n.log = append(n.log, newEntries...)
		n.storage.AppendLog(newEntries)
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, len(n.log))
	}

	return AppendEntriesResponse{ Term: n.currentTerm, Success: true } 
}
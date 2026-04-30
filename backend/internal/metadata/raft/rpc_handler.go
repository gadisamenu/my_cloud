package raft

import (
	"fmt"
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
		fmt.Printf("Stepping down latest term %d is found\n", req.Term )
		n.stepDown(req.Term)
		// n.persist()
	}

	n.electionResetEvent = time.Now()

	if req.PrevLogIndex < n.lastIncludedIndex {
		// leader is too far behind
		fmt.Println("Leader is too far behind")
		return AppendEntriesResponse{ Success: false }
	}

	// check log consistency
	if req.PrevLogIndex > n.lastLogIndex(){
		fmt.Println("Index didn't match")
		return AppendEntriesResponse{ Term: n.currentTerm, Success:  false, ConflictIndex: n.lastLogIndex() + 1 }
	}
	
	if req.PrevLogIndex > 0 && 
		n.getLogTerm(req.PrevLogIndex) != req.PrevLogTerm {
		fmt.Printf("Conflicting term prevLogTerm -> %d \n", req.PrevLogTerm)
		return AppendEntriesResponse{ Term: n.currentTerm, Success:  false, ConflictIndex: req.PrevLogIndex }
	}

	// find first mismatch
	entryIdx := 0
	for ; entryIdx < len(req.Entries); entryIdx++ {
		index := req.PrevLogIndex + 1 + entryIdx

		if index > n.lastLogIndex() {
			break
		}

		if n.getLogTerm(index) != req.Entries[entryIdx].Term {
			// truncate conflicting suffix
			truncateIndex := req.PrevLogIndex - n.lastIncludedIndex + entryIdx
			if truncateIndex < len(n.log) {
				n.log = n.log[:truncateIndex]
				n.storage.RewriteLog(n.log)
			}
			break
		}
	}

	// append only NEW entries
	if entryIdx < len(req.Entries){
		newEntries := req.Entries[entryIdx:]
		
		n.log = append(n.log, newEntries...)
		n.storage.AppendLog(newEntries)
	}

	if req.LeaderCommit > n.commitIndex {
		fmt.Printf("Updating commitIndex, leader commit %d and current lastlogIndex %d \n", req.LeaderCommit, n.lastLogIndex() )
		n.commitIndex = min(req.LeaderCommit, n.lastLogIndex())
	}

	return AppendEntriesResponse{ Term: n.currentTerm, Success: true } 
}
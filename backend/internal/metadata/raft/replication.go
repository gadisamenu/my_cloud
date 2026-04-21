package raft

import (
	"cloud-storage/internal/metadata/raft/models"
	"time"
)


func (n *RaftNode) startHeartbeatLoop() {
	ticker := time.NewTicker(50 *time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.sendHeartbeats()
	}
}

// func (n *RaftNode) sendAppendEntriesToAll() {
// 	for _, peer := range n.peers {
// 		go func(p string) {

// 			n.mu.Lock()

// 			nextIdx := n.nextIndex[p]
// 			prevIdx := nextIdx - 1

// 			var prevTerm int
// 			if prevIdx > 0 {
// 				prevTerm = n.log[prevIdx-1].Term
// 			}

// 			entries := n.log[nextIdx-1:]

// 			req := AppendEntriesRequest{
// 				Term:         n.currentTerm,
// 				LeaderID:     n.id,
// 				PrevLogIndex: prevIdx,
// 				PrevLogTerm:  prevTerm,
// 				Entries:      entries,
// 				LeaderCommit: n.commitIndex,
// 			}

// 			n.mu.Unlock()

// 			resp := n.sendAppendEntries(p, req)

// 			n.handleAppendEntriesResponse(p, resp, req)

// 		}(peer)
// 	}
// }

func (n *RaftNode) sendHeartbeats() {
	for _,peer := range n.peers {
		go n.replicateToPeer(peer, nil)
	}
}

func (n *RaftNode) replicateToPeer(peer string, entries []models.LogEntry) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1

	var prevLogTerm int
	if prevLogIndex > 0 && prevLogIndex <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	// entries := n.log[nextIdx-1:]

	req := AppendEntriesRequest {
		Term: n.currentTerm,
		LeaderID: n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.Unlock()

	resp := n.sendAppendEntries(peer, req)
	n.handleAppendEntriesResponse(peer,resp, req)
}


func (n *RaftNode) handleAppendEntriesResponse(peer string, resp AppendEntriesResponse, req AppendEntriesRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader { return }

	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}

	if resp.Success {
		n.matchIndex[peer] = req.PrevLogIndex + len(req.Entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.updateCommitIndex()
	} else {
		// fallback to follower's known state
		if resp.ConflictIndex != 0 {
			n.nextIndex[peer] = resp.ConflictIndex
		} else {
			n.nextIndex[peer]--
			if n.nextIndex[peer] < 1 {
				n.nextIndex[peer] = 1
			}
		}
	}
}
// func (n *RaftNode) updateCommitIndex() {
// 	for i := len(n.log); i > n.commitIndex; i-- {

// 		count := 1 // leader itself

// 		for peer := range n.matchIndex {
// 			if n.matchIndex[peer] >= i {
// 				count++
// 			}
// 		}

// 		if count >= (len(n.peers)+1)/2+1 {
// 			n.commitIndex = i
// 			break
// 		}
// 	}
// }

func (n *RaftNode) updateCommitIndex() {
	for i := n.commitIndex + 1; i <= len(n.log); i++ {
		count := 1 //leader itself

		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				count++
			}
		}

		if count >= (len(n.peers)+1)/2+1 &&
		 	n.log[i-1].Term == n.currentTerm {
				n.commitIndex = i
		}
	}
}


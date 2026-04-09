package raft

import "time"


func (n *RaftNode) startHeartbeatLoop() {
	ticker := time.NewTicker(50 *time.Millisecond)
	defer ticker.Stop()

	for {
		<- ticker.C

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		// n.sendHeartbeats()
	}
}

func (n *RaftNode) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()

	for _,peer := range n.peers {
		go func(peer string) {
			req := AppendEntriesRequest {
				Term: term,
				LeaderID: n.id,
				PrevLogIndex: 0,
				PrevLogTerm: 0,
				Entries: []LogEntry{},
				LeaderCommit: n.commitIndex,
			}

			resp := n.sendAppendEntries(peer, req)
			n.handleAppendEntriesResponse(peer, resp, term)
		}(peer)
	}
}

func (n *RaftNode) handleAppendEntriesResponse(peer string, resp AppendEntriesResponse, term int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term != n.currentTerm {
		return
	}

	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return
	}
}
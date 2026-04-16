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
	for _,peer := range n.peers {
		go n.replicateToPeer(peer)
	}
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
		lastSent := req.PrevLogIndex + len(req.Entries)
		n.matchIndex[peer] = lastSent
		n.nextIndex[peer] = lastSent + 1
		n.updateCommitIndex()
	} else {
		n.nextIndex[peer]--
		if n.nextIndex[peer] < 1 {
			n.nextIndex[peer] = 1
		}
	}
}

func (n *RaftNode) updateCommitIndex() {
	for i := n.commitIndex + 1; i <= len(n.log); i++ {
		count := 1 //leader itself

		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				count++
			}
		}

		if count > len(n.peers)/2 && 
			n.log[i-1].Term == n.currentTerm {
				n.commitIndex = 1
		}
	}
}

func (n *RaftNode) replicateToPeer(peer string) {
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

	entries := n.log[nextIdx-1:]

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
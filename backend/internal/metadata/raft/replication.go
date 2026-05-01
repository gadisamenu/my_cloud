package raft

import (
	"fmt"
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

func (n *RaftNode) sendHeartbeats() {
	for _,peer := range n.peers {
		go n.replicateToPeer(peer)
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
	
	if nextIdx <= n.lastIncludedIndex { // follower is too far behind → needs snapshot (later)
		fmt.Printf("follower %s is too far behind nextIndex: %d,lastIncludedIndex: %d \n",peer, nextIdx, n.lastIncludedIndex)
		n.mu.Unlock()
		return
	}

	prevLogTerm := n.getLogTerm(prevLogIndex)

	start := max(0, nextIdx - n.lastIncludedIndex - 1)

	entries := n.log[start:]

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
		minIndex := n.lastIncludedIndex + 1

		if resp.ConflictIndex != 0 {
			n.nextIndex[peer] = max(minIndex, resp.ConflictIndex)
		} else {
			n.nextIndex[peer]--
			if n.nextIndex[peer] < minIndex {
				n.nextIndex[peer] = minIndex
			}
		}
	}
}

func (n *RaftNode) updateCommitIndex() {
	lastIndex := n.lastLogIndex()

	for i := n.commitIndex + 1; i <= lastIndex; i++ {
		count := 1

		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				count++
			}
		}

		if count >= (len(n.peers)+1)/2+1 &&
			n.getLogTerm(i) == n.currentTerm {
			n.commitIndex = i
		}
	}
}


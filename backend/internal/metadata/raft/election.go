package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (n *RaftNode) startElection() {
	n.logState("starting election")
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm ++
	n.votedFor = &n.id
	n.voteCount = 1;
	n.storage.SaveState(n.currentTerm, n.votedFor)
	// n.persist()

	n.electionResetEvent = time.Now();

	term := n.currentTerm
	lastIndex := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	majority := (len(n.peers)+1)/2 + 1
	n.mu.Unlock()

	if len(n.peers) == 0 {
		n.becomeLeader()
		return
	}

	for _, peer := range n.peers {

		go func(peer string) {
			resp := n.sendRequestVote(peer, RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || term != n.currentTerm {
				return
			}

			if resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				return
			}

			if resp.VoteGranted {
				n.voteCount++
				if n.voteCount >= majority {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *RaftNode) randomElectionTimeout() time.Duration {
	return time.Duration(150 + rand.Intn(150)) * time.Millisecond
}

func (n *RaftNode) runElectionTimer() {
	for {
		timeout	:= n.randomElectionTimeout()

		n.mu.Lock()
		lastReset := n.electionResetEvent
		n.mu.Unlock()

		time.Sleep(timeout);
		n.mu.Lock()
		

		// if state changed ignore
		if n.state == Leader {
			// fmt.Println(n.id, "skipping election, state changed now a leader")
			n.mu.Unlock()
			continue
		}

		// skip if timer was reset
		if lastReset != n.electionResetEvent {
			// fmt.Println(n.id, "skipping election, timer was reset")
			n.mu.Unlock()
			continue
		}
		fmt.Println(n.id, "timeout, starting election")
		n.mu.Unlock()
		n.startElection()
	}
}

func(n *RaftNode) becomeLeader() {
	n.state = Leader
	n.electionResetEvent = time.Now()

	n.logState("became leader")
	// initialize leader sate 
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1
		n.matchIndex[peer] = 0
	}

	go n.startHeartbeatLoop();
}

func (n *RaftNode) Run() {
    go n.runElectionTimer()
	go n.applyLoop()
}
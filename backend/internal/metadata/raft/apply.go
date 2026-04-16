package raft

import (
	statemachine "cloud-storage/internal/metadata/state_machine"
	"time"
)

func (n *RaftNode) applyLoop() {
	for {
		n.mu.Lock()

		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.log[n.lastApplied-1]

			n.mu.Unlock()

			cmd := statemachine.DecodeCommand(entry.Command)
			n.stateMachine.Apply(cmd)

			n.mu.Lock()
		}

		n.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}
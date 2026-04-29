package raft

import (
	statemachine "cloud-storage/internal/metadata/state_machine"
	"fmt"
	"time"
)

func (n *RaftNode) applyLoop() {
	fmt.Printf("Apply loop is called for %s node \n", n.id)
	for {
		n.mu.Lock()

		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.log[n.lastApplied-n.lastIncludedIndex-1]

			n.mu.Unlock()

			cmd := statemachine.DecodeCommand(entry.Command)
			n.stateMachine.Apply(cmd)

			n.mu.Lock()
		}

		n.mu.Unlock()
		if n.lastApplied-n.lastIncludedIndex != 0 && (n.lastApplied-n.lastIncludedIndex) % 5 == 0 {
			// fmt.Printf("Node %s is ready for snapshot , is true %d \n", n.id, n.lastApplied-n.lastIncludedIndex)
			if n.canSnapshot() {
				n.maybeSnapshot()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
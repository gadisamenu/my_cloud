package raft

import (
	"cloud-storage/internal/metadata/raft/models"
	statemachine "cloud-storage/internal/metadata/state_machine"
	"errors"
	"fmt"
)

func (n *RaftNode) Propose(cmd statemachine.Command) error {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return errors.New("not a leader")
	}

	entry := models.LogEntry {
		Term: n.currentTerm,
		Index: len(n.log) + 1,
		Command: statemachine.EncodeCommand(cmd),
	}

	fmt.Printf("proposed a cmd => %v", cmd)

	n.log = append(n.log, entry)
	n.storage.AppendLog([]models.LogEntry{entry})
	
	peers := append([]string{}, n.peers...)

	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
	return nil
}
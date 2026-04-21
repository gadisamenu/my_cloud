package raft

import (
	"cloud-storage/internal/metadata/raft/models"
	statemachine "cloud-storage/internal/metadata/state_machine"
	"errors"
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

	n.log = append(n.log, entry)
	n.storage.AppendLog([]models.LogEntry{entry})
	n.persist()

	n.mu.Unlock()

	return nil
}
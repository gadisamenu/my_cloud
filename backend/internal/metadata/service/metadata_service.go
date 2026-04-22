package service

import (
	"cloud-storage/internal/metadata/raft"
	statemachine "cloud-storage/internal/metadata/state_machine"
	"encoding/json"
)

type MetaDataService struct {
	raftNode *raft.RaftNode
}

func (s *MetaDataService) CreateFile(file statemachine.FileMetadata) error {
	data,_ := json.Marshal(file)

	cmd := statemachine.Command{
		Type: statemachine.CreateFile,
		Data: data,
	}
	return s.raftNode.Propose(cmd)
}
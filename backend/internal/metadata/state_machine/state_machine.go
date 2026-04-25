package statemachine

import (
	"encoding/json"
	"sync"
)

type StateMachine struct {
	mu sync.Mutex
	files map[string]FileMetadata
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		files: make(map[string]FileMetadata),
	}
}


func (sm *StateMachine) Apply(cmd Command) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch cmd.Type {
	case CreateFile:
		var file FileMetadata 
		json.Unmarshal(cmd.Data, &file)
		sm.files[file.ID] = file
	case DeleteFile:
		var fileID string
		json.Unmarshal(cmd.Data, &fileID)
		delete(sm.files, fileID)
	case UpdateChunks:
		var update ChunkUpdate
		json.Unmarshal(cmd.Data, &update)
		file := sm.files[update.FileID]
		file.Chunks = update.Chunks
		sm.files[update.FileID] = file
	}

	return nil
}

func (sm *StateMachine) Snapshot() []byte {
	data, _ := json.Marshal(sm.files)
	return data
}
func (sm *StateMachine) Restore(data []byte) {
	json.Unmarshal(data, &sm.files)
}
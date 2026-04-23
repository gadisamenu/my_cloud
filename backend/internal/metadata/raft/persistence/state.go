package persistence

import (
	"bytes"
	"cloud-storage/internal/metadata/raft/models"
	"encoding/json"
	"os"
)

type State struct {
	Term int
	VotedFor *string
}


func (s *DiskStorage) SaveState(term int, votedFor *string) error {
	state := State {
		Term: term,
		VotedFor: votedFor,
	}

	data,_ := json.Marshal(state);
	return os.WriteFile(s.stateFile, data, 0644)
}

func (s *DiskStorage) LoadState()(int, *string, error) {
	data, err := os.ReadFile(s.stateFile)

	if err != nil {
		return 0, nil, err
	}

	var state State

	json.Unmarshal(data, &state)
	return state.Term, state.VotedFor, nil
}

func (s *DiskStorage) AppendLog(entries []models.LogEntry) error {
	f, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}
	defer f.Close()

	for _, entry := range entries {
		data, _ := json.Marshal(entry)
		f.Write(append(data, '\n'))
	}
	return f.Sync()
}

func (s *DiskStorage)LoadLog()([]models.LogEntry, error) {
	data, err := os.ReadFile(s.logFile)
	if err != nil {
		return nil, err
	}

	var logs []models.LogEntry

	lines := bytes.Split(data, []byte("\n"))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var entry models.LogEntry 
		json.Unmarshal(line, &entry)
		
		logs = append(logs, entry)
	}
	return logs, nil
}

func (s *DiskStorage)RewriteLog(logs []models.LogEntry) error {
	f, err := os.Create(s.logFile)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, entry := range logs {
		data,_ := json.Marshal(entry)
		f.Write(append(data,'\n'))
	}

	return f.Sync()
}
package persistence

import (
	"cloud-storage/internal/metadata/raft/models"
	"cloud-storage/internal/metadata/snapshot"
)

type Storage interface {
	SaveState(term int, votedFor *string) error 
	LoadState()(int, *string, error)
	AppendLog(entries []models.LogEntry) error
	LoadLog()([]models.LogEntry, error)
	RewriteLog([]models.LogEntry) error
	LoadSnapshot()(*snapshot.Snapshot, error)
	SaveSnapshot(snapshot snapshot.Snapshot) error
}
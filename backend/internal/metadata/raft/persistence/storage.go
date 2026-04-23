package persistence

import "cloud-storage/internal/metadata/raft/models"

type Storage interface {
	SaveState(term int, votedFor *string) error 
	LoadState()(int, *string, error)
	AppendLog(entries []models.LogEntry) error
	LoadLog()([]models.LogEntry, error)
	RewriteLog([]models.LogEntry) error
}
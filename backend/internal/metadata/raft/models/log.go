package models

type LogEntry struct {
	Term int
	Index int
	Command []byte //serialized metadata
}
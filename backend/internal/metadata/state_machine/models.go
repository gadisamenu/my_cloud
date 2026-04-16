package statemachine

type FileMetadata struct {
	ID string
	Name string
	Chunks []string
}

type ChunkUpdate struct {
	FileID string
	Chunks []string
}
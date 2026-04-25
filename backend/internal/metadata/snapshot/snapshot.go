package snapshot

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}


package raft


type LogEntry struct {
	Term int
	Index int
	Command []byte //serialized metadata
}


func (n *RaftNode) lastLogIndex() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log) - 1].Index
}

func (n *RaftNode) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNode)isLogUpToDate(lastIndex int, lastTerm int) bool {
	if lastTerm > n.lastLogTerm() { return true }
	if lastTerm == n.lastLogTerm() && lastIndex >= n.lastLogIndex() {
		return true
	}
	return false
}
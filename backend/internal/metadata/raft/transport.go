package raft

func (n *RaftNode) sendRequestVote(peer string, req RequestVoteRequest) RequestVoteResponse {
	return RequestVoteResponse{
		Term: req.Term,
		VoteGranted: true,
	}
}

func (n *RaftNode) sendAppendEntries(peer string, req AppendEntriesRequest) AppendEntriesResponse {
	//gRPC call
	return AppendEntriesResponse{}
}
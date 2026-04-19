package raft

import (
	"encoding/json"
	"net/http"
)

func (n *RaftNode) StartHttpServer() {
	http.HandleFunc("/requestVote", n.handleVoteResponseHTTP)
	go http.ListenAndServe(n.address, nil);
}

func (n *RaftNode) handleVoteResponseHTTP (w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest

	json.NewDecoder(r.Body).Decode(&req)

	resp := n.HandleRequestVote(req)

	json.NewEncoder(w).Encode(resp)
}
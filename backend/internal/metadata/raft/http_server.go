package raft

import (
	statemachine "cloud-storage/internal/metadata/state_machine"
	"encoding/json"
	"net/http"
)

func (n *RaftNode) StartHttpServer() {
	http.HandleFunc("/requestVote", n.handleVoteResponseHTTP)
	http.HandleFunc("/appendEntries", n.handleAppendEntriesHTTP)
	http.HandleFunc("/propose", n.handleProposeHTTP)
	go http.ListenAndServe(n.address, nil);
}

func (n *RaftNode) handleVoteResponseHTTP (w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest

	json.NewDecoder(r.Body).Decode(&req)

	resp := n.HandleRequestVote(req)

	json.NewEncoder(w).Encode(resp)
}

func (n *RaftNode) handleAppendEntriesHTTP (w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest

	json.NewDecoder(r.Body).Decode(&req)

	resp := n.HandleAppendEntries(req)

	json.NewEncoder(w).Encode(resp)
}

func (n *RaftNode) handleProposeHTTP(w http.ResponseWriter, r *http.Request) {
	if n.state != Leader {
		http.Error(w, "not a leader", http.StatusBadRequest)
		return
	}

	var cmd statemachine.Command

	json.NewDecoder(r.Body).Decode(&cmd)
	n.Propose(cmd)
	w.Write([]byte("Ok"))
}
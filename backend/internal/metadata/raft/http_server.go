package raft

import (
	statemachine "cloud-storage/internal/metadata/state_machine"
	"encoding/json"
	"fmt"
	"net/http"
)

func (n *RaftNode) StartHttpServer() {
	http.HandleFunc("/requestVote", n.handleVoteResponseHTTP)
	http.HandleFunc("/appendEntries", n.handleAppendEntriesHTTP)
	http.HandleFunc("/propose", n.handleProposeHTTP)
	http.HandleFunc("/nodeState", n.handleGetNodeStateHTTP)
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
	fmt.Println("<*********** proposed log success  ***********>")
	w.Write([]byte("Ok"))
}

func (n *RaftNode) handleGetNodeStateHTTP(w http.ResponseWriter, r *http.Request) {
	data, _ := json.Marshal(map[string]any{ 
		"lastIncludedIndex": n.lastIncludedIndex,
		"lastIncludedTerm": n.lastIncludedTerm,
		"log": n.log,
		"lastApplied": n.lastApplied,
		"committedIndex": n.commitIndex,
		"nextIndex": n.nextIndex,
		"matchIndex": n.matchIndex,
	})
	w.Write(data)
}
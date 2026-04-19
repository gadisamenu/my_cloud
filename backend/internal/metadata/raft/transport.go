package raft

import (
	"bytes"
	"encoding/json"
	"net/http"
)

func (n *RaftNode) sendRequestVote(peer string, req RequestVoteRequest) RequestVoteResponse {

	url := "http://" + peer + "/requestVote"
	data,_ := json.Marshal(req)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return RequestVoteResponse{
			VoteGranted: false,
		}
	}

	defer resp.Body.Close()
	var result RequestVoteResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return result;
}

func (n *RaftNode) sendAppendEntries(peer string, req AppendEntriesRequest) AppendEntriesResponse {
	url := "http://" + peer + "/appendEntries"

	data,_ := json.Marshal(req)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data));

	if err != nil {
		return AppendEntriesResponse{Success: false}
	}

	defer resp.Body.Close()

	var result AppendEntriesResponse
	json.NewDecoder(resp.Body).Decode(&result)

	return result
}
package raft

import (
	"encoding/json"
	"net/http"
)

func NewRaftHTTPGet(raft *Raft) *RaftHTTPGet {
	return &RaftHTTPGet{raft: raft}
}

type RaftHTTPGet struct {
	raft *Raft
}

func (handler *RaftHTTPGet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if handler.raft == nil {
		w.Write([]byte(`{}`))
		return
	}

	data := make(map[string]interface{})
	data["ID"] = handler.raft.Node.Id()
	data["Leader"] = handler.raft.Node.Leader()
	data["LogDir"] = handler.raft.LogDir
	data["ClusterName"] = handler.raft.Name

	json.NewEncoder(w).Encode(data)
}

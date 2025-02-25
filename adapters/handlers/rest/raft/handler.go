//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/weaviate/weaviate/usecases/schema"
)

// RaftHandler struct implements all the http endpoints for raft related requests
type RaftHandler struct {
	schemaHandler schema.Handler
}

// JoinNodeRequest defines the needed parameter for a node to join a raft cluster
type JoinNodeRequest struct {
	// Node is the ID of the node that will join the cluster.
	// It needs the following format NODE_ID[:NODE_PORT]
	// If NODE_PORT is not specified, default raft interal port will be used
	Node string `json:"node"`
	// Voter is whether or not the node wants to join as a voter in the raft cluster
	Voter bool `json:"voter"`
}

// Validate ensures that r is valid.
// If an error is returned the request should not be used.
func (r JoinNodeRequest) Validate() error {
	if r.Node == "" {
		return fmt.Errorf("node parameter is empty")
	}
	return nil
}

// JoinNode parses the received request and body, ensures that they are valid and then forwards the parameters to the scheme handler that
// will join the node to the cluster.
// If the request is invalid, returns http.StatusBadRequest
// If an internal error occurs, returns http.StatusInternalServerError
func (h RaftHandler) JoinNode(w http.ResponseWriter, r *http.Request) {
	var joinRequest JoinNodeRequest

	// Decode the request body
	err := json.NewDecoder(r.Body).Decode(&joinRequest)
	if err != nil {
		errString := err.Error()
		if errors.Is(err, io.EOF) {
			// Nicer error message than "EOF"
			errString = "request body is empty"
		}
		http.Error(w, errString, http.StatusBadRequest)
		return
	}

	// Verify that the request is valid
	err = joinRequest.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	nodeAddrs := strings.Split(joinRequest.Node, ":")
	// This should not happen with the previous validate step, but better safe than sorry
	if len(nodeAddrs) < 1 {
		http.Error(w, "node parameter is empty", http.StatusBadRequest)
		return
	}
	nodeAddr := nodeAddrs[0]
	nodePort := ""
	if len(nodeAddrs) >= 2 {
		nodePort = nodeAddrs[1]
	}

	// Forward to the handler
	err = h.schemaHandler.JoinNode(context.Background(), nodeAddr, nodePort, joinRequest.Voter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RemoveNodeRequest defines the needed parameter for a node to be removed from the raft cluster.
type RemoveNodeRequest struct {
	// Node is the ID of the node that will be removed from the cluster.
	Node string `json:"node"`
}

// Validate ensures that r is valid.
// If an error is returned the request should not be used.
func (r RemoveNodeRequest) Validate() error {
	if r.Node == "" {
		return fmt.Errorf("node parameter is empty")
	}
	return nil
}

// RemoveNode parses the received request and body, ensures that they are valid and then forwards the parameters to the scheme handler that
// will remove the node from the cluster.
// If the request is invalid, returns http.StatusBadRequest
// If an internal error occurs, returns http.StatusInternalServerError
func (h RaftHandler) RemoveNode(w http.ResponseWriter, r *http.Request) {
	var removeRequest RemoveNodeRequest

	// Decode the request body
	err := json.NewDecoder(r.Body).Decode(&removeRequest)
	if err != nil {
		errString := err.Error()
		if errors.Is(err, io.EOF) {
			// Nicer error message than "EOF"
			errString = "request body is empty"
		}
		http.Error(w, errString, http.StatusBadRequest)
		return
	}

	// Verify that the request is valid
	err = removeRequest.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.schemaHandler.RemoveNode(context.Background(), removeRequest.Node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// StoreSchemaV1 migrate from v2 (RAFT) to v1 (Non-RAFT)
func (h RaftHandler) StoreSchemaV1(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	restore := h.schemaHandler.StoreSchemaV1()
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(restore)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ClusterRouter returns a *mux.Router that will requests starting with "/v1/cluster".
// The schemaHandler is kept in memory internally to forward request to once parsed.
func ClusterRouter(schemaHandler schema.Handler) *http.ServeMux {
	raftHandler := RaftHandler{schemaHandler: schemaHandler}
	r := http.NewServeMux()

	root := "/v1/cluster"
	r.HandleFunc(root+"/join", raftHandler.JoinNode)
	r.HandleFunc(root+"/remove", raftHandler.RemoveNode)
	r.HandleFunc(root+"/schema-v1", raftHandler.StoreSchemaV1)

	return r
}

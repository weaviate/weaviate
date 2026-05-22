//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// GetNamespaces returns the namespaces with the given names. An empty names
// slice returns all known namespaces; missing names are silently omitted.
func (s *Raft) GetNamespaces(names ...string) ([]cmd.Namespace, error) {
	req := cmd.QueryGetNamespacesRequest{Names: names}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_NAMESPACES,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	resp := cmd.QueryGetNamespacesResponse{}
	if err := json.Unmarshal(queryResp.Payload, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return resp.Namespaces, nil
}

// NamespaceCount returns the number of known namespaces. Used by the startup
// invariant check in configure_api.go. Reads local FSM state (no RAFT round
// trip) — safe to call after the meta store is ready.
func (s *Raft) NamespaceCount() int {
	return s.store.namespaceManager.Count()
}

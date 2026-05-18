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
	"errors"
	"fmt"
	"slices"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	clusterUtils "github.com/weaviate/weaviate/usecases/cluster"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// AddNamespace proposes an AddNamespace RAFT command and returns the
// persisted namespace alongside the apply version. The apply side rejects
// duplicates with [namespaces.ErrAlreadyExists] and invalid names with
// [namespaces.ErrBadRequest]. Callers that need a follow-up local read on
// a non-leader node should pass the returned version to WaitForUpdate.
//
// An empty ns.HomeNode is filled from the cluster's storage candidates
// before propose.
func (s *Raft) AddNamespace(ctx context.Context, ns cmd.Namespace) (cmd.Namespace, uint64, error) {
	if ns.HomeNode == "" {
		picked, err := s.nextHomeNode(s.StorageCandidates())
		if err != nil {
			return cmd.Namespace{}, 0, fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
		}
		ns.HomeNode = picked
	}

	req := cmd.AddNamespaceRequest{
		Namespace: ns,
		Version:   cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return cmd.Namespace{}, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_NAMESPACE,
		SubCommand: subCommand,
	}
	version, err := s.Execute(ctx, command)
	if err != nil {
		return cmd.Namespace{}, 0, err
	}
	return ns, version, nil
}

// UpdateNamespace proposes an UpdateNamespace RAFT command and returns the
// apply version. The apply side returns [namespaces.ErrNotFound] when the
// target namespace does not exist, and [namespaces.ErrBadRequest] for an
// empty HomeNode. Only HomeNode is mutable; existing live shards are not
// moved.
func (s *Raft) UpdateNamespace(ctx context.Context, ns cmd.Namespace) (uint64, error) {
	req := cmd.UpdateNamespaceRequest{
		Namespace: ns,
		Version:   cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_NAMESPACE,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// ChangeNamespaceState proposes a ChangeNamespaceState RAFT command and
// returns the apply version. The apply side returns [namespaces.ErrNotFound]
// for unknown namespaces and [namespaces.ErrInvalidStateTransition] for
// forbidden transitions; same-state transitions are idempotent.
func (s *Raft) ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	req := cmd.ChangeNamespaceStateRequest{
		Name:        name,
		TargetState: target,
		Version:     cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// RemoveNamespaceEntity proposes a RemoveNamespaceEntity RAFT command and
// returns the apply version. The apply side returns [namespaces.ErrNotFound]
// for unknown namespaces and [namespaces.ErrInvalidState] when called on an
// active namespace.
func (s *Raft) RemoveNamespaceEntity(ctx context.Context, name string) (uint64, error) {
	req := cmd.RemoveNamespaceEntityRequest{
		Name:    name,
		Version: cmd.NamespaceLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REMOVE_NAMESPACE_ENTITY,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// nextHomeNode returns the next home_node from nodes, rotating across calls.
// The iterator is constructed lazily with StartRandom so cold starts aren't
// biased to nodes[0], and rebuilt whenever the candidate set changes from
// the one the cached iterator was built with — otherwise membership changes
// (node join/leave) would leave the iterator rotating through a stale set,
// silently locking out newly added nodes and still picking removed ones.
func (s *Raft) nextHomeNode(nodes []string) (string, error) {
	if len(nodes) == 0 {
		return "", errors.New("no storage candidates available")
	}

	s.homeNodeIteratorMu.Lock()
	defer s.homeNodeIteratorMu.Unlock()

	if s.homeNodeIterator == nil || !slices.Equal(s.homeNodeCandidates, nodes) {
		it, err := clusterUtils.NewNodeIterator(nodes, clusterUtils.StartRandom)
		if err != nil {
			return "", err
		}
		s.homeNodeIterator = it
		s.homeNodeCandidates = slices.Clone(nodes)
	}
	return s.homeNodeIterator.Next(), nil
}

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
// persisted namespace alongside the apply version. An empty ns.HomeNodes
// is filled from the cluster's storage candidates before propose. The
// apply side rejects duplicates with [namespaces.ErrAlreadyExists] and
// invalid names with [namespaces.ErrBadRequest]. Callers that need a
// follow-up local read on a non-leader node should pass the returned
// version to WaitForUpdate.
func (s *Raft) AddNamespace(ctx context.Context, ns cmd.Namespace) (cmd.Namespace, uint64, error) {
	if len(ns.HomeNodes) == 0 {
		picked, err := s.nextHomeNode(s.StorageCandidates())
		if err != nil {
			return cmd.Namespace{}, 0, fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
		}
		ns.HomeNodes = []string{picked}
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
// target namespace does not exist, and [namespaces.ErrBadRequest] when
// HomeNodes does not contain exactly one entry. Only HomeNodes is mutable;
// existing live shards are not moved.
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

// ChangeNamespaceState proposes an unconditional ChangeNamespaceState RAFT
// command and returns the apply version. The apply side returns
// [namespaces.ErrNotFound] for unknown namespaces and
// [namespaces.ErrInvalidStateTransition] for forbidden transitions;
// same-state transitions are idempotent. Callers that must not overwrite a
// concurrent flip use ChangeNamespaceStateIfUnchanged instead.
func (s *Raft) ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	return s.changeNamespaceState(ctx, name, target, 0)
}

// ChangeNamespaceStateIfUnchanged proposes the flip only if the namespace's
// state has not changed since this call read it.
//
// Execute re-proposes when leadership is lost, without knowing whether the
// first attempt committed. Suspend and resume can each undo the other, so an
// unconditional retry can revert a flip that succeeded in between — and that
// flip's caller was already told it worked. The retry carries the index read
// here, so the apply refuses it instead. The refused caller gets
// [namespaces.ErrStateChangedConcurrently] and re-reads before deciding
// again. Returns [namespaces.ErrNotFound] when the namespace does not exist.
//
// The read happens once, here, above Execute. Reading inside Execute's retry
// would fetch a fresh index on every re-propose, which would always match
// and leave the precondition doing nothing. The value itself need not be
// current: the apply compares against authoritative state, so an old index
// can only cause a spurious refusal, never a wrong accept.
func (s *Raft) ChangeNamespaceStateIfUnchanged(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	expectedIndex, err := s.stateChangeIndex(name)
	if err != nil {
		return 0, err
	}
	return s.changeNamespaceState(ctx, name, target, expectedIndex)
}

// stateChangeIndex reads the namespace's current StateChangeIndex. Anything
// other than exactly one row is [namespaces.ErrNotFound]: GetNamespaces
// omits names it cannot find, and returning 0 for a missing namespace would
// propose the flip with no precondition.
func (s *Raft) stateChangeIndex(name string) (uint64, error) {
	got, err := s.GetNamespaces(name)
	if err != nil {
		return 0, err
	}
	if len(got) != 1 {
		return 0, fmt.Errorf("%w: %q", usecasesNamespaces.ErrNotFound, name)
	}
	return got[0].StateChangeIndex, nil
}

// changeNamespaceState proposes the flip. expectedIndex of 0 applies
// unconditionally.
func (s *Raft) changeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState, expectedIndex uint64) (uint64, error) {
	req := cmd.ChangeNamespaceStateRequest{
		Name:                     name,
		TargetState:              target,
		Version:                  cmd.NamespaceLatestCommandPolicyVersion,
		ExpectedStateChangeIndex: expectedIndex,
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

// nextHomeNode picks the next home_node from nodes, rotating across calls.
// Lazy StartRandom avoids biasing cold starts to nodes[0]; the iterator is
// rebuilt whenever the candidate set changes, otherwise membership churn
// would leave it rotating through a stale set. Inputs are sorted first
// because StorageCandidates can hand back an unsorted memberlist slice on
// the MetaVoterOnly fallback — without normalisation the cache key would
// flip on every call and discard the rotation state.
func (s *Raft) nextHomeNode(nodes []string) (string, error) {
	if len(nodes) == 0 {
		return "", errors.New("no storage candidates available")
	}

	sorted := slices.Clone(nodes)
	slices.Sort(sorted)

	s.homeNodeIteratorMu.Lock()
	defer s.homeNodeIteratorMu.Unlock()

	if s.homeNodeIterator == nil || !slices.Equal(s.homeNodeCandidates, sorted) {
		it, err := clusterUtils.NewNodeIterator(sorted, clusterUtils.StartRandom)
		if err != nil {
			return "", err
		}
		s.homeNodeIterator = it
		s.homeNodeCandidates = sorted
	}
	return s.homeNodeIterator.Next(), nil
}

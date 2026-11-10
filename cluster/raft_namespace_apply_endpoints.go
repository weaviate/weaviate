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
// is filled by autoPickHomeNodes. Before propose, a list shorter than
// MinimumFactor returns [namespaces.ErrBadRequest]. The apply side rejects
// duplicates with [namespaces.ErrAlreadyExists] and invalid names with
// [namespaces.ErrBadRequest]. Callers that need a follow-up local read on a
// non-leader node should pass the returned version to WaitForUpdate.
func (s *Raft) AddNamespace(ctx context.Context, ns cmd.Namespace) (cmd.Namespace, uint64, error) {
	if len(ns.HomeNodes) == 0 {
		picked, err := s.autoPickHomeNodes(ns.Name)
		if err != nil {
			return cmd.Namespace{}, 0, err
		}
		ns.HomeNodes = picked
	} else if err := s.checkHomeNodesMinimum(ns.HomeNodes); err != nil {
		return cmd.Namespace{}, 0, err
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
// apply version. The apply side returns the errors of
// [usecasesNamespaces.Controller.Update]. Only HomeNodes is mutable; existing
// live shards are not moved. A list shorter than MinimumFactor is refused with
// [namespaces.ErrBadRequest] before propose.
func (s *Raft) UpdateNamespace(ctx context.Context, ns cmd.Namespace) (uint64, error) {
	if err := s.checkHomeNodesMinimum(ns.HomeNodes); err != nil {
		return 0, err
	}

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
// state has not changed since this call read it. Returns
// [namespaces.ErrStateChangedConcurrently] if it has, and
// [namespaces.ErrNotFound] for an unknown namespace.
//
// Execute re-proposes on lost leadership without knowing whether the first
// attempt committed. Suspend and resume undo each other, so an unconditional
// retry could revert a flip that landed in between; the index read here lets
// the apply refuse it. It is read once, above Execute — reading inside the
// retry would re-read a fresh matching index each time and defeat the check.
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

// autoPickHomeNodes picks MaximumFactor home nodes, at most the storage-node
// count, for a namespace created without any. It refuses when no maximum is
// set or there are fewer storage nodes than MinimumFactor.
func (s *Raft) autoPickHomeNodes(namespace string) ([]string, error) {
	repl := s.store.cfg.Replication
	if repl.MaximumFactor <= 0 {
		return nil, fmt.Errorf("%w: namespace %q has no home_nodes and REPLICATION_MAXIMUM_FACTOR is not set; pass home_nodes or set REPLICATION_MAXIMUM_FACTOR",
			usecasesNamespaces.ErrBadRequest, namespace)
	}
	picked, err := s.nextHomeNodes(s.StorageCandidates(), repl.MaximumFactor)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", usecasesNamespaces.ErrBadRequest, err)
	}
	if len(picked) < repl.MinimumFactor {
		return nil, fmt.Errorf("%w: only %d storage node(s) to auto-pick home nodes from; REPLICATION_MINIMUM_FACTOR is %d",
			usecasesNamespaces.ErrBadRequest, len(picked), repl.MinimumFactor)
	}
	return picked, nil
}

// checkHomeNodesMinimum refuses a home-node list shorter than MinimumFactor,
// which would leave no class creatable in the namespace.
func (s *Raft) checkHomeNodesMinimum(homeNodes []string) error {
	minFactor := s.store.cfg.Replication.MinimumFactor
	if len(homeNodes) < minFactor {
		return fmt.Errorf("%w: home_nodes has %d entries; REPLICATION_MINIMUM_FACTOR requires at least %d",
			usecasesNamespaces.ErrBadRequest, len(homeNodes), minFactor)
	}
	return nil
}

// nextHomeNodes returns min(n, len(nodes)) distinct nodes, the iterator's next
// node followed by the ones after it in sorted order, wrapping past the end.
// Lazy StartRandom avoids biasing cold starts to nodes[0]; the iterator is
// rebuilt whenever the candidate set changes, otherwise membership churn
// would leave it rotating through a stale set. Inputs are sorted first
// because StorageCandidates can hand back an unsorted memberlist slice on
// the MetaVoterOnly fallback — without normalisation the cache key would
// flip on every call and discard the rotation state.
func (s *Raft) nextHomeNodes(nodes []string, n int) ([]string, error) {
	if len(nodes) == 0 {
		return nil, errors.New("no storage candidates available")
	}

	sorted := slices.Clone(nodes)
	slices.Sort(sorted)

	s.homeNodeIteratorMu.Lock()
	defer s.homeNodeIteratorMu.Unlock()

	if s.homeNodeIterator == nil || !slices.Equal(s.homeNodeCandidates, sorted) {
		it, err := clusterUtils.NewNodeIterator(sorted, clusterUtils.StartRandom)
		if err != nil {
			return nil, err
		}
		s.homeNodeIterator = it
		s.homeNodeCandidates = sorted
	}

	head := slices.Index(sorted, s.homeNodeIterator.Next())
	count := min(n, len(sorted))
	picked := make([]string, 0, count)
	for i := range count {
		picked = append(picked, sorted[(head+i)%len(sorted)])
	}
	return picked, nil
}

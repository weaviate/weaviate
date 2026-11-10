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

package api

import (
	"fmt"
	"slices"
	"strings"
)

const (
	// NamespaceLatestCommandPolicyVersion is bumped when a namespace RAFT
	// command's wire shape requires version-gated apply handling.
	NamespaceLatestCommandPolicyVersion = iota
)

// Namespace is the cluster-level control-plane entity. HomeNodes holds one
// or more unique node names and pins every namespace-owned shard.
type Namespace struct {
	Name      string
	HomeNodes []string
	State     NamespaceState
	// StateChangeIndex is the RAFT log index of the create command, or of the
	// last accepted State flip. 0 means unknown.
	StateChangeIndex uint64
}

// Primary returns the namespace's home node, or "" if unset.
func (n Namespace) Primary() string {
	if len(n.HomeNodes) == 0 {
		return ""
	}
	return n.HomeNodes[0]
}

// MissingHomeNodes returns the HomeNodes entries not in candidates, in stored
// order.
func (n Namespace) MissingHomeNodes(candidates []string) []string {
	var missing []string
	for _, node := range n.HomeNodes {
		if !slices.Contains(candidates, node) {
			missing = append(missing, node)
		}
	}
	return missing
}

// CheckReplicationFactor refuses a factor above the HomeNodes count, because
// each replica of a namespaced shard goes on a different home node.
func (n Namespace) CheckReplicationFactor(factor int64) error {
	if factor > int64(len(n.HomeNodes)) {
		return fmt.Errorf("replication factor %d exceeds the %d home node(s) of the namespace; "+
			"an administrator must add home nodes", factor, len(n.HomeNodes))
	}
	return nil
}

// CheckClassFactors refuses HomeNodes shorter than the largest value of
// factors, which maps each class of the namespace onto its replication factor.
// The error names the classes above the bound, so the caller knows which to lower.
func (n Namespace) CheckClassFactors(factors map[string]int64) error {
	var above []string
	var largest int64
	for class, factor := range factors {
		if factor > int64(len(n.HomeNodes)) {
			above = append(above, class)
			largest = max(largest, factor)
		}
	}
	if len(above) == 0 {
		return nil
	}
	slices.Sort(above)
	return fmt.Errorf("home_nodes has %d entries, fewer than the replication factor of collection(s) %s "+
		"in namespace %q (largest %d); lower their replication factor or keep at least %d home nodes",
		len(n.HomeNodes), strings.Join(above, ", "), n.Name, largest, largest)
}

// NamespaceState is the lifecycle state of a Namespace entity.
type NamespaceState string

const (
	// NamespaceStateActive accepts create-like operations against the namespace.
	NamespaceStateActive NamespaceState = "active"
	// NamespaceStateDeleting rejects create-like operations; the entity is
	// removed once cleanup empties it.
	NamespaceStateDeleting NamespaceState = "deleting"
	// NamespaceStateSuspended rejects create-like operations; the entity and
	// everything it owns are retained.
	NamespaceStateSuspended NamespaceState = "suspended"
	// NamespaceStateResuming rejects create-like operations while the
	// namespace is on its way back to active.
	NamespaceStateResuming NamespaceState = "resuming"
)

// AddNamespaceRequest is the RAFT apply payload for creating a namespace.
type AddNamespaceRequest struct {
	Namespace Namespace
	Version   int
}

// UpdateNamespaceRequest is the RAFT apply payload for changing
// HomeNodes. New placements use the new value; live shards are not moved.
type UpdateNamespaceRequest struct {
	Namespace Namespace
	Version   int
}

// ChangeNamespaceStateRequest transitions a namespace into TargetState.
// Same-state transitions are idempotent.
type ChangeNamespaceStateRequest struct {
	Name        string
	TargetState NamespaceState
	// Version is the command policy version, not a RAFT index. The apply's
	// log index arrives separately as ApplyRequest.Version.
	Version int
	// ExpectedStateChangeIndex stops a re-proposed command from undoing a
	// state change made after it was first proposed: the apply refuses the
	// flip unless the namespace's stored StateChangeIndex still equals this
	// value. 0 skips the check, for callers whose command cannot undo
	// anything.
	ExpectedStateChangeIndex uint64
}

// RemoveNamespaceEntityRequest removes a deleting namespace's entity.
type RemoveNamespaceEntityRequest struct {
	Name    string
	Version int
}

// QueryGetNamespacesRequest lists namespaces. Empty Names returns all.
type QueryGetNamespacesRequest struct {
	Names []string
}

type QueryGetNamespacesResponse struct {
	Namespaces []Namespace
}

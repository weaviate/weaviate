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

const (
	// NamespaceLatestCommandPolicyVersion is bumped when a namespace RAFT
	// command's wire shape requires version-gated apply handling.
	NamespaceLatestCommandPolicyVersion = iota
)

// Namespace is the cluster-level control-plane entity. HomeNodes pins every
// namespace-owned shard to its members. State drives the deletion lifecycle
// (active → deleting → entity removed); empty State restores as
// NamespaceStateActive.
//
// HomeNodes is shaped as a slice to lock in the wire format up front, but
// the controller currently enforces len(HomeNodes) == 1. Callers should
// read the singular value via Primary() rather than indexing directly, so
// a future relaxation to true multi-home-node placement doesn't have to
// rewrite every reader.
type Namespace struct {
	Name      string
	HomeNodes []string
	State     NamespaceState
}

// Primary returns this namespace's primary home node. While
// len(HomeNodes) == 1 is the enforced invariant, this accessor centralises
// the assumption so callers don't reach into the slice directly.
func (n Namespace) Primary() string {
	if len(n.HomeNodes) == 0 {
		return ""
	}
	return n.HomeNodes[0]
}

// NamespaceState is the lifecycle state of a Namespace entity.
type NamespaceState string

const (
	// NamespaceStateActive accepts create-like operations against the namespace.
	NamespaceStateActive NamespaceState = "active"
	// NamespaceStateDeleting rejects create-like operations; the entity is
	// removed once cleanup empties it.
	NamespaceStateDeleting NamespaceState = "deleting"
)

// AddNamespaceRequest is the RAFT apply payload for creating a namespace.
type AddNamespaceRequest struct {
	Namespace Namespace
	Version   int
}

// UpdateNamespaceRequest is the RAFT apply payload for changing the
// namespace's stored HomeNodes. Future placement decisions read the new
// value; existing live shards are not moved.
type UpdateNamespaceRequest struct {
	Namespace Namespace
	Version   int
}

// ChangeNamespaceStateRequest transitions a namespace into TargetState.
// Same-state transitions are idempotent.
type ChangeNamespaceStateRequest struct {
	Name        string
	TargetState NamespaceState
	Version     int
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

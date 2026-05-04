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
	// NamespaceLatestCommandPolicyVersion is bumped when the on-the-wire
	// shape of a namespace RAFT command changes in a way that requires
	// version-gated handling on the apply side.
	NamespaceLatestCommandPolicyVersion = iota
)

// Namespace is the cluster-level control-plane entity. It is the sole source
// of truth for namespace existence. Restrictions is a forward-compatible
// placeholder so later work can add namespace-scoped knobs without a schema
// reshape. A State field is intentionally omitted: the current delete
// semantics are a hard delete, so there is no active/deleting lifecycle to
// represent. A future addition will be tolerated by the JSON-based snapshot
// format (unknown-field defaults).
type Namespace struct {
	Name         string
	Restrictions NamespaceRestrictions
}

// NamespaceRestrictions is an empty forward-compatibility placeholder.
type NamespaceRestrictions struct{}

// AddNamespaceRequest is the RAFT apply payload for creating a namespace.
type AddNamespaceRequest struct {
	Namespace Namespace
	Version   int
}

// DeleteNamespaceRequest is the RAFT apply payload for deleting a namespace.
// Delete is hard and idempotent at the manager layer.
type DeleteNamespaceRequest struct {
	Name    string
	Version int
}

// QueryGetNamespacesRequest lists namespaces. Empty Names returns all known
// namespaces.
type QueryGetNamespacesRequest struct {
	Names []string
}

type QueryGetNamespacesResponse struct {
	Namespaces []Namespace
}

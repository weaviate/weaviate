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

import "context"

// HostnameResolver resolves a node name to its host, including the internal cluster
// API port.
type HostnameResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

// HostnameLister lists the hosts of all known cluster nodes.
type HostnameLister interface {
	AllHostnames() []string
}

// NodeCounter counts the known cluster nodes.
type NodeCounter interface {
	NodeCount() int
}

// NodeLister lists the names of all known cluster nodes.
type NodeLister interface {
	AllNames() []string
}

// NodeReader reports the cluster's nodes as this node's memberlist sees them.
type NodeReader interface {
	NodeLister
	// LocalName returns the local node name.
	LocalName() string
	// ClusterHealthScore returns an aggregate health score for the cluster; the lower
	// the better.
	ClusterHealthScore() int
}

// StorageCandidateLister lists the nodes that can hold data, sorted by free disk space
// in descending order.
type StorageCandidateLister interface {
	StorageCandidates() []string
}

// RaftMembership changes and reports the membership of the RAFT cluster. Its
// StorageCandidates are the storage nodes in the RAFT configuration, or memberlist's
// when it knows of more.
type RaftMembership interface {
	StorageCandidateLister
	// Join adds the node to the RAFT cluster, through the leader.
	Join(ctx context.Context, nodeID, raftAddr string, voter bool) error
	// Remove removes the node from the RAFT cluster, through the leader.
	Remove(ctx context.Context, nodeID string) error
	// Stats returns RAFT internals, for informational and debugging use only.
	Stats() map[string]any
}

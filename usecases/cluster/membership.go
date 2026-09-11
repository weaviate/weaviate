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

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

import (
	"fmt"
	"strings"
)

// Router defines the interface for routing logic in a distributed system.
// It determines which nodes (replicas) should be used for read and write operations,
// and provides information about node hostnames.
type Router interface {
	// GetReadWriteReplicasLocation returns the primary and additional replicas
	// for both reading and writing to the specified collection and partitioning key (e.g., tenant).
	GetReadWriteReplicasLocation(collection string, partitioningKey string) (ReplicaSet, ReplicaSet, error)

	// GetWriteReplicasLocation returns the primary replicas that should receive write requests
	// for the specified collection and partitioning key.
	GetWriteReplicasLocation(collection string, partitioningKey string) (ReplicaSet, error)

	// GetReadReplicasLocation returns the replicas that should be used to serve read requests
	// for the specified collection and partitioning key.
	GetReadReplicasLocation(collection string, partitioningKey string) (ReplicaSet, error)

	// BuildReadRoutingPlan constructs a routing plan for a read operation based on the provided options.
	BuildReadRoutingPlan(params RoutingPlanBuildOptions) (ReadRoutingPlan, error)

	// BuildWriteRoutingPlan constructs a routing plan for a write operation based on the provided options.
	BuildWriteRoutingPlan(params RoutingPlanBuildOptions) (WriteRoutingPlan, error)

	// NodeHostname returns the host address for a given node name.
	// The second return value indicates whether the hostname was found.
	NodeHostname(nodeName string) (string, bool)

	// AllHostnames returns a list of all hostnames known to the router.
	AllHostnames() []string
}

// Replica represents a single replica in the system, containing enough information
// to route traffic to it: the node name, shard name, and host address.
type Replica struct {
	NodeName  string
	ShardName string
	HostAddr  string
}

// String returns a human-readable representation of a single Replica,
// including node name, shard name, and host address.
func (r Replica) String() string {
	return fmt.Sprintf("{node: %q, shard: %q, host: %q}", r.NodeName, r.ShardName, r.HostAddr)
}

// ReplicaSet groups multiple replicas together.
// Typically used to represent the set of replicas responsible for a given shard or tenant.
type ReplicaSet struct {
	Replicas []Replica
}

// String returns a human-readable representation of a ReplicaSet,
// showing all replicas in the set.
func (s ReplicaSet) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i, r := range s.Replicas {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.String())
	}
	b.WriteString("]")
	return b.String()
}

// NodeNames returns a list of node names contained in the ReplicaSet.
func (s ReplicaSet) NodeNames() []string {
	nodeNames := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		nodeNames = append(nodeNames, replica.NodeName)
	}
	return nodeNames
}

// HostAddresses returns a list of host addresses for all replicas in the ReplicaSet.
func (s ReplicaSet) HostAddresses() []string {
	hostAddresses := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		hostAddresses = append(hostAddresses, replica.HostAddr)
	}
	return hostAddresses
}

// Shards returns a list of shard names for all replicas in the ReplicaSet.
func (s ReplicaSet) Shards() []string {
	shards := make([]string, 0, len(s.Replicas))
	for _, replica := range s.Replicas {
		shards = append(shards, replica.ShardName)
	}
	return shards
}

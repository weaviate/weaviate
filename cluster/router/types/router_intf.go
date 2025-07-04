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
)

// Router defines the contract for determining routing plans for reads and writes
// within a cluster. It abstracts the logic to identify read/write Replicas,
// construct routing plans, and access cluster host information including hostnames
// and ip addresses.
type Router interface {
	// GetReadWriteReplicasLocation returns the read and write Replicas for a given
	// collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get Replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - readReplicas: a replica set serving as read Replicas.
	//   - writeReplicas: a replica set serving as primary write Replicas.
	//   - additionalWriteReplicas: a replica set serving as additional write Replicas.
	//   - error: if an error occurs while retrieving Replicas.
	GetReadWriteReplicasLocation(collection string, shard string) (readReplicas ReadReplicaSet, writeReplicas WriteReplicaSet, err error)

	// GetWriteReplicasLocation returns the write Replicas for a given collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get write Replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - writeReplicas: a replica set serving as primary write Replicas.
	//   - additionalWriteReplicas: a replica set serving as additional write Replicas.
	//   - error: if an error occurs while retrieving Replicas.
	GetWriteReplicasLocation(collection string, shard string) (WriteReplicaSet, error)

	// GetReadReplicasLocation returns the read Replicas for a given collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get read Replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - readReplicas: a replica set serving as read Replicas.
	//   - error: if an error occurs while retrieving Replicas.
	GetReadReplicasLocation(collection string, shard string) (ReadReplicaSet, error)

	// NodeHostname returns the hostname for a given node name.
	//
	// Parameters:
	//   - nodeName: the name of the node to get the hostname for.
	//
	// Returns:
	//   - hostname: the hostname of the node.
	//   - ok: true if the hostname was found, false if the node name is unknown or unregistered.
	NodeHostname(nodeName string) (string, bool)

	// AllHostnames returns all known hostnames in the cluster.
	//
	// Returns:
	//   - hostnames: a slice of all known hostnames; always returns a valid slice, possibly empty.
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

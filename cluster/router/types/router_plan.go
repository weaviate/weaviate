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

	"github.com/sirupsen/logrus"
)

// RoutingPlanBuildOptions contains parameters used to construct a routing plan
// for either read or write operations.
//
// Fields:
//   - Shard: The name of the shard to route to. For multi-tenant collections, this must be the tenant name.
//     For single-tenant collections, this should be empty to route to all shards, or optionally set to a specific shard
//     if targeting all shards when creating routing plans for reading.
//   - Tenant: The tenant name targeted by this routing plan. Expected to be empty and ignored for single-tenant collections.
//   - ConsistencyLevel: The desired level of consistency for the operation.
//   - DirectCandidateNode: Optional. The preferred node to use first when building the routing plan.
//     If empty, the local node is used as the default candidate.
//   - ReadExecutorStopOnError: If true, the read executor when executing the plan will stop when an error occurs.
type RoutingPlanBuildOptions struct {
	Shard               string
	Tenant              string
	ConsistencyLevel    ConsistencyLevel
	DirectCandidateNode string

	ReadExecutorStopOnError bool
}

// String returns a human-readable representation of the RoutingPlanBuildOptions.
// Useful for debugging and logging.
func (o RoutingPlanBuildOptions) String() string {
	return fmt.Sprintf(
		"RoutingPlanBuildOptions{shard: %q, tenant: %q, consistencyLevel: %s, directCandidateNode: %q}",
		o.Shard, o.Tenant, o.ConsistencyLevel, o.DirectCandidateNode,
	)
}

// ReadRoutingPlan represents the plan for routing a read operation.
//
// Fields:
//   - Shard: The (optional) shard targeted by this routing plan. If empty, all relevant shards are targeted.
//   - Tenant: The tenant name targeted by this routing plan. Expected to be empty and ignored for single-tenant collections.
//   - ReplicaSet: The ordered list of Replicas to contact.
//   - ConsistencyLevel: The user-specified consistency level.
//   - IntConsistencyLevel: The resolved numeric value for the consistency level.
//   - ReadExecutorStopOnError: If true, the read executor when executing the plan will stop when an error occurs.
type ReadRoutingPlan struct {
	Shard               string
	Tenant              string
	ReplicaSet          ReadReplicaSet
	ConsistencyLevel    ConsistencyLevel
	IntConsistencyLevel int
}

// String returns a human-readable representation of the ReadRoutingPlan,
// including shard, consistency level, and list of Replicas.
func (p ReadRoutingPlan) String() string {
	return fmt.Sprintf(
		"ReadRoutingPlan{shard: %q, tenant: %q, consistencyLevel: %s (%d), Replicas: %v}",
		p.Shard, p.Tenant, p.ConsistencyLevel, p.IntConsistencyLevel, p.ReplicaSet,
	)
}

// WriteRoutingPlan represents the plan for routing a write operation.
//
// Fields:
//   - Shard: The shard targeted by this routing plan. For writing, this is required as a write operation
//     always targets a specific shard. Usually, the shard is determined based on the object's UUID.
//   - Tenant: The tenant name targeted by this routing plan. Expected to be empty and ignored for single-tenant collections.
//   - ReplicaSet: The ordered list of primary write Replicas.
//     Write Replicas will normally also include read Replicas. A node that accepts writes is also eligible to
//     serve reads.
//   - AdditionalReplicaSet: Any secondary or additional Replicas to include in the write operation.
//   - ConsistencyLevel: The user-specified consistency level.
//   - IntConsistencyLevel: The resolved numeric value for the consistency level.
type WriteRoutingPlan struct {
	Shard               string
	Tenant              string
	ReplicaSet          WriteReplicaSet
	ConsistencyLevel    ConsistencyLevel
	IntConsistencyLevel int
}

// String returns a human-readable representation of the WriteRoutingPlan,
// including shard, consistency level, write Replicas, and additional Replicas.
func (p WriteRoutingPlan) String() string {
	return fmt.Sprintf(
		"WriteRoutingPlan{shard: %q, tenant: %q, consistencyLevel: %s (%d), writeReplicas: %v}",
		p.Shard, p.Tenant, p.ConsistencyLevel, p.IntConsistencyLevel, p.ReplicaSet,
	)
}

// LogFields returns a structured representation of the ReadRoutingPlan for logging purposes.
func (p ReadRoutingPlan) LogFields() logrus.Fields {
	tenant := p.Tenant
	if tenant == "" {
		tenant = "no tenant"
	}
	return logrus.Fields{
		"shard":             p.Shard,
		"tenant":            tenant,
		"read_replica_set":  p.ReplicaSet,
		"consistency_level": p.ConsistencyLevel,
	}
}

// LogFields returns a structured representation of the WriteRoutingPlan for logging purposes.
func (p WriteRoutingPlan) LogFields() logrus.Fields {
	tenant := p.Tenant
	if tenant == "" {
		tenant = "no tenant"
	}
	return logrus.Fields{
		"shard":             p.Shard,
		"tenant":            tenant,
		"write_replica_set": p.ReplicaSet,
		"consistency_level": p.ConsistencyLevel,
	}
}

// NodeNames returns the hostnames of the Replicas included in the ReadRoutingPlan.
func (p ReadRoutingPlan) NodeNames() []string {
	return p.ReplicaSet.NodeNames()
}

// HostAddresses returns the host addresses of all Replicas in the ReadRoutingPlan.
func (p ReadRoutingPlan) HostAddresses() []string {
	return p.ReplicaSet.HostAddresses()
}

// Shards returns the logical shard names associated with the Replicas
// in the ReadRoutingPlan.
func (p ReadRoutingPlan) Shards() []string {
	return p.ReplicaSet.Shards()
}

// Replicas returns a list of replicas
func (p ReadRoutingPlan) Replicas() []Replica {
	return p.ReplicaSet.Replicas
}

// HostNames returns the hostnames of the primary write Replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) HostNames() []string {
	return p.ReplicaSet.HostAddresses()
}

// HostAddresses returns the host addresses of the primary write Replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) HostAddresses() []string {
	return p.ReplicaSet.HostAddresses()
}

// Shards returns the logical shard names associated with the primary write
// Replicas in the WriteRoutingPlan.
func (p WriteRoutingPlan) Shards() []string {
	return p.ReplicaSet.Shards()
}

// Replicas returns a list of replicas
func (p WriteRoutingPlan) Replicas() []Replica {
	return p.ReplicaSet.Replicas
}

// AdditionalHostNames returns the hostnames of the additional write Replicas,
// which are not part of the primary ReplicaSet, in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalHostNames() []string {
	return p.ReplicaSet.AdditionalHostAddresses()
}

// AdditionalHostAddresses returns the host addresses of the additional write
// Replicas, which are not part of the primary ReplicaSet, in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalHostAddresses() []string {
	return p.ReplicaSet.AdditionalHostAddresses()
}

// AdditionalShards returns the shard names associated with the additional write Replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalShards() []string {
	return p.ReplicaSet.AdditionalShards()
}

// AdditionalReplicas returns a list of additional replicas
func (p WriteRoutingPlan) AdditionalReplicas() []Replica {
	return p.ReplicaSet.AdditionalReplicas
}

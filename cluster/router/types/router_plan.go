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
//   - ConsistencyLevel: The desired level of consistency for the operation.
//   - DirectCandidateNode: Optional. The preferred node to use first when building the routing plan.
//     If empty, the local node is used as the default candidate.
type RoutingPlanBuildOptions struct {
	Shard               string
	ConsistencyLevel    ConsistencyLevel
	DirectCandidateNode string
}

// String returns a human-readable representation of the RoutingPlanBuildOptions.
// Useful for debugging and logging.
func (o RoutingPlanBuildOptions) String() string {
	return fmt.Sprintf(
		"RoutingPlanBuildOptions{shard: %q, consistencyLevel: %s, directCandidateNode: %q}",
		o.Shard, o.ConsistencyLevel, o.DirectCandidateNode,
	)
}

// ReadRoutingPlan represents the plan for routing a read operation.
//
// Fields:
//   - Shard: The (optional) shard targeted by this routing plan. If empty, all relevant shards are targeted.
//   - ReplicaSet: The ordered list of replicas to contact.
//   - ConsistencyLevel: The user-specified consistency level.
//   - IntConsistencyLevel: The resolved numeric value for the consistency level.
type ReadRoutingPlan struct {
	Shard               string
	ReplicaSet          ReplicaSet
	ConsistencyLevel    ConsistencyLevel
	IntConsistencyLevel int
}

// String returns a human-readable representation of the ReadRoutingPlan,
// including shard, consistency level, and list of replicas.
func (p ReadRoutingPlan) String() string {
	return fmt.Sprintf(
		"ReadRoutingPlan{shard: %q, consistencyLevel: %s (%d), replicas: %v}",
		p.Shard, p.ConsistencyLevel, p.IntConsistencyLevel, p.ReplicaSet,
	)
}

// Replicas returns the replicas involved in the read operation.
func (p ReadRoutingPlan) Replicas() []Replica {
	return p.ReplicaSet.Replicas
}

// WriteRoutingPlan represents the plan for routing a write operation.
//
// Fields:
//   - Shard: The shard targeted by this routing plan. For writing, this is required as a write operation
//     always targets a specific shard. Usually, the shard is determined based on the object's UUID.
//   - ReplicaSet: The ordered list of primary write replicas.
//     Write replicas will normally also include read replicas. A node that accepts writes is also eligible to
//     serve reads.
//   - AdditionalReplicaSet: Any secondary or additional replicas to include in the write operation.
//   - ConsistencyLevel: The user-specified consistency level.
//   - IntConsistencyLevel: The resolved numeric value for the consistency level.
type WriteRoutingPlan struct {
	Shard                string
	ReplicaSet           ReplicaSet
	AdditionalReplicaSet ReplicaSet
	ConsistencyLevel     ConsistencyLevel
	IntConsistencyLevel  int
}

// String returns a human-readable representation of the WriteRoutingPlan,
// including shard, consistency level, write replicas, and additional replicas.
func (p WriteRoutingPlan) String() string {
	return fmt.Sprintf(
		"WriteRoutingPlan{shard: %q, consistencyLevel: %s (%d), writeReplicas: %v, additionalReplicas: %v}",
		p.Shard, p.ConsistencyLevel, p.IntConsistencyLevel, p.ReplicaSet, p.AdditionalReplicaSet,
	)
}

// Replicas returns the primary write replicas for the operation.
func (p WriteRoutingPlan) Replicas() []Replica {
	return p.ReplicaSet.Replicas
}

// AdditionalReplicas returns secondary write replicas,
// typically used during shard migration or replication.
func (p WriteRoutingPlan) AdditionalReplicas() []Replica {
	return p.AdditionalReplicaSet.Replicas
}

// LogFields returns a structured representation of the ReadRoutingPlan for logging purposes.
func (p ReadRoutingPlan) LogFields() logrus.Fields {
	return logrus.Fields{
		"shard":             p.Shard,
		"read_replica_set":  p.ReplicaSet,
		"consistency_level": p.ConsistencyLevel,
	}
}

// LogFields returns a structured representation of the WriteRoutingPlan for logging purposes.
func (p WriteRoutingPlan) LogFields() logrus.Fields {
	return logrus.Fields{
		"shard":                  p.Shard,
		"write_replica_set":      p.ReplicaSet,
		"additional_replica_set": p.AdditionalReplicaSet,
		"consistency_level":      p.ConsistencyLevel,
	}
}

// ValidateConsistencyLevel validates that the resolved consistency level can be satisfied
// by the number of available read replicas.
//
// Returns:
//   - The resolved numeric consistency level.
//   - An error if the level exceeds the number of available replicas.
func (p ReadRoutingPlan) ValidateConsistencyLevel() (int, error) {
	return validateConsistencyLevel(p.ConsistencyLevel, p.Replicas())
}

// ValidateConsistencyLevel validates that the resolved consistency level can be satisfied
// by the number of available write replicas.
//
// Returns:
//   - The resolved numeric consistency level.
//   - An error if the level exceeds the number of available replicas.
func (p WriteRoutingPlan) ValidateConsistencyLevel() (int, error) {
	return validateConsistencyLevel(p.ConsistencyLevel, p.Replicas())
}

func validateConsistencyLevel(level ConsistencyLevel, replicas []Replica) (int, error) {
	resolved := level.ToInt(len(replicas))
	if resolved > len(replicas) {
		return 0, fmt.Errorf(
			"impossible to satisfy consistency level (%d) > available replicas (%d) replicas=%+q",
			resolved, len(replicas), replicas,
		)
	}
	return resolved, nil
}

// NodeNames returns the hostnames of the replicas included in the ReadRoutingPlan.
func (p ReadRoutingPlan) NodeNames() []string {
	return p.ReplicaSet.NodeNames()
}

// HostAddresses returns the host addresses of all replicas in the ReadRoutingPlan.
func (p ReadRoutingPlan) HostAddresses() []string {
	return p.ReplicaSet.HostAddresses()
}

// Shards returns the logical shard names associated with the replicas
// in the ReadRoutingPlan.
func (p ReadRoutingPlan) Shards() []string {
	return p.ReplicaSet.Shards()
}

// HostNames returns the hostnames of the primary write replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) HostNames() []string {
	return p.ReplicaSet.HostAddresses()
}

// HostAddresses returns the host addresses of the primary write replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) HostAddresses() []string {
	return p.ReplicaSet.HostAddresses()
}

// Shards returns the logical shard names associated with the primary write
// replicas in the WriteRoutingPlan.
func (p WriteRoutingPlan) Shards() []string {
	return p.ReplicaSet.Shards()
}

// AdditionalHostNames returns the hostnames of the additional write replicas,
// which are not part of the primary ReplicaSet, in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalHostNames() []string {
	return p.AdditionalReplicaSet.HostAddresses()
}

// AdditionalHostAddresses returns the host addresses of the additional write
// replicas, which are not part of the primary ReplicaSet, in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalHostAddresses() []string {
	return p.AdditionalReplicaSet.HostAddresses()
}

// AdditionalShards returns the shard names associated with the additional write replicas
// in the WriteRoutingPlan.
func (p WriteRoutingPlan) AdditionalShards() []string {
	return p.AdditionalReplicaSet.Shards()
}

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

package types

type ReplicationFSMReader interface {
	// FilterOneShardReplicasRead returns the read and write replicas for a given shard
	// It returns a tuple of readReplicas
	FilterOneShardReplicasRead(collection string, shard string, shardReplicasLocation []string) []string
	// FilterOneShardReplicasWrite returns the write replicas for a given shard
	// It returns a tuple of (writeReplicas, additionalWriteReplicas)
	FilterOneShardReplicasWrite(collection string, shard string, shardReplicasLocation []string) ([]string, []string)
	// IsLocalShardWritable is the source-side fence at PREPARE time. The
	// caller emits a StatusRouteStale rejection (with LastAppliedIndex =
	// catchUpIndex) when allowed is false, forcing the coordinator to
	// catch its FSM up and refresh routing. Two rules fold into this:
	//
	//   - MOVE && state == DEHYDRATING with local node as source: reject
	//     unconditionally. catchUpIndex is the op's state-transition
	//     RAFT index (caller falls back to its own applied index when 0).
	//   - COPY && state ∈ {INTEGRATING, READY} with local node as source:
	//     reject iff schemaVersion < op.AddReplicaVersion (the target
	//     hasn't been visible long enough on this coordinator's FSM for
	//     its routing plan to include the target). catchUpIndex is the
	//     op's AddReplicaVersion.
	//
	// Multiple qualifying ops: the highest catchUpIndex wins.
	IsLocalShardWritable(localNode, collection, shard string, schemaVersion uint64) (allowed bool, catchUpIndex uint64)
	// HasReplicationOpsForShard reports whether any replication op (in any
	// state) currently targets the (collection, shard). The Index uses this
	// to force a write through the Replicator path on RF=1 collections that
	// are mid-scale-out: a coordinator whose local FSM hasn't applied the
	// ReplicationAddReplicaToShard yet would otherwise see a single replica
	// in the sharding state and take the direct Index.putObject path,
	// bypassing the source-side fence and the CL=ALL fan-out — silently
	// missing the newly-added target.
	HasReplicationOpsForShard(collection, shard string) bool
}

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
	// IsLocalShardWritable is the source-side PREPARE fence. When allowed is
	// false the caller emits StatusRouteStale with LastAppliedIndex=catchUpIndex
	// to drive the coord's catch-up + retry. Two rules:
	//   - DEHYDRATING (MOVE) on source: reject; catchUp=LastStateChangeVersion.
	//   - INTEGRATING|READY on source && schemaVersion<AddReplicaVersion:
	//     reject; catchUp=AddReplicaVersion.
	// Multiple qualifying ops: the highest catchUpIndex wins.
	IsLocalShardWritable(localNode, collection, shard string, schemaVersion uint64) (allowed bool, catchUpIndex uint64)
	// HasReplicationOpsForShard reports whether any replication op currently
	// targets (collection, shard). The Index uses this to force the
	// Replicator path on RF=1 mid-scale-out, so a coord that hasn't applied
	// the add yet can't bypass the source-side fence via the direct path.
	HasReplicationOpsForShard(collection, shard string) bool
}

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

type ReplicationFSMReader interface {
	// FilterOneShardReplicasRead returns the read and write replicas for a given shard
	// It returns a tuple of readReplicas
	FilterOneShardReplicasRead(collection string, shard string, shardReplicasLocation []string) []string
	// FilterOneShardReplicasWrite returns the write replicas for a given shard
	// It returns a tuple of (writeReplicas, additionalWriteReplicas)
	FilterOneShardReplicasWrite(collection string, shard string, shardReplicasLocation []string) ([]string, []string)
}

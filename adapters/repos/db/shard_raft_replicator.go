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

package db

import (
	"context"

	"github.com/weaviate/weaviate/entities/storobj"
)

// ShardRaftReplicator is the interface for shard-level RAFT replication.
// This is injected into the Index to handle write operations via RAFT consensus
// instead of the traditional 2PC replication.
type ShardRaftReplicator interface {
	// PutObject replicates a PutObject operation through the shard's RAFT cluster.
	// The operation is routed to the shard's RAFT leader and applied to all replicas.
	PutObject(ctx context.Context, className, shardName string, obj *storobj.Object, schemaVersion uint64) error

	// IsLeader returns true if this node is the leader for the specified shard.
	IsLeader(className, shardName string) bool

	// VerifyLeaderForRead verifies this node is the leader for linearizable reads.
	VerifyLeaderForRead(ctx context.Context, className, shardName string) error

	// LeaderAddress returns the current leader's address for the specified shard.
	LeaderAddress(className, shardName string) string
}

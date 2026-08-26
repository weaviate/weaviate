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

package backup

import (
	"context"

	"github.com/weaviate/weaviate/usecases/replica"
)

// ReplicaCheckpointer proves per-shard replica convergence via async-replication
// checkpoints. Implemented by *db.DB; nil on coordinators that never dedupe.
type ReplicaCheckpointer interface {
	// ShardReplicas returns shard name -> replica node names for class.
	ShardReplicas(ctx context.Context, class string) (map[string][]string, error)
	// IsAsyncReplicationEnabled reports whether the class's replicas are kept
	// consistent by async replication (true also for RF=1, where it is irrelevant).
	IsAsyncReplicationEnabled(ctx context.Context, class string) bool
	CreateAsyncCheckpoints(ctx context.Context, class string, cutoffMs int64, shards []string) error
	DeleteAsyncCheckpoints(ctx context.Context, class string, shards []string) error
	GetAsyncCheckpointNodeStatuses(ctx context.Context, class string, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error)
}

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

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type Replicator interface {
	// Write endpoints
	ReplicateObject(ctx context.Context, className, shardName, requestID string, object *storobj.Object, schemaVersion uint64) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, className, shardName, requestID string, objects []*storobj.Object, schemaVersion uint64) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, className, shardName, requestID string, mergeDoc *objects.MergeDocument, schemaVersion uint64) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, className, shardName, requestID string, uuid strfmt.UUID, deletionTime time.Time, schemaVersion uint64) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, className, shardName, requestID string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, className, shardName, requestID string, refs []objects.BatchReference, schemaVersion uint64) replica.SimpleResponse
	CommitReplication(ctx context.Context, className, shardName, requestID string) any
	AbortReplication(ctx context.Context, className, shardName, requestID string) any
	OverwriteObjects(ctx context.Context, className, shard string, vobjects []*objects.VObject) ([]types.RepairResponse, error)
	// Read endpoints
	FetchObject(ctx context.Context, className, shardName string, id strfmt.UUID) (replica.Replica, error)
	FetchObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) ([]replica.Replica, error)
	DigestObjects(ctx context.Context, className, shardName string, ids []strfmt.UUID) (result []types.RepairResponse, err error)
	DigestObjectsInRange(ctx context.Context, className, shardName string,
		initialUUID, finalUUID strfmt.UUID, limit int) (result []types.RepairResponse, err error)
	// CompareDigests receives the source node's local digests and returns the
	// subset that requires attention on the source side: objects missing from
	// this node (UpdateTime==0), objects stale on this node (source has a
	// strictly newer UpdateTime), or objects for which this node holds a
	// tombstone (Deleted=true, verdict determined by the collection's
	// DeletionStrategy). Equal-timestamp objects are never returned because the
	// hashtree leaf digest is hash(uuid, updateTime): identical UpdateTime values
	// produce identical digests and are therefore invisible to the hashtree diff.
	CompareDigests(ctx context.Context, className, shardName string,
		digests []types.RepairResponse) ([]types.RepairResponse, error)
	HashTreeLevel(ctx context.Context, className, shardName string,
		level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)

	// CreateAsyncCheckpoint creates (or atomically replaces) a time-bounded
	// hashtree checkpoint on multiple shards in one call.
	// createdAt is determined once by the initiating node's index layer and
	// propagated to every replica unchanged, so all copies share the same
	// creation timestamp. It acts as a tie-breaker: a shard rejects the request
	// (StatusConflict) when it already holds a checkpoint with createdAt ≥ the
	// incoming value.
	CreateAsyncCheckpoint(ctx context.Context, className string, shardNames []string, cutoffMs int64, createdAt time.Time) error
	// DeleteAsyncCheckpoint removes the active checkpoint from multiple shards
	// in one call. Idempotent.
	DeleteAsyncCheckpoint(ctx context.Context, className string, shardNames []string) error
	// GetAsyncCheckpointStatus returns the checkpoint state for multiple shards on
	// this node in one call. Returns a map from shard name to its status.
	// CutoffMs == 0 in an entry means no active checkpoint for that shard.
	// An active checkpoint always has CutoffMs > 0 because CreateAsyncCheckpoint
	// rejects cutoffMs <= 0 and also rejects cutoffMs below the shard's highest
	// registered update time.
	GetAsyncCheckpointStatus(ctx context.Context, className string, shardNames []string) (map[string]replica.AsyncCheckpointShardStatus, error)
}

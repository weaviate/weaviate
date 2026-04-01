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
	"fmt"
	"sort"
	"time"

	"github.com/weaviate/weaviate/usecases/replica"
)

// asyncCheckpointBroadcaster is the subset of *replica.Replicator consumed by
// the index-level checkpoint fan-out logic. It is defined as an interface
// so the core implementation functions can be tested without a live Replicator.
// *replica.Replicator (which embeds *replica.Finder) satisfies this interface.
type asyncCheckpointBroadcaster interface {
	LocalNodeName() string
	// BroadcastCreateAsyncCheckpoint fans out a create to all remote replicas.
	// shardNames is the full set of targeted shards; the broadcaster groups them
	// by remote node and issues one batch request per node.
	BroadcastCreateAsyncCheckpoint(ctx context.Context, shardNames []string, cutoffMs int64, createdAt time.Time)
	// BroadcastDeleteAsyncCheckpoint fans out a delete to all remote replicas.
	// shardNames is the full set of targeted shards; the broadcaster groups them
	// by remote node and issues one batch request per node.
	BroadcastDeleteAsyncCheckpoint(ctx context.Context, shardNames []string)
	// BroadcastGetAsyncCheckpointStatus fetches checkpoint status for all given
	// shards from their non-local replicas. Shards are grouped by remote node so
	// one request is issued per node. Returns map[shardName][]status.
	BroadcastGetAsyncCheckpointStatus(ctx context.Context, shardNames []string) map[string][]replica.AsyncCheckpointNodeStatus
}

// CreateAsyncCheckpoints creates (or atomically replaces) a checkpoint on
// every shard of this collection and fans the operation out to all remote replicas.
//
// If shards is nil or empty, all shards known to this Index are targeted.
// If shards lists specific shard names, only those shards are targeted; a
// listed shard that does not exist in this collection returns an error before
// any fan-out is performed. A listed shard that exists but is not loaded on
// this node (i.e. this node is not a replica) is not an error: its replicas
// are still reached via broadcast.
//
// cutoffMs must be ≥ the newest object update time already registered in each
// targeted shard's hashtree; lower values are rejected by the receiving shard.
// Remote replica failures are silently ignored — use GetAsyncCheckpointStatus
// to discover which replicas were reached.
func (i *Index) CreateAsyncCheckpoints(ctx context.Context, cutoffMs int64, shards []string) error {
	allNames, loaded, err := i.resolveAllShards(shards)
	if err != nil {
		return err
	}
	// Determine createdAt once so all replicas of every shard share the same
	// creation timestamp. Truncate to millisecond precision: the local shard and
	// remote shards (which receive it as UnixMilli over the wire) must agree on
	// the exact value for the tie-breaker check to work consistently.
	createdAt := time.UnixMilli(time.Now().UnixMilli())
	return createAsyncCheckpointsImpl(ctx, allNames, loaded, cutoffMs, createdAt, i.Config.ClassName.String(), i.replicator)
}

// DeleteAsyncCheckpoints removes the active checkpoint from every shard of
// this collection and fans the operation out to all remote replicas.
//
// If shards is nil or empty, all shards known to this Index are targeted.
// If shards lists specific shard names, only those shards are targeted; a
// listed shard that does not exist in this collection returns an error.
// Shards that exist but are not loaded locally are still reached via broadcast.
//
// The operation is idempotent: shards or replicas that have no active
// checkpoint are unaffected.
func (i *Index) DeleteAsyncCheckpoints(ctx context.Context, shards []string) error {
	allNames, loaded, err := i.resolveAllShards(shards)
	if err != nil {
		return err
	}
	return deleteAsyncCheckpointsImpl(ctx, allNames, loaded, i.replicator)
}

// GetAsyncCheckpointStatus returns the per-replica checkpoint state for every
// targeted shard. The map key is the shard name; each value is a slice with
// one entry per successfully contacted replica (local + remote).
//
// If shards is nil or empty, all shards known to this Index are targeted.
// If shards lists specific shard names, only those shards are targeted; a
// listed shard that does not exist in this collection returns an error.
// Shards that exist but are not loaded locally still appear in the result
// with entries only for their remote replicas.
//
// Interpretation of the result:
//   - CutoffMs > 0: replica has an active checkpoint; Root is the bounded root.
//   - CutoffMs == 0: replica has no active checkpoint (node restarted, or
//     checkpoint was never created); Root is zero (not the unbounded root).
//
// Group entries by (CutoffMs, Root) to determine which replicas agree.
func (i *Index) GetAsyncCheckpointStatus(ctx context.Context, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	allNames, loaded, err := i.resolveAllShards(shards)
	if err != nil {
		return nil, err
	}
	return getAsyncCheckpointStatusImpl(ctx, allNames, loaded, i.replicator), nil
}

// createAsyncCheckpointsImpl is the testable core of CreateAsyncCheckpoints.
// It applies the checkpoint locally to every loaded shard first, then issues a
// single batch broadcast for all shard names. The broadcaster is responsible for
// grouping those shards by remote replica node and sending one request per node,
// rather than one request per shard.
//   - If loaded locally (this node is a replica): the checkpoint is applied to
//     the local shard. The broadcaster will skip this node when fanning out.
//   - If not loaded locally (this node is not a replica): the broadcaster reaches
//     all replicas of that shard, since none is filtered as "local".
//
// Partial-success semantics: if a local shard returns an error (e.g.
// StatusPreconditionFailed because cutoffMs is below that shard's highest
// registered update time), the function returns immediately without applying the
// checkpoint to subsequent shards and without broadcasting. Shards that
// succeeded before the failure are left with the new checkpoint. This is
// intentional: the operation is idempotent (overwrite) so the caller can
// correct the cutoff and re-issue; the re-issue will atomically overwrite any
// checkpoint already created in this attempt.
func createAsyncCheckpointsImpl(
	ctx context.Context,
	allNames []string,
	loaded map[string]ShardLike,
	cutoffMs int64,
	createdAt time.Time,
	className string,
	bc asyncCheckpointBroadcaster,
) error {
	for _, shardName := range allNames {
		if shard, ok := loaded[shardName]; ok {
			if err := shard.CreateAsyncCheckpoint(cutoffMs, createdAt); err != nil {
				return fmt.Errorf("class %q shard %q: %w", className, shardName, err)
			}
		}
	}
	// Single batch broadcast for all shards. The broadcaster groups shards by
	// remote node and sends one request per node instead of one per shard.
	bc.BroadcastCreateAsyncCheckpoint(ctx, allNames, cutoffMs, createdAt)
	return nil
}

// deleteAsyncCheckpointsImpl is the testable core of DeleteCheckpoints.
// The same local-first, single-batch-broadcast pattern as createAsyncCheckpointsImpl.
func deleteAsyncCheckpointsImpl(
	ctx context.Context,
	allNames []string,
	loaded map[string]ShardLike,
	bc asyncCheckpointBroadcaster,
) error {
	for _, shardName := range allNames {
		if shard, ok := loaded[shardName]; ok {
			shard.DeleteAsyncCheckpoint()
		}
	}
	// Single batch broadcast for all shards.
	bc.BroadcastDeleteAsyncCheckpoint(ctx, allNames)
	return nil
}

// getAsyncCheckpointStatusImpl is the testable core of GetAsyncCheckpointStatus.
// Local shard state is collected first, then a single batch broadcast fetches all
// remote replicas' state grouped by node (one request per node, not per shard).
func getAsyncCheckpointStatusImpl(
	ctx context.Context,
	allNames []string,
	loaded map[string]ShardLike,
	bc asyncCheckpointBroadcaster,
) map[string][]replica.AsyncCheckpointNodeStatus {
	result := make(map[string][]replica.AsyncCheckpointNodeStatus, len(allNames))
	localNode := bc.LocalNodeName()

	for _, shardName := range allNames {
		var statuses []replica.AsyncCheckpointNodeStatus
		if shard, ok := loaded[shardName]; ok {
			root, cutoffMs, createdAt, _ := shard.AsyncCheckpointRoot()
			statuses = append(statuses, replica.AsyncCheckpointNodeStatus{
				Node:      localNode,
				CutoffMs:  cutoffMs,
				CreatedAt: createdAt,
				Root:      root,
			})
		}
		result[shardName] = statuses
	}

	// Single batch broadcast: one request per remote node for all shards.
	remoteAll := bc.BroadcastGetAsyncCheckpointStatus(ctx, allNames)
	for shardName, remotes := range remoteAll {
		result[shardName] = append(result[shardName], remotes...)
	}

	return result
}

// resolveAllShards returns all targeted shard names and the subset that are
// currently loaded on this node (i.e. this node is a replica of those shards).
//
// If shards is nil or empty, all shards known to this Index are targeted.
// If shards is non-empty, each listed name must exist in this Index's shard
// map; an error is returned if any name does not exist. A shard that exists
// but is unloaded (this node is not a replica) is NOT an error — the caller
// will broadcast to its replicas directly.
func (i *Index) resolveAllShards(shards []string) (allNames []string, loaded map[string]ShardLike, err error) {
	// Build the loaded set (shards this node is a replica of).
	loaded = make(map[string]ShardLike)
	_ = i.ForEachLoadedShard(func(name string, shard ShardLike) error {
		loaded[name] = shard
		return nil
	})

	if len(shards) == 0 {
		// Target all shards known to this Index. The callback only reads the name
		// to avoid triggering a lazy-load of unloaded shards.
		_ = i.ForEachShard(func(name string, _ ShardLike) error {
			allNames = append(allNames, name)
			return nil
		})
		// Sort for deterministic iteration order: ForEachShard walks a sync.Map
		// whose enumeration order is undefined. Sorting makes partial-success
		// outcomes stable across runs.
		sort.Strings(allNames)
		return allNames, loaded, nil
	}

	// Validate each requested shard exists in this collection (loaded or not).
	known := make(map[string]struct{})
	_ = i.ForEachShard(func(name string, _ ShardLike) error {
		known[name] = struct{}{}
		return nil
	})
	seen := make(map[string]struct{}, len(shards))
	deduped := make([]string, 0, len(shards))
	for _, name := range shards {
		if _, ok := known[name]; !ok {
			return nil, nil, fmt.Errorf("shard %q does not exist in this collection", name)
		}
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			deduped = append(deduped, name)
		}
	}
	return deduped, loaded, nil
}

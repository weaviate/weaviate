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
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/export"
	"github.com/weaviate/weaviate/usecases/multitenancy"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ShardOwnership returns a map of node name to shard names for a given class.
// Shards are distributed across their replica nodes using a least-loaded
// strategy so that export work is balanced across the cluster.
func (db *DB) ShardOwnership(ctx context.Context, className string) (map[string][]string, error) {
	shardNodes := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			if len(shard.BelongsToNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has no assigned nodes", shardName, className)
			}

			// Filter out empty node names to avoid assigning shards to an invalid node.
			validNodes := make([]string, 0, len(shard.BelongsToNodes))
			for _, node := range shard.BelongsToNodes {
				if node != "" {
					validNodes = append(validNodes, node)
				}
			}
			if len(validNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has only empty assigned nodes", shardName, className)
			}

			shardNodes[shardName] = validNodes
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	return assignShardsToNodes(shardNodes), nil
}

// assignShardsToNodes distributes shards across their replica nodes using a
// least-loaded strategy. For each shard (processed in sorted order for
// determinism), it picks the replica node with the fewest already-assigned
// shards. Ties are broken by lexicographic node name order.
func assignShardsToNodes(shards map[string][]string) map[string][]string {
	result := make(map[string][]string)
	if len(shards) == 0 {
		return result
	}

	// Process shards in sorted order for determinism.
	shardNames := make([]string, 0, len(shards))
	for name := range shards {
		shardNames = append(shardNames, name)
	}
	sort.Strings(shardNames)

	for _, shardName := range shardNames {
		nodes := shards[shardName]

		// Pick the node with the least load; break ties lexicographically.
		best := nodes[0]
		bestLoad := len(result[best])
		for _, node := range nodes[1:] {
			nl := len(result[node])
			if nl < bestLoad || (nl == bestLoad && node < best) {
				best = node
				bestLoad = nl
			}
		}

		result[best] = append(result[best], shardName)
	}

	return result
}

// IsMultiTenant returns true if the class has multi-tenancy enabled.
func (db *DB) IsMultiTenant(_ context.Context, className string) bool {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return false
	}
	class := idx.getClass()
	return class != nil && class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
}

// IsAsyncReplicationEnabled returns true if async replication is either
// enabled or not required for correctness. Classes with a replication
// factor ≤ 1 have no replicas, so async replication is irrelevant and
// the method returns true. For RF > 1 the class must have async
// replication enabled and it must not be globally disabled at the
// cluster level.
func (db *DB) IsAsyncReplicationEnabled(_ context.Context, className string) bool {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return false
	}
	class := idx.getClass()
	if class == nil {
		return false
	}
	// No replicas → async replication is not needed for correctness.
	if class.ReplicationConfig == nil || class.ReplicationConfig.Factor <= 1 {
		return true
	}
	if db.config.Replication.AsyncReplicationDisabled.Get() {
		return false
	}
	return class.ReplicationConfig.AsyncEnabled
}

// SnapshotShards creates point-in-time snapshots of the objects buckets for
// the given shards of a class. It acquires the index's dropIndex.RLock once
// for the entire batch.
func (db *DB) SnapshotShards(ctx context.Context, className string, shardNames []string, exportID string) ([]export.ShardSnapshotResult, error) {
	var (
		idx    *Index
		exists bool
	)
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		idx, exists = db.indices[indexID(schema.ClassName(className))]
		if exists {
			idx.dropIndex.RLock()
		}
	}()

	if !exists {
		return nil, fmt.Errorf("index not found for class %s", className)
	}
	defer idx.dropIndex.RUnlock()

	return idx.snapshotShardsForExport(ctx, shardNames, exportID)
}

func (i *Index) snapshotShardsForExport(ctx context.Context, shardNames []string, exportID string) ([]export.ShardSnapshotResult, error) {
	if !i.probeHardlinkSupport() {
		// Export snapshots rely on hard-linking immutable LSM segment files to
		// create a frozen, read-only view without holding shard locks for the
		// duration of the scan. Supporting filesystems without hard links would
		// require either reading directly from live bucket directories (which
		// conflicts with NewSnapshotBucket's safety checks and concurrent
		// compaction) or a full file-copy fallback — both are significant
		// changes that are not worth the complexity given that all production
		// filesystems (ext4, xfs, btrfs, APFS, NTFS) support hard links.
		return nil, fmt.Errorf("export requires a filesystem that supports hard links; "+
			"the data directory for class %s does not", i.Config.ClassName)
	}

	class := i.getClass()
	if class == nil {
		return nil, fmt.Errorf("class not found for index %s", i.Config.ClassName)
	}

	isMT := multitenancy.IsMultiTenant(class.MultiTenancyConfig)
	snapshotsRoot := i.snapshotsPath()

	// Pre-allocate one result per shard. Each goroutine writes to its own
	// index so no synchronisation is needed on the slice elements.
	results := make([]export.ShardSnapshotResult, len(shardNames))

	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(i.logger, ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for idx, shardName := range shardNames {
		results[idx].ShardName = shardName

		eg.Go(func() error {
			snapshotName := fmt.Sprintf("export-%s-%s-%s", exportID, i.Config.ClassName, shardName)
			snapResult, skipReason, err := i.snapshotLocalShard(egCtx, class, isMT, shardName, snapshotsRoot, snapshotName)
			if err != nil {
				return fmt.Errorf("snapshot shard %s/%s: %w", i.Config.ClassName, shardName, err)
			}
			if snapResult == nil {
				results[idx].SkipReason = skipReason
			} else {
				results[idx].SnapshotDir = snapResult.SnapshotDir
				results[idx].Strategy = snapResult.Strategy
			}
			return nil
		}, shardName)
	}

	if err := eg.Wait(); err != nil {
		// Clean up snapshots created by goroutines that succeeded before
		// the error. The caller also defers cleanup, but we do it here
		// eagerly so disk space is freed as soon as possible.
		for j := range results {
			if results[j].SnapshotDir != "" {
				os.RemoveAll(results[j].SnapshotDir)
				results[j].SnapshotDir = ""
			}
		}
		return results, err
	}

	return results, nil
}

// shouldSkipTenant checks the RAFT-based tenant status and returns a skip
// reason if the tenant should not be exported. Returns ("", nil) if the
// tenant is exportable. Active and cold/inactive tenants are always
// exportable (cold tenants are snapshotted from disk without loading).
// Only offloaded, frozen, and transitional states are skipped.
//
// Errors from the status lookup are returned as errors (not skip reasons)
// so that the caller can fail the export rather than silently skipping
// the tenant — a transient RAFT failure should not produce an incomplete
// export.
func (i *Index) shouldSkipTenant(class *models.Class, shardName string) (string, error) {
	statuses, err := i.tenantsManager.TenantsStatus(class.Class, shardName)
	if err != nil {
		return "", fmt.Errorf("get tenant status for %s/%s: %w", class.Class, shardName, err)
	}
	status := statuses[shardName]

	switch status {
	case models.TenantActivityStatusHOT, models.TenantActivityStatusACTIVE,
		models.TenantActivityStatusCOLD, models.TenantActivityStatusINACTIVE:
		return "", nil
	default:
		return fmt.Sprintf("tenant status is %s", status), nil
	}
}

// snapshotLocalShard snapshots a shard based on its local state. It holds
// shardCreateLocks.RLock for the entire operation to prevent races with
// concurrent shard loading/unloading.
//
// For MT classes the tenant status check (shouldSkipTenant) runs under the
// same lock so that the status cannot change between the check and the
// snapshot.
//
// The local state determines *how* to snapshot: if the shard is loaded
// (including unloaded lazy shards) it is snapshotted from the live bucket;
// if not, it is snapshotted directly from disk.
func (i *Index) snapshotLocalShard(
	ctx context.Context, class *models.Class, isMT bool, shardName string,
	snapshotsRoot, snapshotName string,
) (*export.ShardSnapshotResult, string, error) {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()
	if i.closed {
		return nil, "", errAlreadyShutdown
	}

	// Hold the shard lock for the entire operation so that no concurrent
	// load/unload can change the local shard state during the snapshot.
	i.shardCreateLocks.RLock(shardName)
	defer i.shardCreateLocks.RUnlock(shardName)

	if isMT {
		// this is using the RAFT state and not the local state, so there might be a race between these two. We accept
		// this here as any action concurrent with the export might or might not be reflected in the export.
		skipReason, err := i.shouldSkipTenant(class, shardName)
		if err != nil {
			return nil, "", err
		}
		if skipReason != "" {
			return nil, skipReason, nil
		}
	}

	if shard := i.shards.Load(shardName); shard != nil {
		return i.snapshotShard(ctx, shard, shardName, snapshotsRoot, snapshotName)
	}

	// Shard is not loaded. Snapshot from disk if data exists.
	return i.snapshotFromDisk(shardName, snapshotsRoot, snapshotName)
}

// snapshotShard snapshots a shard that may be lazy-loaded or fully loaded.
// If the shard is a LazyLoadShard that hasn't been loaded yet, it snapshots
// directly from disk to avoid triggering a load. The caller must hold
// shardCreateLocks.RLock.
func (i *Index) snapshotShard(
	ctx context.Context, shard ShardLike, shardName string,
	snapshotsRoot, snapshotName string,
) (*export.ShardSnapshotResult, string, error) {
	if lazyShard, ok := shard.(*LazyLoadShard); ok {
		release := lazyShard.blockLoading()
		defer release()

		if !lazyShard.loaded {
			return i.snapshotFromDisk(shardName, snapshotsRoot, snapshotName)
		}
		shard = lazyShard.shard
	}

	return i.snapshotFromLoadedShard(ctx, shard, shardName, snapshotsRoot, snapshotName)
}

// snapshotFromLoadedShard snapshots the objects bucket of an already-acquired
// shard. The caller must hold whatever lock is appropriate (shardCreateLocks
// for MT, acquireShardWithLock for non-MT).
func (i *Index) snapshotFromLoadedShard(
	ctx context.Context, shard ShardLike, shardName string,
	snapshotsRoot, snapshotName string,
) (*export.ShardSnapshotResult, string, error) {
	store := shard.Store()
	if store == nil {
		return nil, "", fmt.Errorf("store not found for shard %s", shardName)
	}
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, "", fmt.Errorf("objects bucket not found for shard %s", shardName)
	}

	snapshotDir, err := bucket.CreateSnapshot(ctx, snapshotsRoot, snapshotName)
	if err != nil {
		return nil, "", fmt.Errorf("create snapshot for shard %s: %w", shardName, err)
	}

	return &export.ShardSnapshotResult{
		SnapshotDir: snapshotDir,
		Strategy:    bucket.GetDesiredStrategy(),
	}, "", nil
}

// snapshotFromDisk hard-links the objects bucket files from an unloaded
// shard's on-disk directory. The caller must hold shardCreateLocks.RLock
// to prevent concurrent shard status changes. RLock is sufficient because
// concurrent callers produce distinct snapshot directories.
func (i *Index) snapshotFromDisk(
	shardName, snapshotsRoot, snapshotName string,
) (*export.ShardSnapshotResult, string, error) {
	// Path convention: <indexPath>/<shardName>/lsm/<bucketName>. This is the
	// same convention used by shard init (shard_init_lsm.go) via shardPathLSM
	// + helpers.ObjectsBucketLSM, so it stays in sync with loaded shards.
	objectsBucketDir := filepath.Join(shardPathLSM(i.path(), shardName), helpers.ObjectsBucketLSM)
	if _, err := os.Stat(objectsBucketDir); err != nil {
		if os.IsNotExist(err) {
			return nil, "tenant has no data on disk", nil
		}
		return nil, "", fmt.Errorf("stat objects bucket dir for shard %s: %w", shardName, err)
	}

	snapshotDir, err := lsmkv.SnapshotBucketFromDisk(objectsBucketDir, snapshotsRoot, snapshotName)
	if err != nil {
		return nil, "", fmt.Errorf("hardlink unloaded shard %s: %w", shardName, err)
	}

	return &export.ShardSnapshotResult{
		SnapshotDir: snapshotDir,
		Strategy:    lsmkv.StrategyReplace,
	}, "", nil
}

// ListClasses returns all class names (already exists on DB, this is a convenience alias comment).
// DB.ListClasses is defined in index.go.

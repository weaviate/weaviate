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
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// coldObjectCounts caches object counts for COLD tenants (data on local
// disk, shard not loaded), keyed by tenant name. Safe for concurrent use.
type coldObjectCounts struct {
	sync.RWMutex
	counts map[string]int64
}

func newColdObjectCounts() *coldObjectCounts {
	return &coldObjectCounts{counts: map[string]int64{}}
}

// set stores the count for name, overwriting any prior value.
func (c *coldObjectCounts) set(name string, n int64) {
	c.Lock()
	c.counts[name] = n
	c.Unlock()
}

// drop removes the entry for name. No-op when the entry doesn't exist.
func (c *coldObjectCounts) drop(name string) {
	c.Lock()
	delete(c.counts, name)
	c.Unlock()
}

// get returns the cached count and whether an entry exists. Returns (0, false) when not.
func (c *coldObjectCounts) get(name string) (int64, bool) {
	c.RLock()
	defer c.RUnlock()
	v, ok := c.counts[name]
	return v, ok
}

// tracksColdObjects reports whether this Index maintains the cold-tenant
// object-count cache. Centralizes the gating policy so each lifecycle hook
// asks one question; today it's a synonym for partitioningEnabled, but it
// can grow conditions (operator config, runtime feature flag, etc.)
// without touching every call site.
//
// When this returns true, coldObjects is expected to be non-nil; a nil
// here is a NewIndex bug we want to surface loudly via panic.
func (i *Index) tracksColdObjects() bool {
	return i.partitioningEnabled
}

// cacheColdCountFromShard reads the tenant's object count from a loaded
// shard via ObjectCountAsync and writes it to the cache. Caller is
// responsible for shardCreateLocks. Errors are logged and swallowed —
// we'd rather accept a bounded under-count for this tenant than fail
// the deactivation.
func (i *Index) cacheColdCountFromShard(ctx context.Context, shard ShardLike) {
	if !i.tracksColdObjects() {
		return
	}
	c, err := shard.ObjectCountAsync(ctx)
	if err != nil {
		i.logger.WithField("shard", shard.Name()).WithError(err).
			Warn("usagelimits: failed to cache cold object count from shard")
		return
	}
	i.coldObjects.set(shard.Name(), c)
}

// cacheColdCountFromDisk reads the tenant's object count from on-disk
// segment metadata and writes it to the cache. Used when the shard
// isn't loaded (startup walk, post-unfreeze restore). Errors are logged
// and swallowed.
func (i *Index) cacheColdCountFromDisk(tenant string) {
	if !i.tracksColdObjects() {
		return
	}
	u, err := shardusage.CalculateUnloadedObjectsMetrics(i.logger, i.path(), tenant, true)
	if err != nil {
		i.logger.WithField("shard", tenant).WithError(err).
			Warn("usagelimits: failed to cache cold object count from disk")
		return
	}
	i.coldObjects.set(tenant, u.Count)
}

// dropColdObjectCount removes a tenant's entry. Safe to call when no
// entry exists.
func (i *Index) dropColdObjectCount(tenant string) {
	if !i.tracksColdObjects() {
		return
	}
	i.coldObjects.drop(tenant)
}

// LocalObjectCount sums object counts across every local tenant on this
// node, summing loaded shards (HOT) and cached counts (COLD) per the
// per-tenant atomic decision in countObjects. Implements
// usagelimits.ObjectCounter.
//
// Loaded counts come from ObjectCountAsync (excludes the memtable) so
// bulk imports may briefly overshoot; cold counts come from the cache
// populated at HOT→COLD snapshot or post-unfreeze restore. See
// docs/usage_limits.md.
func (db *DB) LocalObjectCount(ctx context.Context) (int64, error) {
	db.indexLock.RLock()
	indices := make([]*Index, 0, len(db.indices))
	for _, idx := range db.indices {
		indices = append(indices, idx)
	}
	db.indexLock.RUnlock()

	var total int64
	for _, idx := range indices {
		count, err := idx.localObjectCount(ctx)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

// localObjectCount sums object counts across every local tenant of this
// index. The schema lock is held only for the cheap name-list copy;
// per-tenant accounting runs outside it, using shardCreateLocks.RLock
// to atomically pick between the loaded shard count and the cached
// cold count without observing both.
//
// Lazy-but-not-loaded HOT shards contribute 0 (shardMap.Loaded skips
// them), matching today's ForEachLoadedShard behavior.
func (i *Index) localObjectCount(ctx context.Context) (int64, error) {
	var localNames []string
	err := i.schemaReader.Read(i.Config.ClassName.String(), true,
		func(_ *models.Class, state *sharding.State) error {
			for name := range state.Physical {
				if state.IsLocalShard(name) {
					localNames = append(localNames, name)
				}
			}
			return nil
		})
	if err != nil {
		return 0, err
	}

	var total int64
	for _, name := range localNames {
		i.shardCreateLocks.RLock(name)
		if shard := i.shards.Loaded(name); shard != nil {
			if c, err := shard.ObjectCountAsync(ctx); err == nil {
				total += c
			} else {
				i.logger.WithField("shard", name).WithError(err).
					Warn("usagelimits: error counting objects for shard")
			}
		} else if i.tracksColdObjects() {
			if cached, ok := i.coldObjects.get(name); ok {
				total += cached
			}
		}
		// else: FROZEN / mid-transition / never-local — contributes 0
		i.shardCreateLocks.RUnlock(name)
	}
	return total, nil
}

// Compile-time assertion that DB satisfies usagelimits.ObjectCounter.
var _ usagelimits.ObjectCounter = (*DB)(nil)

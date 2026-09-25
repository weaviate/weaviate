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
	"sync"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// SetUsageLimits installs the usage-limits Manager on the Index and
// snapshots the cold-tenant cache gate. Must be called by the index's
// owner after NewIndex returns and before the Index is registered in
// db.indices.
func (i *Index) SetUsageLimits(m *usagelimits.Manager) {
	i.usageLimits = m
	if i.partitioningEnabled && m.HasObjectCap() {
		i.coldObjects = newColdObjectCounts()
		i.coldObjectsTracked = true
	}
}

// RestoreColdCounts populates the cold-tenant cache from on-disk
// segment metadata for every local tenant that the schema state marks
// COLD. Intended for startup paths where the index is rehydrated from
// disk and the schema may already contain COLD tenants from a prior
// run. No-op when tracking is disabled.
//
// Errors from the schema read are returned; per-tenant disk-read
// failures are logged and swallowed inside cacheColdCountFromDisk —
// we'd rather accept a bounded under-count than refuse to start.
func (i *Index) RestoreColdCounts() error {
	if !i.coldObjectsTracked {
		return nil
	}

	var coldTenants []string
	className := i.Config.ClassName.String()
	err := i.schemaReader.Read(className, true,
		func(_ *models.Class, state *sharding.State) error {
			if state == nil {
				return fmt.Errorf("unable to retrieve sharding state for class %s", className)
			}
			for shardName, physical := range state.Physical {
				if !state.IsLocalShard(shardName) {
					continue
				}
				if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
					coldTenants = append(coldTenants, shardName)
				}
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("read schema for cold tenants: %w", err)
	}

	if len(coldTenants) == 0 {
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)
	for _, name := range coldTenants {
		eg.Go(func() error {
			i.cacheColdCountFromDisk(name)
			return nil
		}, name)
	}
	return eg.Wait()
}

// cacheColdCountFromShard reads the tenant's object count from a loaded
// shard via ObjectCountAsync and writes it to the cache. Caller is
// responsible for shardCreateLocks. Errors are logged and swallowed —
// we'd rather accept a bounded under-count for this tenant than fail
// the deactivation.
func (i *Index) cacheColdCountFromShard(ctx context.Context, shard ShardLike) {
	if !i.coldObjectsTracked {
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
	if !i.coldObjectsTracked {
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
	if !i.coldObjectsTracked {
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
		} else if i.coldObjectsTracked {
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

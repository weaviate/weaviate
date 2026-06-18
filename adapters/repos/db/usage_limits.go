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

// LocalObjectCount sums async object counts across all locally-loaded
// shards on this node. Implements usagelimits.ObjectCounter.
//
// Uses ObjectCountAsync (excludes the memtable) so bulk imports may
// briefly overshoot; self-corrects on flush. Cold lazy-load shards are
// skipped — counting them wouldn't force a load, but would mean a dir
// walk + per-segment metadata read on every write. See
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
		if err := idx.ForEachLoadedShard(func(name string, shard ShardLike) error {
			// Guard against concurrent tenant deactivation between
			// iteration and the count call.
			idx.shardCreateLocks.RLock(name)
			defer idx.shardCreateLocks.RUnlock(name)
			if idx.shards.Load(name) == nil {
				return nil
			}
			count, err := shard.ObjectCountAsync(ctx)
			if err != nil {
				// Per-shard counting failures are recoverable: a transient
				// miss on one shard should not block all writes.
				db.logger.
					WithField("shard", shard.Name()).
					WithField("error", err.Error()).
					Warn("usagelimits: error counting objects for shard")
				return nil
			}
			total += count
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Compile-time assertion that DB satisfies usagelimits.ObjectCounter.
var _ usagelimits.ObjectCounter = (*DB)(nil)

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

	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// LocalObjectCount sums async object counts across all locally-loaded
// shards on this node. Implements usagelimits.ObjectCounter; see
// docs/usage_limits.md.
//
// Uses ObjectCountAsync — the count excludes the in-memory memtable, so
// during fast bulk imports it lags on-disk state. The bounded overshoot
// self-corrects on the next flush.
//
// Cold (unloaded lazy-load) shards are skipped from the sum. Counting them
// would not force a load (LazyLoadShard.ObjectCountAsync can read counts
// straight from on-disk segment metadata), but it would add a directory
// walk plus one open+read per segment metadata file on every enforced
// write — for an MT account with many cold tenants that quickly turns
// into hundreds of file I/Os on the hot path. We skip them for that cost
// reason; the trade-off is that an account with dormant tenants may sit
// slightly under-counted until those tenants are activated again. See
// docs/usage_limits.md ("Accepted imperfections") for the deferred fix.
func (db *DB) LocalObjectCount(ctx context.Context) (int64, error) {
	db.indexLock.RLock()
	indices := make([]*Index, 0, len(db.indices))
	for _, idx := range db.indices {
		indices = append(indices, idx)
	}
	db.indexLock.RUnlock()

	var total int64
	for _, idx := range indices {
		if err := idx.ForEachLoadedShard(func(_ string, shard ShardLike) error {
			count, err := shard.ObjectCountAsync(ctx)
			if err != nil {
				// Treat per-shard counting failures as recoverable so a
				// transient error on one shard doesn't veto the whole
				// limit check (which would block all writes). Logged as a
				// warning (not an error) — operator action is not
				// required for one transient miss. Error attached as a
				// field rather than via WithError because the latter
				// renders poorly in our log aggregator (Dash0).
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

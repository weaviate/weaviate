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
// shards on this node. Implements usagelimits.ObjectCounter for the
// Free-Tier guardrails.
//
// Uses the async path (ObjectCountAsync) — the count excludes the
// in-memory memtable, so during fast bulk imports it lags slightly
// behind the on-disk state. This bounded overshoot is documented in the
// RFC's "Accepted imperfections" and self-corrects on the next flush.
//
// Cold (unloaded lazy) shards are skipped: counting them would force a
// load, which is expensive and undermines lazy-load semantics. For the
// Free-Tier use case (single-node, RF=1, typically one or few hot
// shards) this matches the intended scope=node accounting.
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
				// limit check (which would block all writes).
				db.logger.WithError(err).
					WithField("shard", shard.Name()).
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

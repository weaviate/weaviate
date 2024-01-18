//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"time"

	"github.com/pkg/errors"
)

type BucketOption func(b *Bucket) error

func WithStrategy(strategy string) BucketOption {
	return func(b *Bucket) error {
		switch strategy {
		case StrategyReplace, StrategyMapCollection, StrategySetCollection,
			StrategyRoaringSet:
		default:
			return errors.Errorf("unrecognized strategy %q", strategy)
		}

		b.strategy = strategy
		return nil
	}
}

func WithMemtableThreshold(threshold uint64) BucketOption {
	return func(b *Bucket) error {
		b.memtableThreshold = threshold
		return nil
	}
}

func WithWalThreshold(threshold uint64) BucketOption {
	return func(b *Bucket) error {
		b.walThreshold = threshold
		return nil
	}
}

func WithIdleThreshold(threshold time.Duration) BucketOption {
	return func(b *Bucket) error {
		b.flushAfterIdle = threshold
		return nil
	}
}

func WithSecondaryIndices(count uint16) BucketOption {
	return func(b *Bucket) error {
		b.secondaryIndices = count
		return nil
	}
}

func WithLegacyMapSorting() BucketOption {
	return func(b *Bucket) error {
		b.legacyMapSortingBeforeCompaction = true
		return nil
	}
}

func WithPread(with bool) BucketOption {
	return func(b *Bucket) error {
		b.mmapContents = !with
		return nil
	}
}

func WithDynamicMemtableSizing(
	initialMB, maxMB, minActiveSeconds, maxActiveSeconds int,
) BucketOption {
	return func(b *Bucket) error {
		mb := 1024 * 1024
		cfg := memtableSizeAdvisorCfg{
			initial:     initialMB * mb,
			stepSize:    10 * mb,
			maxSize:     maxMB * mb,
			minDuration: time.Duration(minActiveSeconds) * time.Second,
			maxDuration: time.Duration(maxActiveSeconds) * time.Second,
		}
		b.memtableResizer = newMemtableSizeAdvisor(cfg)
		return nil
	}
}

type secondaryIndexKeys [][]byte

type SecondaryKeyOption func(s secondaryIndexKeys) error

func WithSecondaryKey(pos int, key []byte) SecondaryKeyOption {
	return func(s secondaryIndexKeys) error {
		if pos > len(s) {
			return errors.Errorf("set secondary index %d on an index of length %d",
				pos, len(s))
		}

		s[pos] = key

		return nil
	}
}

func WithMonitorCount() BucketOption {
	return func(b *Bucket) error {
		if b.strategy != StrategyReplace {
			return errors.Errorf("count monitoring only supported on 'replace' buckets")
		}
		b.monitorCount = true
		return nil
	}
}

func WithKeepTombstones(keepTombstones bool) BucketOption {
	return func(b *Bucket) error {
		b.keepTombstones = keepTombstones
		return nil
	}
}

func WithUseBloomFilter(useBloomFilter bool) BucketOption {
	return func(b *Bucket) error {
		b.useBloomFilter = useBloomFilter
		return nil
	}
}

func WithCalcCountNetAdditions(calcCountNetAdditions bool) BucketOption {
	return func(b *Bucket) error {
		b.calcCountNetAdditions = calcCountNetAdditions
		return nil
	}
}

/*
Background for this option:

We use the LSM store in two places:
Our existing key/value and inverted buckets
As part of the new brute-force based index (to be built this week).

Brute-force index
This is a simple disk-index where we use a cursor to iterate over all objects. This is what we need the force-compaction for. The experimentation so far has shown that the cursor is much more performant on a single segment than it is on multiple segments. This is because with a single segment it’s essentially just one conitiguuous chunk of data on disk that we read through. But with multiple segments (and an unpredicatable order) it ends up being many tiny reads (inefficient).
Existing uses of the LSM store
For existing uses, e.g. the object store, we don’t want to force-compact. This is because they can grow massive. For example, you could have a 100GB segment, then a new write leads to a new segment that is just a few bytes. If we would force-compact those two we would write 100GB every time the user sends a few bytes to Weaviate. In this case, the existing tiered compaction strategy makes more sense.
Configurability of buckets
*/
func WithForceCompation(opt bool) BucketOption {
	return func(b *Bucket) error {
		b.forceCompaction = opt
		return nil
	}
}

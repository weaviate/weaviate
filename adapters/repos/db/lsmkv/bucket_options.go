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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type BucketOption func(b *Bucket) error

func WithStrategy(strategy string) BucketOption {
	return func(b *Bucket) error {
		if err := CheckExpectedStrategy(strategy); err != nil {
			return err
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

func WithMinMMapSize(minMMapSize int64) BucketOption {
	return func(b *Bucket) error {
		b.minMMapSize = minMMapSize
		return nil
	}
}

func WithMinWalThreshold(threshold int64) BucketOption {
	return func(b *Bucket) error {
		b.minWalThreshold = uint64(threshold)
		return nil
	}
}

func WithWalThreshold(threshold uint64) BucketOption {
	return func(b *Bucket) error {
		b.walThreshold = threshold
		return nil
	}
}

// WithLazySegmentLoading enables that segments are only initialized when they are actually used
//
// This option should be used:
//   - For buckets that are NOT used in every request. For example, the object bucket is accessed for
//     almost all operations anyway.
//   - For implicit request only (== requests originating with auto-tenant activation). Explicit activation should
//     always load all segments.
func WithLazySegmentLoading(lazyLoading bool) BucketOption {
	return func(b *Bucket) error {
		b.lazySegmentLoading = lazyLoading
		return nil
	}
}

func WithDirtyThreshold(threshold time.Duration) BucketOption {
	return func(b *Bucket) error {
		b.flushDirtyAfter = threshold
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

func WithAllocChecker(mm memwatch.AllocChecker) BucketOption {
	return func(b *Bucket) error {
		b.allocChecker = mm
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

func WithMaxSegmentSize(maxSegmentSize int64) BucketOption {
	return func(b *Bucket) error {
		b.maxSegmentSize = maxSegmentSize
		return nil
	}
}

func WithSegmentsCleanupInterval(interval time.Duration) BucketOption {
	return func(b *Bucket) error {
		b.segmentsCleanupInterval = interval
		return nil
	}
}

func WithSegmentsChecksumValidationEnabled(enable bool) BucketOption {
	return func(b *Bucket) error {
		b.enableChecksumValidation = enable
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
func WithForceCompaction(opt bool) BucketOption {
	return func(b *Bucket) error {
		b.forceCompaction = opt
		return nil
	}
}

func WithDisableCompaction(disable bool) BucketOption {
	return func(b *Bucket) error {
		b.disableCompaction = disable
		return nil
	}
}

func WithKeepLevelCompaction(keepLevelCompaction bool) BucketOption {
	return func(b *Bucket) error {
		b.keepLevelCompaction = keepLevelCompaction
		return nil
	}
}

func WithKeepSegmentsInMemory(keep bool) BucketOption {
	return func(b *Bucket) error {
		b.keepSegmentsInMemory = keep
		return nil
	}
}

func WithBitmapBufPool(bufPool roaringset.BitmapBufPool) BucketOption {
	return func(b *Bucket) error {
		b.bitmapBufPool = bufPool
		return nil
	}
}

func WithBM25Config(bm25Config *models.BM25Config) BucketOption {
	return func(b *Bucket) error {
		b.bm25Config = bm25Config
		return nil
	}
}

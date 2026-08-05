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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (s *Shard) makeDefaultBucketOptions(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
	options := []lsmkv.BucketOption{
		lsmkv.WithStrategy(strategy),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsChecksumValidationEnabled(s.index.Config.LSMEnableSegmentsChecksumValidation),
		lsmkv.WithMinMMapSize(s.index.Config.MinMMapSize),
		lsmkv.WithMinWalThreshold(s.index.Config.MaxReuseWalSize),
		lsmkv.WithWriteSegmentInfoIntoFileName(s.index.Config.SegmentInfoIntoFileNameEnabled),
		lsmkv.WithWriteMetadata(s.index.Config.WriteMetadataFilesEnabled),
		lsmkv.WithDirtyThreshold(
			time.Duration(s.index.Config.MemtablesFlushDirtyAfter) * time.Second),
		lsmkv.WithSegmentsCleanupInterval(
			time.Duration(s.index.Config.SegmentsCleanupIntervalSeconds) * time.Second),
		lsmkv.WithDynamicMemtableSizing(
			s.index.Config.MemtablesInitialSizeMB,
			s.index.Config.MemtablesMaxSizeMB,
			s.index.Config.MemtablesMinActiveSeconds,
			s.index.Config.MemtablesMaxActiveSeconds,
		),
		lsmkv.WithLazySegmentLoading(s.lazySegmentLoadingEnabled),
	}

	switch strategy {
	case lsmkv.StrategyRoaringSet:
		options = append(options,
			lsmkv.WithBitmapBufPool(s.bitmapBufPool),
		)
	case lsmkv.StrategyRoaringSetRange:
		options = append(options,
			lsmkv.WithBitmapBufPool(s.bitmapBufPool),
			lsmkv.WithKeepSegmentsInMemory(s.index.Config.IndexRangeableInMemory),
			lsmkv.WithUseBloomFilter(false),
		)
	case lsmkv.StrategyMapCollection:
		if s.versioner.Version() < 2 {
			options = append(options,
				lsmkv.WithLegacyMapSorting(),
			)
		}
	case lsmkv.StrategyInverted:
		options = append(options,
			lsmkv.WithLazyPropertyLengths(s.index.Config.LazyPropertyLengthsEnabled),
			lsmkv.WithBM25FilterTombMergeGateRatio(s.index.Config.BM25FilterTombMergeGateRatio),
		)
	}

	return append(options, customOptions...)
}

// containsAcceleratorFactory builds the resident columnar ContainsAny
// accelerator for a roaringset bucket at open, sized to the shard's current
// docID counter. Declines (returns nil) if the counter isn't wired yet or the
// property is not unique (BuildFromBucket with requireUnique errors), in which
// case ContainsAny falls back to the standard fold.
func (s *Shard) containsAcceleratorFactory() lsmkv.ContainsAcceleratorFactory {
	return func(bkt *lsmkv.Bucket) lsmkv.ContainsAnyResolver {
		if s.counter == nil {
			return nil
		}
		idx, err := columnar.BuildFromBucket(bkt, s.counter.Get(), s.index.logger)
		if err != nil {
			return nil
		}
		return idx
	}
}

// detachContainsAccelerator drops the columnar ContainsAny accelerator from
// propName's filterable bucket, if it carries one. Called when the property's
// tokenization is changing: the accelerator's base was built by reading the
// bucket's keys, and a retokenization rewrites what those keys are, which it has
// no way to notice on its own.
func (s *Shard) detachContainsAccelerator(propName string) {
	if propName == "" || s.store == nil {
		return
	}
	if bkt := s.store.Bucket(helpers.BucketFromPropNameLSM(propName)); bkt != nil {
		bkt.DetachContainsAccelerator()
	}
}

func (s *Shard) overwrittenMakeDefaultBucketOptions(overwrittenDefaults ...lsmkv.BucketOption) lsmkv.MakeBucketOptions {
	return func(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
		return s.makeDefaultBucketOptions(strategy, append(overwrittenDefaults, customOptions...)...)
	}
}

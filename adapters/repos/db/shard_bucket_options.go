//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"time"

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
	}

	return append(options, customOptions...)
}

func (s *Shard) overwrittenMakeDefaultBucketOptions(overwrittenDefaults ...lsmkv.BucketOption) lsmkv.MakeBucketOptions {
	return func(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
		return s.makeDefaultBucketOptions(strategy, append(overwrittenDefaults, customOptions...)...)
	}
}

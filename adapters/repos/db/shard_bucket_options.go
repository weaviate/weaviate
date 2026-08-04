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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
		idx, err := columnar.BuildFromBucket(bkt, s.counter.Get(), true, s.index.logger)
		if err != nil {
			return nil
		}
		return idx
	}
}

// detachContainsAccelerator drops the columnar ContainsAny accelerator from
// propName's filterable bucket, if it carries one. Called when the property's
// tokenization is changing: the accelerator was attached because the property
// produced one key per document, and a retokenization can end that without
// producing any error the accelerator itself would notice.
func (s *Shard) detachContainsAccelerator(propName string) {
	if propName == "" || s.store == nil {
		return
	}
	if bkt := s.store.Bucket(helpers.BucketFromPropNameLSM(propName)); bkt != nil {
		bkt.DetachContainsAccelerator()
	}
}

// columnarContainsEligible reports whether a property's filterable bucket takes
// exactly one key per document, which is what the columnar accelerator's
// deletion handling requires: it applies a flushed memtable's deletions to the
// whole result, sound only while a docID belongs to a single key. A property
// that spreads one document across several keys — an array, a reference, or a
// text property tokenized into more than one term — would lose that document
// from every one of its keys as soon as it lost any one of them.
//
// Tokenization is read through the same resolver the query path uses, so a
// property mid-retokenization is judged by the tokenization its bucket actually
// holds. That value can still change afterwards, which is why
// SetTokenizationOverlay detaches the accelerator rather than relying on this
// decision holding for the bucket's lifetime.
func (s *Shard) columnarContainsEligible(prop *models.Property) bool {
	if !entcfg.ColumnarContainsEnabled() {
		return false
	}
	if len(prop.DataType) != 1 || schema.IsArrayDataType(prop.DataType) ||
		schema.IsRefDataType(prop.DataType) {
		return false
	}

	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeText:
		return inverted.ResolveTokenization(s.TokenizationFor, prop.Name, prop.Tokenization) ==
			models.PropertyTokenizationField
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return true
	default:
		// geo, blob, and anything added later opt in deliberately: a datatype
		// must be shown to produce one key per document before it qualifies.
		return false
	}
}

func (s *Shard) overwrittenMakeDefaultBucketOptions(overwrittenDefaults ...lsmkv.BucketOption) lsmkv.MakeBucketOptions {
	return func(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
		return s.makeDefaultBucketOptions(strategy, append(overwrittenDefaults, customOptions...)...)
	}
}

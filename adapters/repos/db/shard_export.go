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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// Exported wrappers so the reindex sub-package can drive the shard's
// inverted-index write path without leaking unexported names. Same
// implementation; new name avoids two-way name churn in the moved
// reindex files. See adapters/repos/db/reindex/iface_shardlike.go.

func (s *Shard) PathLSM() string { return s.pathLSM() }

// Unwrap satisfies the [reindex.ShardLike] interface so reindex code
// can iterate a mixed slice of *Shard / *LazyLoadShard without a
// type-switch. *Shard returns itself; *LazyLoadShard ensures the
// underlying shard is loaded first.
func (s *Shard) Unwrap(_ context.Context) (reindex.ShardLike, error) {
	return s, nil
}

func (s *Shard) AddToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	return s.addToPropertySetBucket(bucket, docID, key)
}

func (s *Shard) DeleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	return s.deleteFromPropertySetBucket(bucket, docID, key)
}

func (s *Shard) AddToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	return s.addToPropertyMapBucket(bucket, pair, key)
}

func (s *Shard) AddToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	return s.addToPropertyRangeBucket(bucket, docID, key)
}

func (s *Shard) DeleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	return s.deleteFromPropertyRangeBucket(bucket, docID, key)
}

func (s *Shard) DeleteInvertedIndexItemWithFrequencyLSM(bucket *lsmkv.Bucket, item inverted.Countable, docID uint64) error {
	return s.deleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID)
}

func (s *Shard) PairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	return s.pairPropertyWithFrequency(docID, freq, propLen)
}

func (s *Shard) MakeDefaultBucketOptions(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
	return s.makeDefaultBucketOptions(strategy, customOptions...)
}

func (s *Shard) MarkSearchableBlockmaxProperties(propNames ...string) {
	s.markSearchableBlockmaxProperties(propNames...)
}

func (s *Shard) SetRangeableLocallyReady(prop string, ready bool) {
	s.setRangeableLocallyReady(prop, ready)
}

// RegisterAddToPropertyValueIndex installs cb as a double-write
// callback during reindex. Returns a disable closure; reindex stores
// it and invokes during teardown to stop the callback firing.
func (s *Shard) RegisterAddToPropertyValueIndex(cb reindex.OnAddToPropertyValueIndex) func() {
	return s.registerAddToPropertyValueIndex(func(shard *Shard, docID uint64, property *inverted.Property) error {
		return cb(shard, docID, property)
	})
}

// RegisterDeleteFromPropertyValueIndex installs cb as a double-write
// callback during reindex; see [Shard.RegisterAddToPropertyValueIndex]
// for the return-value contract.
func (s *Shard) RegisterDeleteFromPropertyValueIndex(cb reindex.OnDeleteFromPropertyValueIndex) func() {
	return s.registerDeleteFromPropertyValueIndex(func(shard *Shard, docID uint64, property *inverted.Property) error {
		return cb(shard, docID, property)
	})
}

func (s *Shard) SetFallbackToSearchable(fallback bool) {
	s.setFallbackToSearchable(fallback)
}

// ParentIndex returns the parent index as a [reindex.IndexLike]
// handle. *Shard.Index() (returning the concrete *Index) remains the
// db-internal accessor; ParentIndex is the reindex-facing one.
func (s *Shard) ParentIndex() reindex.IndexLike {
	return s.index.ReindexHandle()
}

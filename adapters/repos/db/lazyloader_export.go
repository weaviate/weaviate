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

// Exported wrappers on *LazyLoadShard that mirror [Shard]'s reindex-
// facing surface. Each forwards to the underlying shard after
// ensuring it is loaded. See shard_export.go for the *Shard side.

func (l *LazyLoadShard) PathLSM() string {
	l.mustLoad()
	return l.shard.PathLSM()
}

func (l *LazyLoadShard) AddToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.AddToPropertySetBucket(bucket, docID, key)
}

func (l *LazyLoadShard) DeleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.DeleteFromPropertySetBucket(bucket, docID, key)
}

func (l *LazyLoadShard) AddToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	l.mustLoad()
	return l.shard.AddToPropertyMapBucket(bucket, pair, key)
}

func (l *LazyLoadShard) AddToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.AddToPropertyRangeBucket(bucket, docID, key)
}

func (l *LazyLoadShard) DeleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.DeleteFromPropertyRangeBucket(bucket, docID, key)
}

func (l *LazyLoadShard) DeleteInvertedIndexItemWithFrequencyLSM(bucket *lsmkv.Bucket, item inverted.Countable, docID uint64) error {
	l.mustLoad()
	return l.shard.DeleteInvertedIndexItemWithFrequencyLSM(bucket, item, docID)
}

func (l *LazyLoadShard) PairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	l.mustLoad()
	return l.shard.PairPropertyWithFrequency(docID, freq, propLen)
}

func (l *LazyLoadShard) MakeDefaultBucketOptions(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption {
	l.mustLoad()
	return l.shard.MakeDefaultBucketOptions(strategy, customOptions...)
}

func (l *LazyLoadShard) MarkSearchableBlockmaxProperties(propNames ...string) {
	l.mustLoad()
	l.shard.MarkSearchableBlockmaxProperties(propNames...)
}

func (l *LazyLoadShard) SetRangeableLocallyReady(prop string, ready bool) {
	l.mustLoad()
	l.shard.SetRangeableLocallyReady(prop, ready)
}

func (l *LazyLoadShard) RegisterAddToPropertyValueIndex(cb reindex.OnAddToPropertyValueIndex) func() {
	l.mustLoad()
	return l.shard.RegisterAddToPropertyValueIndex(cb)
}

func (l *LazyLoadShard) RegisterDeleteFromPropertyValueIndex(cb reindex.OnDeleteFromPropertyValueIndex) func() {
	l.mustLoad()
	return l.shard.RegisterDeleteFromPropertyValueIndex(cb)
}

func (l *LazyLoadShard) SetFallbackToSearchable(fallback bool) {
	l.mustLoad()
	l.shard.SetFallbackToSearchable(fallback)
}

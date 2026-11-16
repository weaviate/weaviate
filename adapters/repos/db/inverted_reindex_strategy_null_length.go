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
	"fmt"
	"slices"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// NullLengthStrategy builds the null-state bucket, or with length the property-length
// bucket, of a property an enable run indexes for the first time: only then do they exist.
type NullLengthStrategy struct {
	length     bool
	propNames  []string
	generation int // see genSuffix godoc
}

func (s *NullLengthStrategy) PreReindexHook(*Shard, []string) {}

func (s *NullLengthStrategy) StrategyCode() MigrationStrategyCode {
	if s.length {
		return StrategyCodePropertyLength
	}
	return StrategyCodeNullState
}

func (s *NullLengthStrategy) SourceBucketName(propName string) string {
	if s.length {
		return helpers.BucketFromPropNameLengthLSM(propName)
	}
	return helpers.BucketFromPropNameNullLSM(propName)
}

func (s *NullLengthStrategy) suffixBase() string {
	if s.length {
		return "__enable_proplength_"
	}
	return "__enable_nullstate_"
}

func (s *NullLengthStrategy) ReindexSuffix() string {
	return s.suffixBase() + "reindex" + genSuffix(s.generation)
}

func (s *NullLengthStrategy) IngestSuffix() string {
	return s.suffixBase() + "ingest" + genSuffix(s.generation)
}

func (s *NullLengthStrategy) SourceStrategy() string { return lsmkv.StrategyRoaringSet }

func (s *NullLengthStrategy) TargetStrategy() string { return lsmkv.StrategyRoaringSet }

func (s *NullLengthStrategy) SourceIndexType() PropertyIndexType {
	if s.length {
		return IndexTypePropLength
	}
	return IndexTypePropNull
}

func (s *NullLengthStrategy) key(shard ShardLike, prop inverted.Property) (key []byte, ok bool, err error) {
	e := nullLengthEntryOf(shard.Index().invertedIndexConfig, prop)
	switch {
	case s.length && e.addLength:
		key, err = bucketKeyPropertyLength(e.length)
	case !s.length && e.addNull:
		key, err = bucketKeyPropertyNull(e.isNull)
	default:
		return nil, false, nil
	}
	return key, err == nil, err
}

func (s *NullLengthStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	key, ok, err := s.key(shard, prop)
	if !ok {
		return err
	}
	if err := bucket.RoaringSetAddOne(key, docID); err != nil {
		return fmt.Errorf("adding prop '%s' to %s: %w", prop.Name, s.StrategyCode(), err)
	}
	return nil
}

func (s *NullLengthStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return s.callback(bucketNamer, armed, (*Shard).addToPropertySetBucket)
}

func (s *NullLengthStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return onDeleteFromPropertyValueIndex(s.callback(bucketNamer, armed, (*Shard).deleteFromPropertySetBucket))
}

func (s *NullLengthStrategy) callback(bucketNamer func(string) string, armed armedMirror,
	apply func(*Shard, *lsmkv.Bucket, uint64, []byte) error,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		key, ok, err := s.key(shard, *property)
		if !ok {
			return err
		}
		if err := apply(shard, bucket, docID, key); err != nil {
			return fmt.Errorf("mirroring prop '%s' into bucket '%s': %w", property.Name, bucketName, err)
		}
		return nil
	}
}

// Only its own properties: the swap asks every task of a run for the whole payload.
func (s *NullLengthStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	var out map[string]inverted.PropertyOverlay
	for _, p := range props {
		if !slices.Contains(s.propNames, p) {
			continue
		}
		if out == nil {
			out = make(map[string]inverted.PropertyOverlay, len(props))
		}
		out[p] = inverted.PropertyOverlay{TracksNullState: !s.length, TracksPropertyLength: s.length}
	}
	return out
}

func (s *NullLengthStrategy) OnMigrationComplete(context.Context, ShardLike) error {
	return nil
}

func NewRuntimeNullLengthTask(logger logrus.FieldLogger, length bool, propNames []string,
	collectionName string, generation int,
) *ShardReindexTaskGeneric {
	strategy := &NullLengthStrategy{length: length, propNames: propNames, generation: generation}
	selected := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selected[p] = struct{}{}
	}
	cfg := reindexTaskConfig{
		concurrency:                   2,
		memtableOptFactor:             4,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,
		selectionEnabled:              true,
		selectedPropsByCollection:     map[string]map[string]struct{}{collectionName: selected},
		selectedShardsByCollection:    map[string]map[string]struct{}{collectionName: nil},
	}
	name := "EnableNullState"
	if length {
		name = "EnablePropertyLength"
	}
	return NewShardReindexTaskGeneric(name, logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync, defaultIndexClosingGuard)
}

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
	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// FilterableToRangeableStrategy implements MigrationStrategy for building
// RoaringSetRange (rangeable) indexes. The strategy is used both as
// "enable-rangeable" (the property currently has no rangeable index) and as
// "repair-rangeable" (the rangeable index already exists and is being
// refreshed).
//
// Backfill source. The reindex iterator scans the objects bucket and runs
// the inverted analyzer to derive per-property values. It does NOT read
// from the filterable bucket — that bucket may not even exist (e.g. a
// numeric property created with IndexFilterable=false explicitly). The
// "FilterableToRangeable" name is historical; treat it as "build rangeable
// from objects".
//
// Schema-flag gating. During the backfill scan, IndexRangeFilters is still
// false on the target property until the cluster-wide flip. Without
// an AnalyzerOverlay forcing the rangeable flag on, the analyzer would
// either drop the property entirely (HasAnyInvertedIndex=false when the
// property is also IndexFilterable=false) or emit it with
// HasRangeableIndex=false. Either way the new rangeable bucket would end
// up empty and the task would still report FINISHED — the silent
// data-loss bug pinned by the int__filt=false_range=nil/false matrix
// cells.
type FilterableToRangeableStrategy struct {
	propNames  []string
	generation int // see genSuffix godoc
}

func (s *FilterableToRangeableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketRangeableFromPropNameLSM(propName)
}

func (s *FilterableToRangeableStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSetRange
}

func (s *FilterableToRangeableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	for _, item := range prop.Items {
		if len(item.Data) != 8 {
			return fmt.Errorf("rangeable prop '%s' docID %d: invalid value length %d, should be 8 bytes",
				prop.Name, docID, len(item.Data))
		}
		if err := bucket.RoaringSetRangeAdd(binary.BigEndian.Uint64(item.Data), docID); err != nil {
			return fmt.Errorf("adding rangeable prop '%s' docID %d: %w", prop.Name, docID, err)
		}
	}
	return nil
}

func (s *FilterableToRangeableStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — the property may be
		// IndexFilterable=false, and we still need to populate the
		// rangeable bucket from the live write. Scope is enforced via
		// the armed scope.
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		for _, item := range property.Items {
			if err := shard.addToPropertyRangeBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("adding rangeable prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableToRangeableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		// Don't gate on HasFilterableIndex — see MakeAddCallback.
		bucket, bucketName, skip := resolveScopedDoubleWriteBucket(shard, property,
			armed, bucketNamer, s.SourceBucketName)
		if skip {
			return nil
		}
		for _, item := range property.Items {
			if err := shard.deleteFromPropertyRangeBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("deleting rangeable prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableToRangeableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]inverted.PropertyOverlay, len(props))
	for _, p := range props {
		out[p] = inverted.PropertyOverlay{ForceRangeable: true}
	}
	return out
}

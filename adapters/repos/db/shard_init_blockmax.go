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
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

// There is at least a searchable bucket in the shard
// that isn't using the block max inverted index
func (s *Shard) areAllSearchableBucketsBlockMax() bool {
	for name, bucket := range s.Store().GetBucketsByName() {
		_, indexType := GetPropNameAndIndexTypeFromBucketName(name)
		if bucket.Strategy() == lsmkv.StrategyMapCollection && indexType == IndexTypePropSearchableValue {
			return false
		}
	}
	return true
}

// SearchablePropertyIsBlockmax reports the actual BlockMax-vs-WAND state of a
// single property's searchable inverted bucket on THIS node, independent of
// the class-wide UsingBlockMaxWAND flag.
//
// The class flag flips only after EVERY searchable property in the class has
// migrated (see ReindexProvider.shouldDeferBlockmaxFlip), so on a
// multi-searchable-property class one property can already be blockmax while
// the class flag is still false. The GA API needs this per-property truth to
// (a) return 200 NO_OP for a repeat blockmax PUT on an already-migrated
// property, (b) allow rebuild on it, and (c) report its true algorithm in
// GET /indexes — all of which otherwise contradict the property's real state
// until the last sibling property migrates.
//
// known=false means the property's searchable bucket is not observable on
// this node (collection/shards not loaded, or no searchable bucket exists) —
// callers fall back to the class-level flag. When observable, blockmax is
// true only if every loaded shard's searchable bucket for the property is on
// the inverted (blockmax) strategy; a single map-strategy shard yields false.
//
// Read-only: it inspects bucket strategy exactly like shouldDeferBlockmaxFlip
// and never touches the deferred-flip machinery itself.
func (db *DB) SearchablePropertyIsBlockmax(collection, propName string) (blockmax, known bool) {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return false, false
	}
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	var sawBucket, anyMap bool
	idx.ForEachLoadedShard(func(_ string, sh ShardLike) error {
		concrete, err := unwrapShard(context.Background(), sh)
		if err != nil {
			return nil
		}
		bucket := concrete.Store().Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		sawBucket = true
		if bucket.Strategy() == lsmkv.StrategyMapCollection {
			anyMap = true
		}
		return nil
	})
	if !sawBucket {
		return false, false
	}
	return !anyMap, true
}

func structToMap(obj interface{}) (newMap interface{}) {
	if obj == nil {
		return nil
	}
	data, _ := json.Marshal(obj)  // Convert to a json string
	json.Unmarshal(data, &newMap) // Convert to a map
	return newMap
}

func updateToBlockMaxInvertedIndexConfig(ctx context.Context, sc *schema.Manager, className string) error {
	class := sc.ReadOnlyClass(className)
	if class == nil {
		return fmt.Errorf("class %q not found", className)
	}
	// nothing to update
	if class.InvertedIndexConfig.UsingBlockMaxWAND {
		return nil
	}
	class.ModuleConfig = structToMap(class.ModuleConfig)
	class.VectorIndexConfig = structToMap(class.VectorIndexConfig)
	class.ShardingConfig = structToMap(class.ShardingConfig)
	for i := range class.VectorConfig {
		tempConfig := class.VectorConfig[i]
		tempConfig.VectorIndexConfig = structToMap(tempConfig.VectorIndexConfig)
		tempConfig.Vectorizer = structToMap(tempConfig.Vectorizer)
		class.VectorConfig[i] = tempConfig
	}
	class.InvertedIndexConfig.UsingBlockMaxWAND = true
	return schema.UpdateClassInternal(&sc.Handler, ctx, className, class)
}

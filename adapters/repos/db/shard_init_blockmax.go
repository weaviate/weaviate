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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/proto/api"
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

func structToMap(obj interface{}) (newMap interface{}) {
	if obj == nil {
		return nil
	}
	data, _ := json.Marshal(obj)  // Convert to a json string
	json.Unmarshal(data, &newMap) // Convert to a map
	return newMap
}

// updateToBlockMaxInvertedIndexConfig flips the class-level
// InvertedIndexConfig.UsingBlockMaxWAND flag to true after the last
// per-property MapToBlockmax migration completes on this shard.
//
// The mutation goes through a TYPE_UPDATE_CLASS RAFT command with a
// FieldsToUpdate mask = [invertedIndexConfig] (mirroring the existing
// UpdatePropertyRequest.FieldsToUpdate pattern), so both the FSM
// merge AND the executor's downstream migrator dispatch are
// restricted to the inverted-index sub-config.
//
// Without the mask, the executor's UpdateClass re-applies every
// non-nil sub-config — including VectorIndexConfig via
// Migrator.UpdateVectorIndexConfig → Shard.UpdateVectorIndexConfig
// → Shard.SetStatusReadonly. That window has been observed to
// overlap with a concurrent runtime-reindex iteration's write to the
// per-prop blockmax_reindex bucket and surface as
// "store is read-only" mid-iteration. See
// weaviate/0-weaviate-issues#240.
func updateToBlockMaxInvertedIndexConfig(ctx context.Context, sc *schema.Manager, className string) error {
	class := sc.ReadOnlyClass(className)
	if class == nil {
		return fmt.Errorf("class %q not found", className)
	}
	// nothing to update
	if class.InvertedIndexConfig.UsingBlockMaxWAND {
		return nil
	}
	// structToMap on the sibling sub-configs is still required so
	// ParseClassUpdate's type assertions on the round-tripped class
	// succeed — even though the mask skips applying them downstream,
	// the parse / validate pipeline (prepareClassForUpdate) still
	// inspects them.
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
	return schema.UpdateClassInternalMasked(&sc.Handler, ctx, className, class,
		api.ClassFieldInvertedIndexConfig)
}

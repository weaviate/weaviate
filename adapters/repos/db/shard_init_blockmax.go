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

package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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

func structToMap(obj interface{}) (newMap map[string]interface{}) {
	data, _ := json.Marshal(obj)  // Convert to a json string
	json.Unmarshal(data, &newMap) // Convert to a map
	return
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

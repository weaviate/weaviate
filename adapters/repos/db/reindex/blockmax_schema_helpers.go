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

package reindex

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/usecases/schema"
)

// structToMap round-trips through JSON to drop struct tags and pointer
// receivers from a configuration object so it survives the schema
// manager's deep copy.
func structToMap(obj interface{}) (newMap interface{}) {
	if obj == nil {
		return nil
	}
	data, _ := json.Marshal(obj)
	json.Unmarshal(data, &newMap)
	return newMap
}

// updateToBlockMaxInvertedIndexConfig flips the class-level
// UsingBlockMaxWAND flag through the schema manager. Called by the
// Map→BlockMax strategy at the end of a successful migration.
func updateToBlockMaxInvertedIndexConfig(ctx context.Context, sc *schema.Manager, className string) error {
	class := sc.ReadOnlyClass(className)
	if class == nil {
		return fmt.Errorf("class %q not found", className)
	}
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

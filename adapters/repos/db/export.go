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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ExportShardLike is the minimal shard interface needed for export operations.
// Matches usecases/export.ShardLike.
type ExportShardLike = interface {
	Store() *lsmkv.Store
	Name() string
}

// GetShardsForClass returns all local shards for a class.
func (db *DB) GetShardsForClass(ctx context.Context, className string) ([]ExportShardLike, error) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, fmt.Errorf("index not found for class %s", className)
	}

	var shards []ExportShardLike
	err := idx.ForEachShard(func(name string, shard ShardLike) error {
		shards = append(shards, shard)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterate shards: %w", err)
	}

	return shards, nil
}

// ShardOwnership returns a map of node name to shard names for a given class.
// Only the primary owner (BelongsToNodes[0]) is included for each shard,
// ensuring replicated shards are never exported twice.
func (db *DB) ShardOwnership(ctx context.Context, className string) (map[string][]string, error) {
	result := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			if len(shard.BelongsToNodes) == 0 {
				return fmt.Errorf("shard %s of class %s has no assigned nodes", shardName, className)
			}
			primaryNode := shard.BelongsToNodes[0]
			result[primaryNode] = append(result[primaryNode], shardName)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	return result, nil
}

// ListClasses returns all class names (already exists on DB, this is a convenience alias comment).
// DB.ListClasses is defined in index.go.

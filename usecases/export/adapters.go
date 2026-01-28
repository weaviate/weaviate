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

package export

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
)

// DBLike defines the DB interface needed for export operations
type DBLike interface {
	GetIndex(className schema.ClassName) IndexLike
	ListClasses(ctx context.Context) []string
}

// IndexLike defines the Index interface needed for export
type IndexLike interface {
	ForEachShard(f func(name string, shard ShardLike) error) error
}

// DBAdapter adapts the DB interface for export operations
type DBAdapter struct {
	db DBLike
}

// NewDBAdapter creates a new DB adapter
func NewDBAdapter(db DBLike) *DBAdapter {
	return &DBAdapter{db: db}
}

// GetShardsForClass returns all shards for a class
func (a *DBAdapter) GetShardsForClass(ctx context.Context, className string) ([]ShardLike, error) {
	index := a.db.GetIndex(schema.ClassName(className))
	if index == nil {
		return nil, fmt.Errorf("index not found for class %s", className)
	}

	var shards []ShardLike
	err := index.ForEachShard(func(name string, shard ShardLike) error {
		shards = append(shards, shard)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterate shards: %w", err)
	}

	return shards, nil
}

// ListClasses returns all available classes
func (a *DBAdapter) ListClasses(ctx context.Context) []string {
	return a.db.ListClasses(ctx)
}

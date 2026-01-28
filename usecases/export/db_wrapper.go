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

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/schema"
)

// dbWrapper wraps *db.DB to implement DBLike
type dbWrapper struct {
	db *db.DB
}

// NewDBWrapper creates a wrapper around *db.DB
func NewDBWrapper(database *db.DB) DBLike {
	return &dbWrapper{db: database}
}

// GetIndex returns an indexWrapper that implements IndexLike
func (w *dbWrapper) GetIndex(className schema.ClassName) IndexLike {
	idx := w.db.GetIndex(className)
	if idx == nil {
		return nil
	}
	return &indexWrapper{index: idx}
}

// ListClasses returns all classes
func (w *dbWrapper) ListClasses(ctx context.Context) []string {
	return w.db.ListClasses(ctx)
}

// indexWrapper wraps *db.Index to implement IndexLike
type indexWrapper struct {
	index *db.Index
}

// ForEachShard iterates over shards
func (w *indexWrapper) ForEachShard(f func(name string, shard ShardLike) error) error {
	// Wrap the callback to handle the db.ShardLike -> ShardLike conversion
	return w.index.ForEachShard(func(name string, shard db.ShardLike) error {
		return f(name, shard)
	})
}

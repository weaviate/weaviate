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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/entities/schema"
)

// reindexIndexHandle wraps *Index so the reindex package can consume
// it through the narrower [reindex.IndexLike] interface without
// having a back-edge from reindex → db. It exists only to satisfy
// signature differences: *Index.ForEachShard takes a db.ShardLike
// callback whereas reindex.IndexLike expects reindex.ShardLike — the
// adapter forwards the call and lets the structural-typing assignment
// happen at the closure boundary.
type reindexIndexHandle struct{ idx *Index }

func (h reindexIndexHandle) ID() string                  { return h.idx.ID() }
func (h reindexIndexHandle) ClassName() schema.ClassName { return h.idx.Config.ClassName }
func (h reindexIndexHandle) Logger() logrus.FieldLogger  { return h.idx.logger }
func (h reindexIndexHandle) GetSchema() reindex.SchemaGetter {
	return h.idx.getSchema
}

func (h reindexIndexHandle) ConfigSnapshot() reindex.IndexConfig {
	return reindex.IndexConfig{
		ClassName:                 h.idx.Config.ClassName,
		RootPath:                  h.idx.Config.RootPath,
		ReplicationFactor:         h.idx.Config.ReplicationFactor,
		MinMMapSize:               h.idx.Config.MinMMapSize,
		MaxReuseWalSize:           h.idx.Config.MaxReuseWalSize,
		MemtablesFlushDirtyAfter:  h.idx.Config.MemtablesFlushDirtyAfter,
		MemtablesInitialSizeMB:    h.idx.Config.MemtablesInitialSizeMB,
		MemtablesMaxSizeMB:        h.idx.Config.MemtablesMaxSizeMB,
		MemtablesMinActiveSeconds: h.idx.Config.MemtablesMinActiveSeconds,
		MemtablesMaxActiveSeconds: h.idx.Config.MemtablesMaxActiveSeconds,
	}
}

func (h reindexIndexHandle) InvertedIndexConfig() schema.InvertedIndexConfig {
	return h.idx.invertedIndexConfig
}

func (h reindexIndexHandle) GetShard(ctx context.Context, shardName string) (reindex.ShardLike, func(), error) {
	shard, release, err := h.idx.GetShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	if shard == nil {
		return nil, release, nil
	}
	return asReindexShard(shard), release, nil
}

func (h reindexIndexHandle) GetShardOrNil(shardName string) reindex.ShardLike {
	loaded := h.idx.shards.Load(shardName)
	if loaded == nil {
		return nil
	}
	return asReindexShard(loaded)
}

func (h reindexIndexHandle) ForEachShard(fn func(name string, shard reindex.ShardLike) error) error {
	return h.idx.ForEachShard(func(name string, shard ShardLike) error {
		return fn(name, asReindexShard(shard))
	})
}

func (h reindexIndexHandle) ForEachLoadedShard(fn func(name string, shard reindex.ShardLike) error) error {
	return h.idx.ForEachLoadedShard(func(name string, shard ShardLike) error {
		return fn(name, asReindexShard(shard))
	})
}

// asReindexShard narrows a db.ShardLike to the reindex package's
// ShardLike. *Shard and *LazyLoadShard both satisfy reindex.ShardLike
// via the exported wrappers in shard_export.go and
// lazyloader_export.go. Panics on a mismatch — a db.ShardLike that
// doesn't satisfy reindex.ShardLike means a new ShardLike
// implementation has landed without picking up the reindex-facing
// surface, and silently returning nil here would hide that until a
// later nil-dereference inside the reindex package.
func asReindexShard(shard ShardLike) reindex.ShardLike {
	r, ok := shard.(reindex.ShardLike)
	if !ok {
		panic(fmt.Sprintf("db.ShardLike of type %T does not satisfy reindex.ShardLike — extend the new wrapper with the reindex-facing exported surface (see shard_export.go / lazyloader_export.go)", shard))
	}
	return r
}

func (h reindexIndexHandle) RefuseIfReindexInFlight(shardName string) error {
	return h.idx.refuseIfReindexInFlight(shardName)
}

func (h reindexIndexHandle) WithDropLock(fn func()) {
	h.idx.dropIndex.RLock()
	defer h.idx.dropIndex.RUnlock()
	fn()
}

// ReindexHandle returns the adapter satisfying [reindex.IndexLike].
// Used by *Shard.ParentIndex and *LazyLoadShard.ParentIndex.
func (i *Index) ReindexHandle() reindex.IndexLike { return reindexIndexHandle{idx: i} }

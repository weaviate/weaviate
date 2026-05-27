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

	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/entities/schema"
)

// reindexDBHandle wraps *DB so the reindex package can consume it via
// the narrower reindex.DBLike interface without a back-edge. The
// adapter's only job is to bridge return-type differences (e.g.
// *Index → reindex.IndexLike) that Go's structural typing won't
// reconcile automatically.
type reindexDBHandle struct{ db *DB }

func (h reindexDBHandle) RootPath() string { return h.db.config.RootPath }

func (h reindexDBHandle) GetIndex(className schema.ClassName) reindex.IndexLike {
	idx := h.db.GetIndex(className)
	if idx == nil {
		return nil
	}
	return idx.ReindexHandle()
}

func (h reindexDBHandle) WaitForStartup(ctx context.Context) error {
	return h.db.WaitForStartup(ctx)
}

func (h reindexDBHandle) WithLoadedIndices(fn func(loadedByID map[string]reindex.IndexLike)) {
	h.db.indexLock.RLock()
	loaded := make(map[string]reindex.IndexLike, len(h.db.indices))
	for id, idx := range h.db.indices {
		loaded[id] = idx.ReindexHandle()
	}
	h.db.indexLock.RUnlock()
	fn(loaded)
}

func (h reindexDBHandle) CleanStalePartialReindexState(ctx context.Context, collection, propName, indexType string) error {
	return h.db.CleanStalePartialReindexState(ctx, collection, propName, indexType)
}

func (h reindexDBHandle) ShardReplicaOwnership(ctx context.Context, className string) (map[string][]string, error) {
	return h.db.ShardReplicaOwnership(ctx, className)
}

func (h reindexDBHandle) ShardReplicaOwnershipForMT(ctx context.Context, className string, tenantNames []string) (map[string][]string, error) {
	return h.db.ShardReplicaOwnershipForMT(ctx, className, tenantNames)
}

// ReindexHandle returns the adapter satisfying [reindex.DBLike]. Pass
// it to reindex.NewReindexProvider in lieu of *DB directly.
func (d *DB) ReindexHandle() reindex.DBLike { return reindexDBHandle{db: d} }

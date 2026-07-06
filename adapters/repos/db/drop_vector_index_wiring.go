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
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

// EditOpBucketsForShards resolves the edit-ops objects buckets for the given
// local shards in a single walk, so the drop-vector provider can register ops and
// poll pending sets without an O(shards) lookup per unit. A shard absent from the
// result is not locally available (a deactivated tenant, or a lazy shard that
// failed to load). Lazy shards are loaded explicitly with the error surfaced —
// never via Store()'s panicking mustLoad — acceptable for a drop, a rare
// operator-initiated action that must touch every shard once anyway.
func (db *DB) EditOpBucketsForShards(ctx context.Context, collection string, shardNames []string) (map[string]editOpBucket, error) {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return nil, fmt.Errorf("index for collection %q not found", collection)
	}
	wanted := make(map[string]struct{}, len(shardNames))
	for _, name := range shardNames {
		wanted[name] = struct{}{}
	}
	buckets := make(map[string]editOpBucket, len(shardNames))
	if err := idx.ForEachShard(func(name string, s ShardLike) error {
		if _, ok := wanted[name]; !ok {
			return nil
		}
		if lazy, ok := s.(*LazyLoadShard); ok {
			if err := lazy.Load(ctx); err != nil {
				db.logger.WithField("collection", collection).WithField("shard", name).
					Warnf("drop-vector: load lazy shard: %v", err)
				return nil // absent from result; the unit fails instead of panicking
			}
		}
		if b := s.Store().Bucket(helpers.ObjectsBucketLSM); b != nil {
			buckets[name] = b
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return buckets, nil
}

// EditOpBucketsForLoadedShards is EditOpBucketsForShards restricted to shards
// already loaded — it never loads a shard. Used by the task-completion op delete,
// which must not force-load (a replayed completion callback on a node with many
// lazy/inactive shards would otherwise mass-load them); an unloaded shard's op is
// disarmed by the sweep on its next load instead.
func (db *DB) EditOpBucketsForLoadedShards(collection string, shardNames []string) (map[string]editOpBucket, error) {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return nil, fmt.Errorf("index for collection %q not found", collection)
	}
	wanted := make(map[string]struct{}, len(shardNames))
	for _, name := range shardNames {
		wanted[name] = struct{}{}
	}
	buckets := make(map[string]editOpBucket, len(shardNames))
	if err := idx.ForEachLoadedShard(func(name string, s ShardLike) error {
		if _, ok := wanted[name]; ok {
			if b := s.Store().Bucket(helpers.ObjectsBucketLSM); b != nil {
				buckets[name] = b
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return buckets, nil
}

// EnsureDroppedVectorFilesRemoved removes the on-disk artifacts (LSM buckets +
// HNSW dirs) of the dropped named vectors for a shard. Idempotent (os.RemoveAll).
func (db *DB) EnsureDroppedVectorFilesRemoved(collection, shardName string, targets []string) error {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return fmt.Errorf("index for collection %q not found", collection)
	}
	helper := newVectorDropIndexHelper()
	for _, target := range targets {
		if err := helper.removeVectorIndexFiles(idx.path(), shardName, target); err != nil {
			return err
		}
	}
	return nil
}

// schemaClassUpdater is the slice of the schema manager the finalizer needs: read a
// class and apply an internal class update. Narrowed to an interface so the
// finalizer's read-modify-write / retry / guard logic is unit-testable.
type schemaClassUpdater interface {
	ReadOnlyClass(collection string) *models.Class
	UpdateClassInternal(ctx context.Context, collection string, updated *models.Class) error
}

// schemaVectorConfigFinalizer removes dropped named-vector entries from a class's
// VectorConfig via the internal schema update path, with fresh read-modify-write
// and bounded retry. Implements dropVectorSchemaFinalizer.
type schemaVectorConfigFinalizer struct {
	mgr schemaClassUpdater
}

// managerClassUpdater adapts *schema.Manager to schemaClassUpdater.
type managerClassUpdater struct{ mgr *schema.Manager }

func (a managerClassUpdater) ReadOnlyClass(collection string) *models.Class {
	return a.mgr.ReadOnlyClass(collection)
}

func (a managerClassUpdater) UpdateClassInternal(ctx context.Context, collection string, updated *models.Class) error {
	return schema.UpdateClassInternal(&a.mgr.Handler, ctx, collection, updated)
}

// NewSchemaVectorConfigFinalizer builds the schema finalizer used to construct
// the DropVectorIndexProvider (exported so the REST wiring can pass it).
func NewSchemaVectorConfigFinalizer(mgr *schema.Manager) *schemaVectorConfigFinalizer {
	return &schemaVectorConfigFinalizer{mgr: managerClassUpdater{mgr}}
}

// deepCopyClass returns a fully independent copy (JSON round-trip; finalize is
// rare, cost is irrelevant).
func deepCopyClass(c *models.Class) (*models.Class, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var cp models.Class
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

const dropVectorFinalizeMaxAttempts = 5

func (f *schemaVectorConfigFinalizer) RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error {
	var lastErr error
	for attempt := 0; attempt < dropVectorFinalizeMaxAttempts; attempt++ {
		// Fresh read each attempt so a concurrent update doesn't get clobbered.
		orig := f.mgr.ReadOnlyClass(collection)
		if orig == nil {
			return fmt.Errorf("drop-vector finalize: class %q not found", collection)
		}

		// ReadOnlyClass returns a SHALLOW clone whose nested pointers are shared
		// with the live FSM class; the update path (setClassDefaults etc.) writes
		// through them. Deep-copy before mutating anything.
		next, err := deepCopyClass(orig)
		if err != nil {
			return fmt.Errorf("drop-vector finalize: copy class %q: %w", collection, err)
		}
		next.VectorConfig = make(map[string]models.VectorConfig, len(orig.VectorConfig))
		changed := false
		for name, cfg := range orig.VectorConfig {
			// Case-insensitive to match the conflict/preflight checks; only remove an
			// entry still marked dropped (keep a live same-name re-creation).
			isTarget := slices.ContainsFunc(targets, func(t string) bool { return strings.EqualFold(t, name) })
			if isTarget && modelsext.IsVectorIndexDropped(cfg) {
				changed = true
				continue
			}
			next.VectorConfig[name] = cfg
		}
		if !changed {
			return nil // idempotent: entries already gone
		}

		if err := f.mgr.UpdateClassInternal(ctx, collection, next); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 50 * time.Millisecond):
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("drop-vector finalize: bounded retry exhausted: %w", lastErr)
}

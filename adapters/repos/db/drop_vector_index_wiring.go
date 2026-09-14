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
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

// EditOpBucketsForShards resolves the edit-ops objects buckets for the given
// local shards by direct map lookup — O(requested), not O(collection) — so a
// bounded round on a huge MT collection does not pay a full shard-map walk. A
// shard absent from the result is not locally available (a deactivated
// tenant, or a lazy shard that failed to load). Lazy shards are loaded
// explicitly with the error surfaced — never via Store()'s panicking mustLoad
// — acceptable for a drop, a rare operator-initiated action that must touch
// every shard once anyway.
func (db *DB) EditOpBucketsForShards(ctx context.Context, collection string, shardNames []string) (map[string]editOpBucket, error) {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return nil, fmt.Errorf("index for collection %q not found", collection)
	}
	buckets := make(map[string]editOpBucket, len(shardNames))
	for _, name := range shardNames {
		s := idx.shards.Load(name)
		if s == nil {
			continue
		}
		if lazy, ok := s.(*LazyLoadShard); ok {
			if err := lazy.Load(ctx); err != nil {
				db.logger.WithField("collection", collection).WithField("shard", name).
					WithFields(enterrors.DocsLinkFields(err)).
					Warnf("drop-vector: load lazy shard: %v", err)
				continue // absent from result; the unit fails instead of panicking
			}
		}
		if b := s.Store().Bucket(helpers.ObjectsBucketLSM); b != nil {
			buckets[name] = b
		}
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
	buckets := make(map[string]editOpBucket, len(shardNames))
	for _, name := range shardNames {
		s := idx.shards.Load(name)
		if s == nil {
			continue
		}
		if lazy, ok := s.(*LazyLoadShard); ok {
			lazy.mutex.Lock()
			loaded := lazy.loaded
			lazy.mutex.Unlock()
			if !loaded {
				continue
			}
		}
		if b := s.Store().Bucket(helpers.ObjectsBucketLSM); b != nil {
			buckets[name] = b
		}
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
	class := idx.getClass()
	for _, target := range targets {
		// Siblings are read per target: the collection's other vector names are
		// what stops a drop deleting a live vector whose own bucket happens to
		// share a name with one of this target's artifacts.
		if err := helper.removeVectorIndexFiles(idx.path(), shardName, target,
			otherTargetVectors(class, target)); err != nil {
			return err
		}
	}
	return nil
}

// RemoveDroppedVectorDimensions clears the dropped vectors' dimension rows on
// one shard and drops its saved usage record.
//
// It runs per unit, before that unit is recorded complete, so a failure fails
// the unit and the next round retries it. Group completion would be too late:
// a completed unit stays credited even when its round fails, so the next round
// would skip the shard and the rows could outlive the drop.
func (db *DB) RemoveDroppedVectorDimensions(ctx context.Context, collection, shardName string, targets []string) error {
	idx := db.GetIndex(entschema.ClassName(collection))
	if idx == nil {
		return fmt.Errorf("index for collection %q not found", collection)
	}
	for _, target := range targets {
		if err := removeDimensionsForDroppedVector(ctx, idx, shardName, target); err != nil {
			return err
		}
	}
	return nil
}

const (
	// dimensionsClaimAttempts bounds the wait for a bucket claim held by a shard
	// that is loading or shutting down. Exhausting it fails the unit, which the
	// next round retries.
	dimensionsClaimAttempts = 4
	dimensionsClaimBackoff  = 250 * time.Millisecond
)

// removeDimensionsForDroppedVector clears target's dimension rows on one shard.
// The route has to branch: a loaded shard holds the bucket open through its own
// store, an unloaded one is opened from disk.
//
// Either end of that decision can lose the bucket's registry claim to the
// other. A shard that finishes loading takes it, and a shard that is shutting
// down holds it until its teardown completes. TryAdd reports both at once
// rather than waiting, so the retries are spaced: an immediate one re-reads the
// same state.
func removeDimensionsForDroppedVector(ctx context.Context, idx *Index, shardName, target string) error {
	var err error
	for attempt := range dimensionsClaimAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * dimensionsClaimBackoff):
			}
		}
		if err = removeDimensionsOnShard(ctx, idx, shardName, target); !errors.Is(err, lsmkv.ErrBucketAlreadyRegistered) {
			break
		}
	}
	if err != nil {
		return err
	}
	return invalidateComputedUsage(idx, shardName)
}

// invalidateComputedUsage drops a cold shard's saved usage record, which is
// keyed only by a hash of the active vector configs. Dropping a vector and
// re-creating it with the same config produces the same hash, so a record
// written before the drop is served again afterwards — reporting the old
// vector's count against a vector that holds nothing. Nothing else invalidates
// it: NewShard is the only other caller, and re-creating a vector does not load
// a cold shard.
func invalidateComputedUsage(idx *Index, shardName string) error {
	if err := shardusage.RemoveComputedUsageDataForUnloadedShard(idx.path(), shardName); err != nil {
		return fmt.Errorf("invalidate computed usage for shard %q: %w", shardName, err)
	}
	return nil
}

func removeDimensionsOnShard(ctx context.Context, idx *Index, shardName, target string) error {
	switch shard := idx.shards.Load(shardName).(type) {
	case *Shard:
		release, err := shard.preventShutdown()
		if err != nil {
			// Tearing down: the store is on its way out, so take the disk route
			// rather than write through a handle that is about to disappear.
			break
		}
		defer release()
		if err := shard.removeAllDimensionsLSM(ctx, target); !errors.Is(err, errAlreadyShutdown) {
			return err
		}
	case *LazyLoadShard:
		return removeDimensionsOnLazyShard(ctx, idx, shardName, target, shard)
	}

	return shardusage.RemoveUnloadedTargetVectorDimensions(ctx, idx.logger,
		idx.path(), shardName, target)
}

// removeDimensionsOnLazyShard commits to the answer it reads under l.mutex. A
// loaded shard's reference is taken before the lock is released, so a
// deactivation cannot blank the store in between — Store.Shutdown clears
// bucketsByName before draining, and a nil bucket would read as nothing to do.
// The cold branch keeps the lock, because loadIfCold holds it across the whole
// of NewShard: released early, this would open a bucket that store is opening
// at the same moment and one of the two would lose the registry claim.
func removeDimensionsOnLazyShard(ctx context.Context, idx *Index,
	shardName, target string, l *LazyLoadShard,
) error {
	l.mutex.Lock()
	if l.loaded && l.shard != nil {
		if release, err := l.shard.preventShutdown(); err == nil {
			inner := l.shard
			l.mutex.Unlock()
			defer release()
			if err := inner.removeAllDimensionsLSM(ctx, target); !errors.Is(err, errAlreadyShutdown) {
				return err
			}
			return shardusage.RemoveUnloadedTargetVectorDimensions(ctx, idx.logger,
				idx.path(), shardName, target)
		}
	}
	defer l.mutex.Unlock()
	return shardusage.RemoveUnloadedTargetVectorDimensions(ctx, idx.logger,
		idx.path(), shardName, target)
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
		// Filter ON THE COPY: rebuilding from orig's entries would carry orig's
		// interface fields — which alias the live FSM class — back into next,
		// defeating the deep copy (the update path mutates through them).
		changed := false
		for name, cfg := range next.VectorConfig {
			// Exact-case match (target vector names are case-sensitive identifiers:
			// a case-differing sibling is a DIFFERENT vector whose marker must stay);
			// only remove an entry still marked dropped (keep a live re-creation).
			if slices.Contains(targets, name) && modelsext.IsVectorIndexDropped(cfg) {
				delete(next.VectorConfig, name)
				changed = true
			}
		}
		if !changed {
			return nil // idempotent: entries already gone
		}
		// Removing the LAST entry lands on the vector-less collection shape;
		// the FSM keeps the legacy fields genuinely empty — nothing to set
		// here.

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

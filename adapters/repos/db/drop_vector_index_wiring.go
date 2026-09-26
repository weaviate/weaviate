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
		// asLazyLoadShard: a recovering shard's blocked Load warns+skips instead of panicking in Store() below.
		if lazy, ok := asLazyLoadShard(s); ok {
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
		if lazy, ok := asLazyLoadShard(s); ok {
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
	// Loads and unloads hold this lock while they update the map and shut the
	// shard down. Under it, a shard in the map stays loaded, and one missing
	// from the map has already closed index.db, so the offline path is safe.
	idx.shardCreateLocks.RLock(shardName)
	defer idx.shardCreateLocks.RUnlock(shardName)

	// A loaded shard retries its own drop: idempotent, it finishes a drop that
	// failed part-way, and it never opens index.db against its own lock.
	if loaded := idx.shards.loaded(shardName); loaded != nil {
		return loaded.retryDroppedVectorIndexes(targets)
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

// errDimensionsShardNotLoaded reports a unit whose shard left memory between
// its drain and its clear.
var errDimensionsShardNotLoaded = errors.New("shard is no longer loaded on this node")

// removeDimensionsForDroppedVector clears target's rows through the loaded
// shard, and fails when the shard is not loaded rather than opening its bucket
// from disk. A clear from disk holds the bucket's registry claim for O(objects)
// under no shard lock, so an activation, backup or delete of the same tenant
// collides with it. Failing costs nothing: the unit is not credited, a later
// round re-covers the tenant once it loads, and the load clears the rows itself
// while the drop is still marked.
func removeDimensionsForDroppedVector(ctx context.Context, idx *Index, shardName, target string) error {
	shard, release, err := loadedShardForDimensionsClear(idx, shardName)
	if err != nil {
		return err
	}
	defer release()
	if err := shard.removeAllDimensionsLSM(ctx, target); err != nil {
		return err
	}
	return invalidateComputedUsage(idx, shardName)
}

// loadedShardForDimensionsClear returns the loaded shard with a reference held,
// so it cannot be torn down mid-clear. It never loads a lazy shard. The locks
// are held for the lookup only: the clear is O(objects), and a delete of the
// tenant and every request routed to it would queue behind them.
func loadedShardForDimensionsClear(idx *Index, shardName string) (*Shard, func(), error) {
	notLoaded := fmt.Errorf("clear dimensions on %q: %w", shardName, errDimensionsShardNotLoaded)

	// The locks getLoadedShard takes, for its reason: every teardown holds one
	// of them, so none can land between the lookup and the reference.
	idx.closeLock.RLock()
	defer idx.closeLock.RUnlock()
	if idx.closed {
		return nil, nil, notLoaded
	}
	idx.shardCreateLocks.RLock(shardName)
	defer idx.shardCreateLocks.RUnlock(shardName)

	var shard *Shard
	switch s := idx.shards.Load(shardName).(type) {
	case *Shard:
		shard = s
	case *LazyLoadShard:
		s.mutex.Lock()
		if s.loaded {
			shard = s.shard
		}
		s.mutex.Unlock()
	}
	if shard == nil {
		return nil, nil, notLoaded
	}
	release, err := shard.preventShutdown()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", notLoaded, err)
	}
	return shard, release, nil
}

// invalidateComputedUsage drops a shard's saved usage record, which is keyed
// only by a hash of the active vector configs. Dropping a vector and
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

// retryDroppedVectorIndexes re-runs the shard's drop for each target, pinned
// against shutdown. A shard shutting down is an error: the ack fails, and the
// next load or the replayed callback sweeps files and record together.
func (s *Shard) retryDroppedVectorIndexes(targets []string) error {
	release, err := s.preventShutdown()
	if err != nil {
		return fmt.Errorf("sweep dropped vectors of shard %q: %w", s.ID(), err)
	}
	defer release()
	for _, target := range targets {
		err := s.DropVectorIndex(context.Background(), target)
		if err != nil {
			return fmt.Errorf("retry drop of vector %q on loaded shard: %w", target, err)
		}
	}
	return nil
}

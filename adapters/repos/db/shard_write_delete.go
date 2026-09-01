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
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

// Phases of the crash-safe delete sequence, reported to the test-only
// phase hook so tests can assert invariants at every intermediate state.
const (
	deletePhasePrepared   = "prepared"
	deletePhaseCleanedUp  = "cleaned-up"
	deletePhaseBarrier    = "barrier"
	deletePhaseRowDeleted = "row-deleted"
)

func (s *Shard) fireDeletePhaseHook(phase string) {
	if h := s.testDeletePhaseHook; h != nil {
		h(phase)
	}
}

// touchedBuckets collects the names of the LSM buckets an inverted-index
// cleanup wrote to, so the delete barrier can sync exactly those WALs. All
// methods are nil-receiver safe: write paths that don't need the barrier
// (e.g. the put path's old-posting cleanup) pass nil.
type touchedBuckets struct {
	names map[string]struct{}
	// all marks that an opaque write path (migration double-write callbacks)
	// may have written to buckets we cannot enumerate; the barrier must then
	// conservatively cover every bucket in the store.
	all bool
}

func newTouchedBuckets() *touchedBuckets {
	return &touchedBuckets{names: map[string]struct{}{}}
}

func (tb *touchedBuckets) add(name string) {
	if tb == nil {
		return
	}
	tb.names[name] = struct{}{}
}

func (tb *touchedBuckets) markAll() {
	if tb == nil {
		return
	}
	tb.all = true
}

// merge folds other into tb (used to build a batch-wide union).
func (tb *touchedBuckets) merge(other *touchedBuckets) {
	if tb == nil || other == nil {
		return
	}
	tb.all = tb.all || other.all
	for name := range other.names {
		tb.names[name] = struct{}{}
	}
}

// list returns the collected names, sorted for deterministic sync order.
func (tb *touchedBuckets) list() []string {
	if tb == nil {
		return nil
	}
	names := make([]string, 0, len(tb.names))
	for name := range tb.names {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// invertedDeleteBarrier makes a delete's inverted-index removals durable
// before the object row delete may become durable. With docID reuse ON an
// orphaned posting (docID in a posting, no object row) would later resolve to
// a DIFFERENT object, so the removals are fsynced via SyncWALs on exactly the
// touched buckets. With the flag OFF we keep today's cheap page-cache flush
// (WriteWALs), which preserves the write ordering without paying an fsync per
// delete. Note the objects bucket is deliberately NOT part of the barrier:
// the row is deleted after it.
func (s *Shard) invertedDeleteBarrier(ctx context.Context, touched *touchedBuckets) error {
	if !docIDReuseEnabled() {
		return s.store.WriteWALs()
	}
	if touched != nil && touched.all {
		return s.store.SyncAllWALs(ctx)
	}
	return s.store.SyncWALs(ctx, touched.list()...)
}

// deletePreparation captures everything read from the object row that the
// later delete phases need, so they never have to re-read it.
type deletePreparation struct {
	idBytes    []byte
	existing   []byte
	docID      uint64
	updateTime int64
}

func (s *Shard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	_, err := s.deleteObject(ctx, id, deletionTime, fromChangeLogReplay(ctx))
	return err
}

// deleteObject tombstones the object (reporting whether it did); with skipIfLocalNewer it keeps a live copy at least as new as deletionTime, since lsmkv does not timestamp-arbitrate and an older repair tombstone would otherwise clobber a newer write.
//
// Crash-safety: the phases run in an order that guarantees no crash can leave
// an ORPHAN POSTING (docID present in an inverted posting while the object
// row is gone) — see deleteObjectCrashSafeLocked. A crash at any point leaves
// either both row and postings, or row-without-postings; both converge on
// retrying the delete, whereas an orphan posting is unrepairable (the row's
// bytes are the only source of which postings to remove) and, once docIDs are
// reused, resolves to a different object.
func (s *Shard) deleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time,
	skipIfLocalNewer bool,
) (bool, error) {
	if err := s.isReadOnly(); err != nil {
		return false, err
	}

	// Wait for hashtree initialization before acquiring the RLock.
	// See shard_write_put.go for the deadlock explanation.
	if err := s.waitForMinimalHashTreeInitialization(ctx); err != nil {
		return false, err
	}

	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return false, err
	}

	bucket, err := s.objectsBucket()
	if err != nil {
		return false, err
	}

	// see comment in shard_write_put.go::putObjectLSM
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]

	lock.Lock()
	defer lock.Unlock()

	docID, deleted, err := s.deleteObjectCrashSafeLocked(ctx, bucket, idBytes, deletionTime, skipIfLocalNewer)
	if err != nil || !deleted {
		return false, err
	}

	if err = s.store.WriteWALs(); err != nil {
		return false, fmt.Errorf("flush all buffered WALs: %w", err)
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from vector index of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Delete(docID); err != nil {
			return fmt.Errorf("delete from geo index queue of prop %q: %w", propName, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush all vector index buffered WALs of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			return fmt.Errorf("flush geo index queue WALs of prop %q: %w", propName, err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

// deleteObjectCrashSafeLocked runs the LSM part of an object delete in the
// crash-safe order. The caller must hold the object's docIdLock. Reports the
// deleted object's docID and whether a live row was actually deleted.
//
// Order (each step only after the previous one):
//
//  1. prepare: read the row and evaluate guards.
//  2. cleanup: remove the docID from every inverted posting the row's
//     properties point to.
//  3. barrier: make those removals durable (invertedDeleteBarrier) BEFORE the
//     row delete below can possibly become durable.
//  4. row delete (+docID secondary key), change-log append, hashtree update.
//
// A crash before 4 leaves the row in place; retrying the delete re-runs the
// (idempotent) cleanup and converges. The reverse order could leave a posting
// pointing at a row that no longer exists, which a retry cannot repair.
func (s *Shard) deleteObjectCrashSafeLocked(ctx context.Context, bucket *lsmkv.Bucket,
	idBytes []byte, deletionTime time.Time, skipIfLocalNewer bool,
) (uint64, bool, error) {
	prep, err := s.prepareObjectDeletionLocked(bucket, idBytes, deletionTime, skipIfLocalNewer)
	if err != nil || prep == nil {
		return 0, false, err
	}
	s.fireDeletePhaseHook(deletePhasePrepared)

	touched, err := s.cleanupInvertedIndexOnDelete(prep.existing, prep.docID)
	if err != nil {
		return 0, false, fmt.Errorf("delete inverted postings of object: %w", err)
	}
	s.fireDeletePhaseHook(deletePhaseCleanedUp)

	if err := s.invertedDeleteBarrier(ctx, touched); err != nil {
		return 0, false, fmt.Errorf("inverted delete barrier: %w", err)
	}
	s.fireDeletePhaseHook(deletePhaseBarrier)

	if err := s.deleteObjectRowLocked(bucket, prep, deletionTime); err != nil {
		return 0, false, err
	}
	s.fireDeletePhaseHook(deletePhaseRowDeleted)

	return prep.docID, true, nil
}

// prepareObjectDeletionLocked reads the object row and evaluates the delete
// guards. Returns (nil, nil) when there is nothing to do (no row, or the
// local row is newer than an incoming repair tombstone). Caller must hold the
// object's docIdLock.
func (s *Shard) prepareObjectDeletionLocked(bucket *lsmkv.Bucket, idBytes []byte,
	deletionTime time.Time, skipIfLocalNewer bool,
) (*deletePreparation, error) {
	existing, err := bucket.Get(idBytes)
	if err != nil {
		return nil, fmt.Errorf("unexpected error on previous lookup: %w", err)
	}

	if existing == nil {
		// nothing to do
		return nil, nil
	}

	// we need the doc ID so we can clean up inverted indices currently
	// pointing to this object
	docID, updateTime, err := storobj.DocIDAndTimeFromBinary(existing)
	if err != nil {
		return nil, fmt.Errorf("get existing doc id from object binary: %w", err)
	}

	if skipIfLocalNewer && !deletionTime.IsZero() && updateTime >= deletionTime.UnixMilli() {
		return nil, nil // live local object is newer; keep it (TimeBased)
	}

	return &deletePreparation{
		idBytes:    idBytes,
		existing:   existing,
		docID:      docID,
		updateTime: updateTime,
	}, nil
}

// deleteObjectRowLocked deletes the object row (with its docID secondary
// key), appends the change-log delete and updates the hashtree. It must only
// run AFTER the inverted cleanup and its barrier. Caller must hold the
// object's docIdLock.
func (s *Shard) deleteObjectRowLocked(bucket *lsmkv.Bucket, prep *deletePreparation,
	deletionTime time.Time,
) error {
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, prep.docID)
	withSecondary := lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)

	var err error
	if deletionTime.IsZero() {
		err = bucket.Delete(prep.idBytes, withSecondary)
	} else {
		err = bucket.DeleteWith(prep.idBytes, deletionTime, withSecondary)
	}
	if err != nil {
		return fmt.Errorf("delete object from bucket: %w", err)
	}

	// Never time.Now() — the target's LWW replay compares this against its
	// local object's updateTime.
	logTime := prep.updateTime
	if !deletionTime.IsZero() {
		logTime = deletionTime.UnixMilli()
	}
	s.AppendChangeLogDelete(prep.idBytes, logTime)

	if err = s.mayDeleteObjectHashTree(prep.idBytes, prep.updateTime, logTime); err != nil {
		return fmt.Errorf("object deletion in hashtree: %w", err)
	}

	return nil
}

// cleanupInvertedIndexOnDelete removes docID from every inverted structure
// the row's properties point to. It reports the names of the buckets it wrote
// to so the caller can run a durability barrier over exactly those WALs.
func (s *Shard) cleanupInvertedIndexOnDelete(previous []byte, docID uint64) (*touchedBuckets, error) {
	bucket, err := s.objectsBucket()
	if err != nil {
		return nil, err
	}
	className, err := bucket.ClassName()
	if err != nil {
		return nil, fmt.Errorf("getting bucket class name: %w", err)
	}
	previousObject, err := storobj.FromBinaryDisk(previous, className)
	if err != nil {
		return nil, fmt.Errorf("unmarshal previous object: %w", err)
	}

	previousProps, previousNilProps, previousNestedProps, err := s.AnalyzeObject(previousObject)
	if err != nil {
		return nil, fmt.Errorf("analyze previous object: %w", err)
	}

	if err = s.subtractPropLengths(previousProps); err != nil {
		return nil, fmt.Errorf("subtract prop lengths: %w", err)
	}

	// Removing the old docId from the factory solves an issue,
	// where, if using a NotEquals filter on a property,
	// there is a possible time period where that docId has been deleted from the inverted index,
	// but is still present in HNSW or other vector indices.
	// For any NotEquals filter, we do an Equals filter and invert it's results.
	s.bitmapFactory.RemoveIds(docID)

	touched := newTouchedBuckets()

	st := s.loadPropValueIndexState()
	if len(st.del) > 0 || len(st.scope.props) > 0 {
		// Migration double-write callbacks resolve their target buckets
		// dynamically (sidecar or swap fallback), so we cannot enumerate
		// them here; be conservative.
		touched.markAll()
	}

	err = s.deleteFromInvertedIndicesLSM(previousProps, previousNilProps, docID, st, touched)
	if err != nil {
		return nil, fmt.Errorf("put inverted indices props: %w", err)
	}

	// Mirrors the delete into the ingest bucket for scope props suppressed
	// above; no-op absent a migration. (Bucket-wise covered by markAll.)
	if err = s.migrationDoubleWriteDelete(st, previousObject, docID); err != nil {
		return nil, fmt.Errorf("migration double-write delete: %w", err)
	}

	if err = s.deleteNestedInvertedIndicesLSM(previousNestedProps, docID, touched); err != nil {
		return nil, fmt.Errorf("delete nested inverted indices: %w", err)
	}

	if s.index.Config.TrackVectorDimensions {
		err = previousObject.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
			if err = s.removeDimensionsLSM(dims, docID, targetVector); err != nil {
				return fmt.Errorf("remove dimension tracking for vector %q: %w", targetVector, err)
			}
			touched.add(helpers.DimensionsBucketLSM)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return touched, nil
}

func (s *Shard) mayDeleteObjectHashTree(uuidBytes []byte, updateTime, deletionEventMs int64) error {
	if s.hashtree == nil {
		return nil
	}

	return s.deleteObjectHashTree(uuidBytes, updateTime, deletionEventMs)
}

func (s *Shard) deleteObjectHashTree(uuidBytes []byte, updateTime, deletionEventMs int64) error {
	if len(uuidBytes) != 16 {
		return fmt.Errorf("invalid object uuid")
	}

	if updateTime < 1 {
		return fmt.Errorf("invalid object update time")
	}

	leaf := s.hashtreeLeafFor(uuidBytes)

	var objectDigest [16 + 8]byte

	copy(objectDigest[:], uuidBytes)
	binary.BigEndian.PutUint64(objectDigest[16:], uint64(updateTime))

	// object deletion is treated as non-existent because the deletion time or
	// tombstone may not be available
	s.hashtree.AggregateLeafWith(leaf, objectDigest[:])

	// Fold a ≤cutoff delete out of the checkpoint. Require BOTH the event and the stored version
	// ≤cutoff: erasing a >cutoff version the clone never held would inject a phantom term.
	if cpht := s.asyncCheckpointHashtree; cpht != nil && deletionEventMs <= s.asyncCheckpointCutoff && updateTime <= s.asyncCheckpointCutoff {
		if err := cpht.AggregateLeafWith(leaf, objectDigest[:]); err != nil {
			return err
		}
	}

	return nil
}

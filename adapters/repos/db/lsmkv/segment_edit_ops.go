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

package lsmkv

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"
	"github.com/weaviate/weaviate/adapters/repos/db/transformers"
)

// SegmentEditOps is a bolt-backed sidecar that records in-place segment edit
// operations and tracks, per operation, which segments still need to be
// rewritten. Drop-vector-index is its first user: an operation names the target
// vectors to strip, and every segment present at registration time is recorded
// as "pending" until compaction or the cleanup driver has rewritten it.
//
// The store is deliberately decoupled from SegmentGroup: callers pass in the
// set of segment IDs (derived from segment file paths via segmentID) so the
// store can be unit-tested in isolation and reused for future edit ops.
//
// On-disk layout (one bolt file per segment group, alongside the segments):
//
//	operations/<opID>            -> OpDescriptor (JSON)
//	pending_segments/<opID>/<segID> -> pendingMeta (JSON)
//	quarantined/<opID>/<segID>      -> pendingMeta (JSON)
//
// pending_segments and quarantined use a nested sub-bucket per operation so the
// op and segment IDs never need an in-key separator.
type SegmentEditOps struct {
	dir string
	// className is the bucket's canonical class name, handed to each transformer
	// factory at build time (the global transformers registry can't capture per-bucket
	// state). Set once at construction.
	className string
	// resolve maps an op type to its transformer factory. It defaults to the global
	// transformers registry (transformers.Lookup); the edit-ops DB drives selection,
	// so the persisted ops — not the bucket wiring — decide what runs. Overridable
	// in tests to inject fakes for op types absent from the real registry.
	resolve transformerResolver

	// db is opened lazily: the bolt sidecar file is created only when the first
	// edit op is registered (see ensureOpen), so an idle objects bucket — the
	// common case, no drop ever issued — carries no sidecar. Read and bookkeeping
	// paths use openIfExists, which opens an already-present file but never creates
	// one, so the constantly-running compaction/cleanup cycles can't materialize it.
	// mu guards the one-time open and warnedMissingTransformer; once set, db is
	// stable until Close.
	mu sync.Mutex
	db *bolt.DB

	// logger is optional (nil disables logging); the segment group sets it after
	// construction. Used only to warn about ops with no registered transformer.
	logger logrus.FieldLogger
	// warnedMissingTransformer dedups the "no transformer for this op type" warning
	// to once per type, so the frequent compaction/cleanup passes can't spam it.
	warnedMissingTransformer map[OpType]struct{}
}

// transformerResolver maps an op type to its transformer factory, reporting
// whether one is registered. Production uses transformers.Lookup; tests inject.
type transformerResolver func(OpType) (OpTransformerFactory, bool)

const segmentEditOpsFileName = "segment_edit_ops.db.bolt"

var (
	editOpsBucketOperations = []byte("operations")
	editOpsBucketPending    = []byte("pending_segments")
	editOpsBucketQuarantine = []byte("quarantined")
)

// The op vocabulary lives in package editops so the transformers package can
// define factories against it without importing lsmkv (avoiding an import cycle).
// These aliases keep the existing lsmkv.X spellings valid for callers and tests.
type (
	OpType               = editops.OpType
	OpDescriptor         = editops.OpDescriptor
	ActiveOp             = editops.ActiveOp
	OpTransformerFactory = editops.OpTransformerFactory
)

// OpTypeRemoveTargetVectors strips dropped named vectors from stored objects.
const OpTypeRemoveTargetVectors = editops.OpTypeRemoveTargetVectors

// PendingSegment is one segment still awaiting rewrite for an operation, with
// its retry bookkeeping.
type PendingSegment struct {
	OpID          string `json:"-"`
	SegmentID     string `json:"-"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"lastError,omitempty"`
	LastAttemptAt int64  `json:"lastAttemptAt,omitempty"`
}

// valueTransformer rewrites a stored value in place during a segment rewrite.
// It must be a pure, idempotent function of the value bytes.
type valueTransformer func(value []byte) ([]byte, error)

// newSegmentEditOps constructs the edit-ops store for the segment group rooted at
// dir, resolving op types against the global transformers registry. This is the
// production constructor. It does NO I/O: the bolt sidecar file is opened (and
// created) lazily on the first registered op, so an objects bucket that never sees
// a drop carries no sidecar — keeping it out of file listings, backups and
// disk-size accounting. className is the canonical class name passed to each
// transformer factory.
func newSegmentEditOps(dir, className string) *SegmentEditOps {
	return newSegmentEditOpsWithLookup(dir, className, nil)
}

// newSegmentEditOpsWithLookup is newSegmentEditOps with an explicit op-type
// resolver. Tests use it to inject fakes (including op types absent from the real
// registry); a nil resolve falls back to the global transformers registry.
func newSegmentEditOpsWithLookup(dir, className string, resolve transformerResolver) *SegmentEditOps {
	if resolve == nil {
		resolve = transformers.Lookup
	}
	return &SegmentEditOps{
		dir:                      dir,
		className:                className,
		resolve:                  resolve,
		warnedMissingTransformer: map[OpType]struct{}{},
	}
}

// ensureOpen opens — creating the file if absent — the bolt sidecar and its
// buckets. Used by the write paths (RegisterOp/SnapshotSegments) so the sidecar
// materializes exactly when an edit op first exists.
func (s *SegmentEditOps) ensureOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return nil
	}
	return s.openLocked()
}

// openIfExists opens the bolt sidecar only when its file is already on disk, so
// read and bookkeeping paths (the constantly-running compaction/cleanup cycles,
// reconcile, completion bookkeeping) never create it on an idle shard. Returns
// false when there is nothing to open yet.
func (s *SegmentEditOps) openIfExists() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return true, nil
	}
	if _, err := os.Stat(filepath.Join(s.dir, segmentEditOpsFileName)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("stat segment edit ops db: %w", err)
	}
	if err := s.openLocked(); err != nil {
		return false, err
	}
	return true, nil
}

// openLocked performs the actual bolt open + bucket init. Caller must hold s.mu
// and have checked s.db == nil.
func (s *SegmentEditOps) openLocked() error {
	// One handle per segment group. The Timeout turns an accidental second open
	// into a fast error instead of a forever-hang; the single-open path is uncontended.
	db, err := bolt.Open(filepath.Join(s.dir, segmentEditOpsFileName), 0o600,
		&bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open segment edit ops db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{editOpsBucketOperations, editOpsBucketPending, editOpsBucketQuarantine} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return fmt.Errorf("init segment edit ops buckets: %w", err)
	}

	s.db = db
	return nil
}

func (s *SegmentEditOps) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// withWriteTx runs fn in a single write transaction. create true materializes the
// sidecar first (paths that establish an op); create false makes an absent sidecar
// a no-op (fn never runs). This is the one home for the "writes-may-create,
// reads-never-create" policy, and it guarantees s.db is non-nil before the tx so no
// caller can nil-deref the handle.
func (s *SegmentEditOps) withWriteTx(create bool, fn func(tx *bolt.Tx) error) error {
	if create {
		if err := s.ensureOpen(); err != nil {
			return err
		}
	} else {
		ok, err := s.openIfExists()
		if err != nil || !ok {
			return err
		}
	}
	return s.db.Update(fn)
}

// withReadTx runs fn in a single read transaction, or is a no-op (fn never runs,
// nil returned) when no sidecar exists yet — an idle bucket has nothing to read.
func (s *SegmentEditOps) withReadTx(fn func(tx *bolt.Tx) error) error {
	ok, err := s.openIfExists()
	if err != nil || !ok {
		return err
	}
	return s.db.View(fn)
}

// BuildCurrentTransformer composes the ops live right now into one value
// transformer for a single compaction or cleanup pass, plus the exact ops it was
// built from. The op types recorded in the sidecar drive selection: ops are
// grouped by type, each present type's registered factory builds a transformer
// over its ops, and the per-type transformers are chained (in first-seen
// CreatedAt order). An op whose type has no registered factory is skipped — a
// forward-compatible no-op. Building per pass keeps it in step with the live ops;
// the returned op set lets RecordCompaction decide what the pass stripped by
// membership. Transformer and set are both nil when nothing applies.
//
// One transformer is applied to every segment of a pass, by design: a dropped
// target must be removed everywhere, so over-applying is always correct (a
// segment created after the op can't carry the target — the write-path reject
// blocked it). Per-segment state lives in pending_segments, not the transformer.
func (s *SegmentEditOps) BuildCurrentTransformer() (valueTransformer, []ActiveOp, error) {
	if s.resolve == nil {
		return nil, nil, nil
	}
	ops, err := s.LoadOps()
	if err != nil {
		return nil, nil, fmt.Errorf("load edit ops: %w", err)
	}

	var order []OpType
	factories := map[OpType]OpTransformerFactory{}
	byType := map[OpType][]ActiveOp{}
	var applied, missing []ActiveOp
	for _, op := range ops {
		opType := op.Descriptor.Type
		if _, resolved := factories[opType]; !resolved {
			factory, ok := s.resolve(opType)
			if !ok {
				missing = append(missing, op)
				continue
			}
			factories[opType] = factory
			order = append(order, opType)
		}
		byType[opType] = append(byType[opType], op)
		applied = append(applied, op)
	}
	s.warnMissingTransformers(missing)
	if len(applied) == 0 {
		return nil, nil, nil
	}

	built := make([]valueTransformer, 0, len(order))
	for _, opType := range order {
		built = append(built, factories[opType](s.className, byType[opType]))
	}
	return chainTransformers(built), applied, nil
}

// warnMissingTransformers logs — once per op type per process — that the sidecar
// holds an op whose type has no registered transformer, so its pending segments
// will never be rewritten and the operation cannot complete. This is reached when
// either a new op type was persisted without adding its factory to the transformers
// registry, or a downgrade dropped support for a type still on disk. Skipping such
// an op is the safe behavior (we don't run a transform we don't understand), but it
// must be visible rather than silently stalling.
func (s *SegmentEditOps) warnMissingTransformers(missing []ActiveOp) {
	if s.logger == nil || len(missing) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range missing {
		if _, warned := s.warnedMissingTransformer[op.Descriptor.Type]; warned {
			continue
		}
		s.warnedMissingTransformer[op.Descriptor.Type] = struct{}{}
		s.logger.WithFields(logrus.Fields{
			"op_id":   op.ID,
			"op_type": op.Descriptor.Type,
		}).Warn("segment edit op has no registered transformer for its type; its pending " +
			"segments will not be rewritten and the operation cannot complete — add the " +
			"op type to the transformers registry (or this op is a leftover from a newer " +
			"version after a downgrade)")
	}
}

// chainTransformers threads the output of each transformer into the next, so
// multiple op types apply in sequence within a single segment rewrite. A lone
// transformer is returned unwrapped.
func chainTransformers(transformers []valueTransformer) valueTransformer {
	if len(transformers) == 1 {
		return transformers[0]
	}
	return func(value []byte) ([]byte, error) {
		var err error
		for _, transform := range transformers {
			if value, err = transform(value); err != nil {
				return nil, err
			}
		}
		return value, nil
	}
}

// RecordCompaction does the post-merge bookkeeping for leftID+rightID ->
// leftID_rightID in one bolt tx (the sequenced step after rename + in-memory
// swap). It marks the merged inputs done for every op, and re-queues the merged
// output for any op absent from builtOps (registered after the transformer was
// built, so not stripped) that had a pending input. Membership — not a timestamp
// — gates this, since the compactor clock and the leader-assigned CreatedAt differ.
//
// Crash window: if the process dies after switchOnDisk but before this commit,
// the merge inputs are gone from disk but the merged output never got a pending
// row for an op absent from builtOps. Reconcile only prunes missing-segment rows,
// so it can't recover this; the re-snapshot on shard load (recoverEditOps) re-queues
// every current segment for each live op, which covers the merged output.
func (s *SegmentEditOps) RecordCompaction(leftID, rightID string, builtOps []ActiveOp) error {
	mergedID := leftID + "_" + rightID

	built := make(map[string]struct{}, len(builtOps))
	for _, op := range builtOps {
		built[op.ID] = struct{}{}
	}

	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		ops, err := s.loadOpsTx(tx)
		if err != nil {
			return err
		}
		for _, op := range ops {
			leftWasPending := s.pendingContainsTx(tx, op.ID, leftID)
			rightWasPending := s.pendingContainsTx(tx, op.ID, rightID)

			if err := s.markSegmentDoneTx(tx, op.ID, leftID); err != nil {
				return err
			}
			if err := s.markSegmentDoneTx(tx, op.ID, rightID); err != nil {
				return err
			}

			if _, wasBuilt := built[op.ID]; !wasBuilt && (leftWasPending || rightWasPending) {
				if err := s.addPendingTx(tx, op.ID, mergedID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RegisterOp persists an operation descriptor. It is idempotent: re-registering
// an existing op keeps the original descriptor (notably its CreatedAt) so a
// retry does not reorder transformer application.
func (s *SegmentEditOps) RegisterOp(opID string, op OpDescriptor) error {
	return s.withWriteTx(true, func(tx *bolt.Tx) error {
		b := tx.Bucket(editOpsBucketOperations)
		if b.Get([]byte(opID)) != nil {
			return nil
		}
		enc, err := json.Marshal(op)
		if err != nil {
			return err
		}
		return b.Put([]byte(opID), enc)
	})
}

// RegisterOpWithSnapshot writes the op descriptor and the pending rows for segIDs
// in one transaction, so the descriptor is never durable without its snapshot (a
// resume would otherwise skip a drop that stripped nothing). Idempotent: an
// existing descriptor keeps its CreatedAt and already-pending segments are left
// untouched. Callers must derive segIDs under maintenanceLock (see
// SnapshotSegments' invariant) and hold it across this call.
func (s *SegmentEditOps) RegisterOpWithSnapshot(opID string, op OpDescriptor, segIDs []string) error {
	return s.withWriteTx(true, func(tx *bolt.Tx) error {
		ops := tx.Bucket(editOpsBucketOperations)
		if ops.Get([]byte(opID)) == nil {
			enc, err := json.Marshal(op)
			if err != nil {
				return err
			}
			if err := ops.Put([]byte(opID), enc); err != nil {
				return err
			}
		}
		sub, err := tx.Bucket(editOpsBucketPending).CreateBucketIfNotExists([]byte(opID))
		if err != nil {
			return err
		}
		for _, segID := range segIDs {
			if sub.Get([]byte(segID)) != nil {
				continue
			}
			enc, err := json.Marshal(PendingSegment{})
			if err != nil {
				return err
			}
			if err := sub.Put([]byte(segID), enc); err != nil {
				return err
			}
		}
		return nil
	})
}

// HasPendingSnapshot reports whether opID's segments have been snapshotted (its
// pending sub-bucket exists, even if now empty). Only a snapshot creates that
// sub-bucket, so this — not descriptor presence — is the correct "resume may skip
// the snapshot" signal.
func (s *SegmentEditOps) HasPendingSnapshot(opID string) (bool, error) {
	if ok, err := s.openIfExists(); err != nil || !ok {
		return false, err
	}
	exists := false
	if err := s.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket(editOpsBucketPending).Bucket([]byte(opID)) != nil
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

// LoadOps returns all active operations sorted by CreatedAt (ties broken by ID)
// so transformers are applied in a deterministic order.
func (s *SegmentEditOps) LoadOps() ([]ActiveOp, error) {
	var ops []ActiveOp
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		var err error
		ops, err = s.loadOpsTx(tx)
		return err
	}); err != nil {
		return nil, err
	}
	return ops, nil
}

// loadOpsTx is LoadOps within an existing transaction, used by the compaction
// completion bookkeeping which already holds a write tx.
func (s *SegmentEditOps) loadOpsTx(tx *bolt.Tx) ([]ActiveOp, error) {
	var ops []ActiveOp
	if err := tx.Bucket(editOpsBucketOperations).ForEach(func(k, v []byte) error {
		var desc OpDescriptor
		if err := json.Unmarshal(v, &desc); err != nil {
			return fmt.Errorf("decode op %q: %w", k, err)
		}
		ops = append(ops, ActiveOp{ID: string(k), Descriptor: desc})
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(ops, func(i, j int) bool {
		if ops[i].Descriptor.CreatedAt != ops[j].Descriptor.CreatedAt {
			return ops[i].Descriptor.CreatedAt < ops[j].Descriptor.CreatedAt
		}
		return ops[i].ID < ops[j].ID
	})
	return ops, nil
}

// SnapshotSegments records segIDs as pending for opID, which must already be
// registered. It is idempotent for segments that are still pending: an existing
// pending row (with its accrued retries) is left untouched, so re-running a
// snapshot after a crash neither duplicates rows nor resets progress.
//
// Progress is encoded as absence from the pending set, so callers must pass the
// segments currently on disk: re-snapshotting an ID that has already been
// completed (and whose segment was merged/cleaned away) re-queues it. Reconcile
// is the safety net — it drops pending rows for segments no longer on disk.
//
// INVARIANT (load-bearing for RecordCompaction's membership re-queue): pass the
// IDs of the in-memory segment list (SegmentGroup.segments) under maintenanceLock,
// never a raw directory listing. switchOnDisk deletes the merge inputs before
// renaming the .tmp output, so a directory snapshot in that window would record
// neither input nor output — silent partial data loss. The in-memory list is
// swapped atomically under the same lock, so a lock-held snapshot stays coherent.
func (s *SegmentEditOps) SnapshotSegments(opID string, segIDs []string) error {
	return s.withWriteTx(true, func(tx *bolt.Tx) error {
		if tx.Bucket(editOpsBucketOperations).Get([]byte(opID)) == nil {
			return fmt.Errorf("snapshot segments: operation %q is not registered", opID)
		}
		sub, err := tx.Bucket(editOpsBucketPending).CreateBucketIfNotExists([]byte(opID))
		if err != nil {
			return err
		}
		for _, segID := range segIDs {
			if sub.Get([]byte(segID)) != nil {
				continue
			}
			enc, err := json.Marshal(PendingSegment{})
			if err != nil {
				return err
			}
			if err := sub.Put([]byte(segID), enc); err != nil {
				return err
			}
		}
		return nil
	})
}

// Pending returns the segment IDs still awaiting rewrite for opID.
func (s *SegmentEditOps) Pending(opID string) ([]string, error) {
	var segIDs []string
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		sub := tx.Bucket(editOpsBucketPending).Bucket([]byte(opID))
		if sub == nil {
			return nil
		}
		return sub.ForEach(func(k, _ []byte) error {
			segIDs = append(segIDs, string(k))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return segIDs, nil
}

// AllPending returns every pending segment across all operations, the feed for
// the cleanup driver.
func (s *SegmentEditOps) AllPending() ([]PendingSegment, error) {
	var out []PendingSegment
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		return tx.Bucket(editOpsBucketPending).ForEachBucket(func(opID []byte) error {
			return tx.Bucket(editOpsBucketPending).Bucket(opID).ForEach(func(segID, v []byte) error {
				ps, err := decodePending(string(opID), string(segID), v)
				if err != nil {
					return err
				}
				out = append(out, ps)
				return nil
			})
		})
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// MarkSegmentDone removes a segment from the pending set for opID, signalling
// the rewrite for that (op, segment) pair is complete.
func (s *SegmentEditOps) MarkSegmentDone(opID, segID string) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		return s.markSegmentDoneTx(tx, opID, segID)
	})
}

func (s *SegmentEditOps) markSegmentDoneTx(tx *bolt.Tx, opID, segID string) error {
	sub := tx.Bucket(editOpsBucketPending).Bucket([]byte(opID))
	if sub == nil {
		return nil
	}
	return sub.Delete([]byte(segID))
}

// pendingContainsTx reports whether segID is currently pending for opID, read
// within the caller's transaction.
func (s *SegmentEditOps) pendingContainsTx(tx *bolt.Tx, opID, segID string) bool {
	sub := tx.Bucket(editOpsBucketPending).Bucket([]byte(opID))
	if sub == nil {
		return false
	}
	return sub.Get([]byte(segID)) != nil
}

// addPendingTx records segID as newly pending for opID within the caller's
// transaction. It is idempotent: an already-pending row (with its retry state)
// is left untouched.
func (s *SegmentEditOps) addPendingTx(tx *bolt.Tx, opID, segID string) error {
	sub, err := tx.Bucket(editOpsBucketPending).CreateBucketIfNotExists([]byte(opID))
	if err != nil {
		return err
	}
	if sub.Get([]byte(segID)) != nil {
		return nil
	}
	enc, err := json.Marshal(PendingSegment{})
	if err != nil {
		return err
	}
	return sub.Put([]byte(segID), enc)
}

// BumpAttempt records a failed rewrite attempt for a pending segment. The
// quarantine threshold decision lives in the cleanup driver; this only persists
// the count and last error.
func (s *SegmentEditOps) BumpAttempt(opID, segID string, opErr error) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		sub := tx.Bucket(editOpsBucketPending).Bucket([]byte(opID))
		if sub == nil {
			return nil
		}
		raw := sub.Get([]byte(segID))
		if raw == nil {
			// Already done or quarantined; do not resurrect a completed segment.
			return nil
		}
		ps, err := decodePending(opID, segID, raw)
		if err != nil {
			return err
		}
		ps.Attempts++
		if opErr != nil {
			ps.LastError = opErr.Error()
		}
		enc, err := json.Marshal(ps)
		if err != nil {
			return err
		}
		return sub.Put([]byte(segID), enc)
	})
}

// Quarantine moves a segment from pending to quarantined for opID, preserving
// its retry metadata. A quarantined segment fails the operation.
func (s *SegmentEditOps) Quarantine(opID, segID string) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		pendingSub := tx.Bucket(editOpsBucketPending).Bucket([]byte(opID))
		var raw []byte
		if pendingSub != nil {
			raw = pendingSub.Get([]byte(segID))
		}
		if raw == nil {
			// Nothing pending to quarantine; keep idempotent.
			return nil
		}
		quarantineSub, err := tx.Bucket(editOpsBucketQuarantine).CreateBucketIfNotExists([]byte(opID))
		if err != nil {
			return err
		}
		if err := quarantineSub.Put([]byte(segID), raw); err != nil {
			return err
		}
		return pendingSub.Delete([]byte(segID))
	})
}

// Quarantined returns the quarantined segments across all operations.
func (s *SegmentEditOps) Quarantined() ([]PendingSegment, error) {
	var out []PendingSegment
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		return tx.Bucket(editOpsBucketQuarantine).ForEachBucket(func(opID []byte) error {
			return tx.Bucket(editOpsBucketQuarantine).Bucket(opID).ForEach(func(segID, v []byte) error {
				ps, err := decodePending(string(opID), string(segID), v)
				if err != nil {
					return err
				}
				out = append(out, ps)
				return nil
			})
		})
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteOp removes an operation and all of its pending and quarantined rows.
// Called once the operation has fully completed (its DTM task is FINISHED).
func (s *SegmentEditOps) DeleteOp(opID string) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		if err := tx.Bucket(editOpsBucketOperations).Delete([]byte(opID)); err != nil {
			return err
		}
		if err := deleteSubBucket(tx.Bucket(editOpsBucketPending), opID); err != nil {
			return err
		}
		return deleteSubBucket(tx.Bucket(editOpsBucketQuarantine), opID)
	})
}

// Recover runs the load-time bookkeeping: sweep ops with no live task, prune rows
// for segments gone from disk (Reconcile), then re-snapshot every surviving op
// over segIDs. resolveLive is called only when ops exist (it may be a remote
// lookup); a nil result skips the sweep. The reload between Reconcile and the
// re-snapshot is load-bearing: the sweep mutates the op set, and re-snapshotting
// a swept op would resurrect it.
func (s *SegmentEditOps) Recover(segIDs []string, resolveLive func() map[string]struct{}) error {
	ops, err := s.LoadOps()
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}

	existing := make(map[string]struct{}, len(segIDs))
	for _, id := range segIDs {
		existing[id] = struct{}{}
	}
	liveOpIDs := resolveLive()
	if err := s.Reconcile(existing, liveOpIDs); err != nil {
		return err
	}
	if liveOpIDs != nil {
		if ops, err = s.LoadOps(); err != nil {
			return err
		}
	}
	for _, op := range ops {
		if err := s.SnapshotSegments(op.ID, segIDs); err != nil {
			return err
		}
	}
	return nil
}

// Reconcile repairs the store against ground truth at open time (C1):
//
//   - pending/quarantined rows for segments that no longer exist on disk are
//     dropped. This covers a crash after a segment was renamed/merged away but
//     before its row could be cleared.
//   - operations whose ID is not in liveOpIDs are dropped entirely (descriptor
//     plus rows), e.g. after a backup restore where the DTM task is gone.
//
// existingSegmentIDs and liveOpIDs are membership sets. A nil liveOpIDs skips
// the orphaned-op sweep (used when the live set is unknown).
func (s *SegmentEditOps) Reconcile(existingSegmentIDs, liveOpIDs map[string]struct{}) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		ops := tx.Bucket(editOpsBucketOperations)

		// Drop orphaned operations first; the segment sweep then skips them.
		if liveOpIDs != nil {
			var orphans []string
			if err := ops.ForEach(func(k, _ []byte) error {
				if _, ok := liveOpIDs[string(k)]; !ok {
					orphans = append(orphans, string(k))
				}
				return nil
			}); err != nil {
				return err
			}
			for _, opID := range orphans {
				if err := ops.Delete([]byte(opID)); err != nil {
					return err
				}
				if err := deleteSubBucket(tx.Bucket(editOpsBucketPending), opID); err != nil {
					return err
				}
				if err := deleteSubBucket(tx.Bucket(editOpsBucketQuarantine), opID); err != nil {
					return err
				}
			}
		}

		for _, top := range [][]byte{editOpsBucketPending, editOpsBucketQuarantine} {
			if err := pruneMissingSegments(tx.Bucket(top), existingSegmentIDs); err != nil {
				return err
			}
		}
		return nil
	})
}

// pruneMissingSegments deletes, across every operation sub-bucket, the segment
// rows whose ID is absent from existingSegmentIDs.
func pruneMissingSegments(parent *bolt.Bucket, existingSegmentIDs map[string]struct{}) error {
	type rowKey struct{ opID, segID string }
	var stale []rowKey
	if err := parent.ForEachBucket(func(opID []byte) error {
		return parent.Bucket(opID).ForEach(func(segID, _ []byte) error {
			if _, ok := existingSegmentIDs[string(segID)]; !ok {
				stale = append(stale, rowKey{opID: string(opID), segID: string(segID)})
			}
			return nil
		})
	}); err != nil {
		return err
	}
	for _, r := range stale {
		if sub := parent.Bucket([]byte(r.opID)); sub != nil {
			if err := sub.Delete([]byte(r.segID)); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteSubBucket(parent *bolt.Bucket, opID string) error {
	if parent.Bucket([]byte(opID)) == nil {
		return nil
	}
	return parent.DeleteBucket([]byte(opID))
}

func decodePending(opID, segID string, raw []byte) (PendingSegment, error) {
	ps := PendingSegment{OpID: opID, SegmentID: segID}
	if len(raw) == 0 {
		return ps, nil
	}
	if err := json.Unmarshal(raw, &ps); err != nil {
		return ps, fmt.Errorf("decode pending segment %s/%s: %w", opID, segID, err)
	}
	ps.OpID = opID
	ps.SegmentID = segID
	return ps, nil
}

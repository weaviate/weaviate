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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"
	"github.com/weaviate/weaviate/adapters/repos/db/transformers"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
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
	// lastOrphanSweep rate-limits SweepOrphans' cluster-level liveness lookup.
	// Guarded by mu.
	lastOrphanSweep time.Time
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
func (s *SegmentEditOps) ensureOpenLocked() error {
	if s.db != nil {
		return nil
	}
	return s.openLocked()
}

// openIfExists opens the bolt sidecar only when its file is already on disk, so
// read and bookkeeping paths (the constantly-running compaction/cleanup cycles,
// reconcile, completion bookkeeping) never create it on an idle shard. Returns
// false when there is nothing to open yet.
func (s *SegmentEditOps) openIfExistsLocked() (bool, error) {
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
		&bolt.Options{Timeout: entlsmkv.BoltFlockTimeout})
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
// It holds s.mu for the whole transaction: the sidecar file is removed once
// its last op is deleted (see DeleteOp), and a handle captured outside the
// lock could otherwise race that close+remove.
func (s *SegmentEditOps) withWriteTx(create bool, fn func(tx *bolt.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if create {
		if err := s.ensureOpenLocked(); err != nil {
			return err
		}
	} else {
		ok, err := s.openIfExistsLocked()
		if err != nil || !ok {
			return err
		}
	}
	return s.db.Update(fn)
}

// withReadTx runs fn in a single read transaction, or is a no-op (fn never runs,
// nil returned) when no sidecar exists yet — an idle bucket has nothing to read.
func (s *SegmentEditOps) withReadTx(fn func(tx *bolt.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.openIfExistsLocked()
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

// RecordCompaction does the post-merge bookkeeping for a leftID+rightID merge
// in one bolt tx (the sequenced step after rename + in-memory swap). It marks
// the merged inputs done for every op — quarantine rows included: the inputs
// no longer exist, and a stale quarantine row would fail every later round
// until a re-arm dropped it — and re-queues the merged output for any op
// absent from builtOps (registered after the transformer was built, so not
// stripped) that had a pending OR quarantined input. Membership — not a
// timestamp — gates this, since the compactor clock and the leader-assigned
// CreatedAt differ. A quarantined input counts because the merge rewrote its
// data into a NEW file the verdict knows nothing about; the merged output is
// covered as ordinary pending with a fresh retry budget. The output is
// renamed to the RIGHT input's ID (stripTmpExtension), so the re-queue uses
// rightID — a row under any other name would never match a live segment
// again: the drain would stall on it until a restart, whose load-time prune
// would then drop it and the drop would complete without stripping the merged
// output's data.
//
// Crash window: if the process dies after switchOnDisk but before this commit,
// the rows are untouched — the left row goes ENOENT (pruned at load) and the
// right row keeps naming the merged output, so the data stays covered; see
// SegmentEditOps.Recover.
func (s *SegmentEditOps) RecordCompaction(leftID, rightID string, builtOps []ActiveOp) error {
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
			inputWasPending := s.pendingContainsTx(tx, op.ID, leftID) ||
				s.pendingContainsTx(tx, op.ID, rightID)
			inputWasQuarantined := false
			if quarantined := tx.Bucket(editOpsBucketQuarantine).Bucket([]byte(op.ID)); quarantined != nil {
				inputWasQuarantined = quarantined.Get([]byte(leftID)) != nil ||
					quarantined.Get([]byte(rightID)) != nil
				for _, segID := range []string{leftID, rightID} {
					if err := quarantined.Delete([]byte(segID)); err != nil {
						return err
					}
				}
			}

			if err := s.markSegmentDoneTx(tx, op.ID, leftID); err != nil {
				return err
			}
			if err := s.markSegmentDoneTx(tx, op.ID, rightID); err != nil {
				return err
			}

			if _, wasBuilt := built[op.ID]; !wasBuilt && (inputWasPending || inputWasQuarantined) {
				if err := s.addPendingTx(tx, op.ID, rightID); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RegisterOp persists an operation descriptor WITHOUT a pending snapshot.
// Production uses RegisterOpWithSnapshot (descriptor + snapshot atomically); this
// primitive remains for tests that need the descriptor-only state (e.g. the
// interrupted-register resume path). Idempotent: re-registering keeps the
// original descriptor (notably its CreatedAt).
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
		return s.addPendingRowsTx(tx, opID, segIDs)
	})
}

// addPendingRowsTx inserts pending rows for segIDs within the caller's
// transaction, preserving existing rows (accrued retries) and skipping segments
// quarantined for the op — a quarantine verdict (retry budget exhausted) holds
// for the rest of the round, not ping-ponging back to pending; the NEXT round's
// re-arm grants a fresh budget (RequeueQuarantined).
func (s *SegmentEditOps) addPendingRowsTx(tx *bolt.Tx, opID string, segIDs []string) error {
	sub, err := tx.Bucket(editOpsBucketPending).CreateBucketIfNotExists([]byte(opID))
	if err != nil {
		return err
	}
	quarantined := tx.Bucket(editOpsBucketQuarantine).Bucket([]byte(opID))
	for _, segID := range segIDs {
		if sub.Get([]byte(segID)) != nil {
			continue
		}
		if quarantined != nil && quarantined.Get([]byte(segID)) != nil {
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
}

// PendForAllOps durably records segID as pending for EVERY registered op in
// one transaction. WAL recovery calls this BEFORE the flush deletes the WAL:
// the flush target's segment ID derives from the WAL name, so the cover can
// be made durable first — a crash after the WAL delete then leaves the row,
// never a clean-looking segment holding pre-arm bytes. Crash-loop safe:
// re-pending is idempotent (existing rows kept, quarantine honored), and a
// row whose flush never produced a segment is pruned by Reconcile.
func (s *SegmentEditOps) PendForAllOps(segID string) error {
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		ops, err := s.loadOpsTx(tx)
		if err != nil {
			return err
		}
		for _, op := range ops {
			if err := s.addPendingTx(tx, op.ID, segID); err != nil {
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
	exists := false
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		exists = tx.Bucket(editOpsBucketPending).Bucket([]byte(opID)) != nil
		return nil
	}); err != nil {
		return false, err
	}
	return exists, nil
}

// HasOps reports whether any op is registered, without decoding or sorting
// (the WAL-recovery probe only needs a boolean; Recover re-reads the full set
// moments later).
func (s *SegmentEditOps) HasOps() (bool, error) {
	has := false
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(editOpsBucketOperations).Cursor().First()
		has = k != nil
		return nil
	}); err != nil {
		return false, err
	}
	return has, nil
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
		return s.addPendingRowsTx(tx, opID, segIDs)
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
// transaction; see addPendingRowsTx for the idempotency/quarantine rules.
func (s *SegmentEditOps) addPendingTx(tx *bolt.Tx, opID, segID string) error {
	return s.addPendingRowsTx(tx, opID, []string{segID})
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
// its retry metadata. A quarantined segment fails the operation's current
// round; the next round's re-arm requeues it with a fresh retry budget
// (RequeueQuarantined).
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

// QuarantinedFor returns opID's quarantined segment IDs (scoped sub-bucket
// read, mirroring Pending).
func (s *SegmentEditOps) QuarantinedFor(opID string) ([]string, error) {
	var out []string
	if err := s.withReadTx(func(tx *bolt.Tx) error {
		sub := tx.Bucket(editOpsBucketQuarantine).Bucket([]byte(opID))
		if sub == nil {
			return nil
		}
		return sub.ForEach(func(segID, v []byte) error {
			out = append(out, string(segID))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// RequeueQuarantined clears opID's quarantine rows at the start of a new round:
// segments still in liveSegIDs go back to pending with a fresh retry budget,
// rows for segments no longer on disk are dropped (the compaction that removed
// them rewrote their data under the op's transformer, or re-queued the merged
// output). Without this, a quarantine verdict would outlive the round that
// exhausted the budget and wedge the drop permanently — the op survives a
// FAILED round as the resume point, and the pending snapshot short-circuits the
// re-arm, so nothing else can ever retry the segment. Within a round the
// verdict stands (addPendingRowsTx skips quarantined segments).
func (s *SegmentEditOps) RequeueQuarantined(opID string, liveSegIDs []string) error {
	live := make(map[string]struct{}, len(liveSegIDs))
	for _, id := range liveSegIDs {
		live[id] = struct{}{}
	}
	return s.withWriteTx(false, func(tx *bolt.Tx) error {
		sub := tx.Bucket(editOpsBucketQuarantine).Bucket([]byte(opID))
		if sub == nil {
			return nil
		}
		var segIDs []string
		if err := sub.ForEach(func(segID, _ []byte) error {
			segIDs = append(segIDs, string(segID))
			return nil
		}); err != nil {
			return err
		}
		if len(segIDs) == 0 {
			return nil
		}
		for _, segID := range segIDs {
			if err := sub.Delete([]byte(segID)); err != nil {
				return err
			}
			if _, ok := live[segID]; !ok {
				continue
			}
			// The quarantine row is gone (above), so addPendingRowsTx's
			// quarantine-skip cannot undo the requeue.
			if err := s.addPendingTx(tx, opID, segID); err != nil {
				return err
			}
		}
		return nil
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
// Called when this shard's work for the op is finished for good: task success
// (delivered as SWAPPING, or FINISHED on a replay), a terminal round whose
// unit here completed, or an orphan sweep. A terminal round's INCOMPLETE
// units keep their ops — the recorded pending sets are the resume points.
func (s *SegmentEditOps) DeleteOp(opID string) error {
	_, _, _, err := s.deleteOp(opID, false)
	return err
}

// DeleteOpIfDrained deletes opID only when it has no pending and no
// quarantined rows, verified and acted on in ONE transaction — separate
// read-then-delete calls would let a row land in between and be deleted with
// the op, dropping cover for unstripped data. Reports whether the op was
// deleted (an absent op counts as deleted: nothing left to disarm) and, when
// kept, the row counts that vetoed the delete.
func (s *SegmentEditOps) DeleteOpIfDrained(opID string) (deleted bool, pending, quarantined int, err error) {
	return s.deleteOp(opID, true)
}

// deleteOp removes an operation and its rows; with onlyIfDrained it first
// counts the op's pending and quarantined rows inside the same transaction
// and keeps everything when either is non-zero.
func (s *SegmentEditOps) deleteOp(opID string, onlyIfDrained bool) (deleted bool, pending, quarantined int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.openIfExistsLocked()
	if err != nil || !ok {
		return err == nil, 0, 0, err // no sidecar: nothing to disarm
	}
	empty := false
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if onlyIfDrained {
			pending = countSubRows(tx.Bucket(editOpsBucketPending), opID)
			quarantined = countSubRows(tx.Bucket(editOpsBucketQuarantine), opID)
			if pending > 0 || quarantined > 0 {
				return nil
			}
		}
		deleted = true
		if err := tx.Bucket(editOpsBucketOperations).Delete([]byte(opID)); err != nil {
			return err
		}
		if err := deleteSubBucket(tx.Bucket(editOpsBucketPending), opID); err != nil {
			return err
		}
		if err := deleteSubBucket(tx.Bucket(editOpsBucketQuarantine), opID); err != nil {
			return err
		}
		k, _ := tx.Bucket(editOpsBucketOperations).Cursor().First()
		empty = k == nil
		return nil
	}); err != nil {
		return false, 0, 0, err
	}
	if deleted && empty {
		// The last op is gone: remove the sidecar file entirely. Leaving it
		// costs a permanent fd + mmap on every shard that ever saw a drop
		// (openIfExists reopens it on every load and cleanup pass, forever).
		// s.mu is held across every reader/writer, so nothing can be mid-tx;
		// a later RegisterOp simply re-creates the file.
		s.closeAndRemoveLocked()
	}
	return deleted, pending, quarantined, nil
}

// closeAndRemoveLocked closes the bolt handle and deletes the sidecar file.
// Caller must hold s.mu. Best-effort: on failure the sidecar merely lingers,
// which is the pre-existing behavior.
func (s *SegmentEditOps) closeAndRemoveLocked() {
	err := s.db.Close()
	// Nil unconditionally: keeping a handle whose Close errored would wedge
	// the sidecar for the shard's remaining lifetime (every open-check treats
	// non-nil as usable). Re-opening after a failed close at worst trips the
	// bolt flock timeout, which is retryable.
	s.db = nil
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("close empty segment edit ops db (file kept): %v", err)
		}
		return
	}
	if err := os.Remove(filepath.Join(s.dir, segmentEditOpsFileName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		if s.logger != nil {
			s.logger.Warnf("remove empty segment edit ops db: %v", err)
		}
	}
}

// Recover runs the load-time bookkeeping: sweep ops with no live task, then
// prune pending rows for segments gone from disk (Reconcile). resolveLive is
// called only when ops exist (it may be a remote lookup); a nil result skips
// the sweep.
//
// The surviving pending sets are kept AS RECORDED — they are authoritative
// per-segment progress, which is what makes an interrupted strip resume
// instead of restarting. Absence from pending firmly means "clean", because
// segment identity survives every rewrite path:
//
//   - a cleanup rewrite keeps the segment's ID, and a compaction's output
//     takes the RIGHT input's ID, so a pending row keeps naming the file
//     that carries the data — including across the crash window between the
//     on-disk rename and the bolt commit (the left row goes ENOENT and its
//     content lives in the file the right row still names);
//   - the compaction-completion bolt tx maintains the rows transactionally,
//     and compaction and cleanup are serialized on the segment group's single
//     compact-or-cleanup goroutine — no two rewrites can race one row, which
//     is what lets a row's presence/absence be read as ground truth at all;
//   - segments created after the op was armed are clean by construction:
//     the arm flushed the memtable before its atomic register+snapshot
//     (RegisterOpWithSnapshot), and post-marker writes are stripped/rejected
//     by the write-path guards — the same guarantee shard-name-keyed
//     coverage inheritance already relies on.
//
// Segments born from WAL replay are no exception, but need nothing here:
// WAL recovery durably pends them for every op BEFORE the flush deletes the
// WAL (PendForAllOps) — a WAL can hold PRE-ARM bytes outside every snapshot
// (an older binary's b.flushing clobber orphaning a failed flush's memtable;
// since fixed, but its WALs survive an upgrade), and only a pend committed
// before the WAL delete survives every crash window.
//
// Quarantined segments stay quarantined (see addPendingRowsTx).
func (s *SegmentEditOps) Recover(segIDs []string, resolveLive, resolveLiveFresh func() map[string]struct{}) error {
	ops, err := s.LoadOps()
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		// A sidecar that exists with zero ops is a pure fd/mmap liability.
		s.removeSidecarIfEmpty()
		return nil
	}

	existing := make(map[string]struct{}, len(segIDs))
	for _, id := range segIDs {
		existing[id] = struct{}{}
	}
	liveOpIDs := resolveLive()
	if liveOpIDs != nil && len(suspectedOrphans(ops, liveOpIDs)) > 0 {
		// About to sweep: the (possibly cached) set may predate a suspect's task
		// commit. Re-resolve fresh; if that fails, skip the sweep this load rather
		// than destroy on stale evidence.
		liveOpIDs = resolveLiveFresh()
	}
	if liveOpIDs != nil {
		// Op types the liveness provider does not cover must survive the sweep
		// (see SetLivenessProvider); treat them as live.
		for _, op := range ops {
			if op.Descriptor.Type != OpTypeRemoveTargetVectors {
				liveOpIDs[op.ID] = struct{}{}
			}
		}
	}
	if err := s.Reconcile(existing, liveOpIDs); err != nil {
		return err
	}
	// Reconcile's orphan sweep deletes ops in its own transaction, bypassing
	// DeleteOp's remove-when-empty — re-check here so a load-time sweep of the
	// last op doesn't leave an empty sidecar open forever.
	s.removeSidecarIfEmpty()
	return nil
}

// removeSidecarIfEmpty deletes the sidecar file when no ops remain (see
// DeleteOp); best-effort, errors only mean the file lingers.
func (s *SegmentEditOps) removeSidecarIfEmpty() {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, err := s.openIfExistsLocked()
	if err != nil || !ok {
		return
	}
	empty := false
	if err := s.db.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(editOpsBucketOperations).Cursor().First()
		empty = k == nil
		return nil
	}); err != nil {
		return
	}
	if empty {
		s.closeAndRemoveLocked()
	}
}

// editOpsOrphanSweepInterval rate-limits the cleanup-cycle orphan sweep's
// cluster-level liveness lookup.
const editOpsOrphanSweepInterval = 5 * time.Minute

// SweepOrphans deletes ops whose task is no longer live. The load-time sweep
// (Recover) only helps on shard load; this cleanup-cycle variant disarms an
// orphan on a RUNNING node — e.g. a finalize whose local op-delete failed, or a
// replica whose sidecar was copied mid-drop — before a compaction could strip a
// re-created same-name vector. Rate-limited; a missing provider or lookup error
// skips the sweep (safe fallback, retried next window).
//
// A sweep that wrongly removes a LIVE op (e.g. a stale task list read during a
// cold-start log replay) self-heals: the active task's StartTask re-registers
// the op with a fresh snapshot; the cost is re-cleaning, not lost cleanup.
func (s *SegmentEditOps) SweepOrphans(ctx context.Context) {
	s.mu.Lock()
	if time.Since(s.lastOrphanSweep) < editOpsOrphanSweepInterval {
		s.mu.Unlock()
		return
	}
	s.lastOrphanSweep = time.Now()
	s.mu.Unlock()

	ops, err := s.LoadOps()
	if err != nil || len(ops) == 0 {
		return
	}
	live, err := editops.LiveOps(ctx)
	if err != nil || live == nil {
		return
	}
	suspected := suspectedOrphans(ops, live)
	if len(suspected) == 0 {
		return
	}
	// The cached set may predate a suspect's task commit (the op is registered
	// strictly after the commit) — deleting on it would kill a live op whose
	// draining unit then reads the empty pending set as "done" and the drop
	// finalizes without stripping. Confirm with a fresh read before destroying.
	live, err = editops.LiveOpsFresh(ctx)
	if err != nil || live == nil {
		return
	}
	for _, op := range suspectedOrphans(suspected, live) {
		s.warnIfSweepingRows(op.ID, "orphan sweep")
		if err := s.DeleteOp(op.ID); err != nil {
			if s.logger != nil {
				s.logger.WithField("op_id", op.ID).Warnf("edit-ops orphan sweep: delete failed: %v", err)
			}
			continue
		}
		if s.logger != nil {
			s.logger.WithField("op_id", op.ID).Info("edit-ops orphan sweep: removed op with no live task")
		}
	}
}

// warnIfSweepingRows surfaces a sweep deleting an op that still holds rows.
// A pending row is not proof of unstripped bytes — the routine journey
// "drained, finalized while unloaded, reactivation re-pends a WAL-recovered
// segment" leaves rows whose data the write-path guards already stripped —
// but it CAN be the residual window's evidence (pre-arm rows re-pended on a
// shard that unloaded before a cleanup pass and finalize both ran), and once
// the op is gone nothing can strip them (the dropped-target fence rightly
// blocks post-finalize strips). Deleting is still right — keeping the op
// could not strip either and would cost decode work forever — but it must
// not happen silently. A failed row read is reported as such, not as a count.
func (s *SegmentEditOps) warnIfSweepingRows(opID, where string) {
	if s.logger == nil {
		return
	}
	pending, perr := s.Pending(opID)
	quarantined, qerr := s.QuarantinedFor(opID)
	if perr != nil || qerr != nil {
		s.logger.WithField("op_id", opID).
			Warnf("edit-ops %s: could not inspect rows before deleting op (pending read: %v, quarantine read: %v)", where, perr, qerr)
		return
	}
	if len(pending)+len(quarantined) > 0 {
		s.logger.WithField("op_id", opID).Warn(sweepingRowsMessage(where, len(pending), len(quarantined)))
	}
}

// sweepingRowsMessage is the shared wording for both sweep sites (SweepOrphans
// and Recover's orphan sweep), so the disclosure cannot drift.
func sweepingRowsMessage(where string, pending, quarantined int) string {
	return fmt.Sprintf("edit-ops %s: deleting op that still holds %d pending / %d quarantined segment(s); "+
		"their dropped-vector bytes may not have been stripped (post-marker writes were stripped on write; "+
		"anything else would need a re-drop of the name to clean)", where, pending, quarantined)
}

// suspectedOrphans returns the sweepable ops (liveness-covered types only; an
// unknown future type fails safe until its producer extends the provider) that
// are absent from live.
func suspectedOrphans(ops []ActiveOp, live map[string]struct{}) []ActiveOp {
	var out []ActiveOp
	for _, op := range ops {
		if op.Descriptor.Type != OpTypeRemoveTargetVectors {
			continue
		}
		if _, ok := live[op.ID]; !ok {
			out = append(out, op)
		}
	}
	return out
}

// Reconcile repairs the store against ground truth at open time (C1):
//
//   - legacy "<left>_<right>" compaction re-queue rows written by an older
//     binary are migrated to the merged output's real ID first — plain
//     pruning would silently drop their cover (see
//     migrateLegacyCompactionRowsTx).
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

		// Drop orphaned operations first; the migration and segment sweep
		// then skip them.
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
				// Same disclosure as SweepOrphans (see warnIfSweepingRows).
				if s.logger != nil {
					pending := countSubRows(tx.Bucket(editOpsBucketPending), opID)
					quarantined := countSubRows(tx.Bucket(editOpsBucketQuarantine), opID)
					if pending+quarantined > 0 {
						s.logger.WithField("op_id", opID).
							Warn(sweepingRowsMessage("recover orphan sweep", pending, quarantined))
					}
				}
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

		if err := s.migrateLegacyCompactionRowsTx(tx, existingSegmentIDs); err != nil {
			return err
		}

		for _, top := range [][]byte{editOpsBucketPending, editOpsBucketQuarantine} {
			if err := pruneMissingSegments(tx.Bucket(top), existingSegmentIDs); err != nil {
				return err
			}
		}
		return nil
	})
}

// migrateLegacyCompactionRowsTx heals pending rows written by a pre-resume
// binary's RecordCompaction, which re-queued a compaction's merged output
// under "<leftID>_<rightID>" — a name no live segment ever carries (the merged
// file takes the RIGHT input's ID; stripTmpExtension). The old code masked
// those rows by re-snapshotting every live segment on load; with recovery now
// trusting the recorded pending set, plain pruning would drop the row and the
// merged output — written without the op's transformer — would be treated as
// clean: the drop would finalize with its data unstripped. So, for a pending
// row absent from disk whose name has the legacy shape: if its right half is
// a live segment, the row is rewritten to it (the exact cover); if the right
// half is gone too (merged again under the old binary), the op re-pends every
// live segment — one conservative re-clean of this shard instead of a silent
// under-strip. Quarantine rows need no migration: cleanup never bumps a row
// whose segment is missing, so a phantom can never have been quarantined.
// The downgrade direction is safe without any of this: an older binary
// re-snapshots every live segment at load, which re-covers anything a
// post-upgrade sidecar recorded.
func (s *SegmentEditOps) migrateLegacyCompactionRowsTx(tx *bolt.Tx, existingSegmentIDs map[string]struct{}) error {
	pending := tx.Bucket(editOpsBucketPending)
	type phantomRow struct {
		opID, segID, rightID string
	}
	var phantoms []phantomRow
	if err := pending.ForEachBucket(func(opID []byte) error {
		return pending.Bucket(opID).ForEach(func(segID, _ []byte) error {
			id := string(segID)
			if _, ok := existingSegmentIDs[id]; ok {
				return nil
			}
			i := strings.LastIndexByte(id, '_')
			if i < 0 {
				return nil // plain missing segment; pruneMissingSegments handles it
			}
			phantoms = append(phantoms, phantomRow{opID: string(opID), segID: id, rightID: id[i+1:]})
			return nil
		})
	}); err != nil {
		return err
	}
	if len(phantoms) == 0 {
		return nil
	}
	resnapshot := map[string]struct{}{}
	for _, row := range phantoms {
		if err := pending.Bucket([]byte(row.opID)).Delete([]byte(row.segID)); err != nil {
			return err
		}
		if s.logger != nil {
			s.logger.WithField("op_id", row.opID).WithField("row", row.segID).
				Warn("edit-ops recover: migrating legacy compaction re-queue row from an older binary")
		}
		if _, ok := existingSegmentIDs[row.rightID]; ok {
			// addPendingRowsTx keeps an existing row's accrued retries and
			// honors a standing quarantine verdict on the target ID.
			if err := s.addPendingTx(tx, row.opID, row.rightID); err != nil {
				return err
			}
			continue
		}
		resnapshot[row.opID] = struct{}{}
	}
	if len(resnapshot) == 0 {
		return nil
	}
	segIDs := make([]string, 0, len(existingSegmentIDs))
	for id := range existingSegmentIDs {
		segIDs = append(segIDs, id)
	}
	sort.Strings(segIDs)
	for opID := range resnapshot {
		if err := s.addPendingRowsTx(tx, opID, segIDs); err != nil {
			return err
		}
	}
	return nil
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

// countSubRows returns the number of rows in parent's opID sub-bucket (0 when
// absent).
func countSubRows(parent *bolt.Bucket, opID string) int {
	sub := parent.Bucket([]byte(opID))
	if sub == nil {
		return 0
	}
	n := 0
	_ = sub.ForEach(func(_, _ []byte) error { n++; return nil })
	return n
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

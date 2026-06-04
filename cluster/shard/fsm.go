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

package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/klauspost/compress/s2"
	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"google.golang.org/protobuf/proto"
)

const (
	// fsmRetryAttempts is the number of retry attempts for transient errors
	// in FSM.Apply() handlers. Combined with fsmRetryInterval, the maximum
	// added latency is fsmRetryAttempts * fsmRetryInterval (300ms), well
	// within RAFT's default 10s apply timeout.
	fsmRetryAttempts = 3

	// fsmRetryInterval is the constant backoff between retry attempts.
	fsmRetryInterval = 100 * time.Millisecond
)

// shard defines the operations that can be performed on a shard.
// This interface is implemented by the actual shard in adapters/repos/db.
type shard interface {
	// PutObject stores an object in the shard.
	PutObject(ctx context.Context, obj *storobj.Object) error

	// DeleteObject deletes an object from the shard by UUID.
	DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error

	// MergeObject applies a partial update to an existing object.
	MergeObject(ctx context.Context, merge objects.MergeDocument) error

	// PutObjectBatch stores multiple objects in the shard.
	PutObjectBatch(ctx context.Context, objects []*storobj.Object) []error

	// DeleteObjectBatch deletes multiple objects from the shard.
	DeleteObjectBatch(ctx context.Context, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects

	// AddReferencesBatch adds cross-references in batch.
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error

	// FlushMemtables flushes all in-memory memtables to disk segments.
	// Called before RAFT snapshots to ensure all applied entries are durable
	// in LSM segments before the RAFT log is truncated.
	FlushMemtables(ctx context.Context) error

	// CreateTransferSnapshot creates a hardlink snapshot of all shard files
	// for out-of-band state transfer. Returns snapshot metadata including the
	// staging directory path. Caller must call ReleaseTransferSnapshot when done.
	CreateTransferSnapshot(ctx context.Context) (TransferSnapshot, error)

	// ReleaseTransferSnapshot deletes the staging directory for a snapshot.
	ReleaseTransferSnapshot(snapshotID string) error

	// Name returns the shard name.
	Name() string
}

// TransferSnapshot holds metadata for a hardlink snapshot created for
// out-of-band state transfer.
type TransferSnapshot struct {
	ID    string             // unique snapshot identifier
	Dir   string             // staging directory path containing hardlinks
	Files []TransferFileInfo // file list with sizes and checksums
}

// TransferFileInfo describes a single file within a transfer snapshot.
type TransferFileInfo struct {
	Name  string // relative path within staging dir
	Size  int64
	CRC32 uint32
}

// StateTransferer handles downloading shard data from a leader when a
// follower needs a full state transfer (e.g. after falling too far behind).
type StateTransferer interface {
	TransferState(ctx context.Context, className, shardName string) error
}

// FSM is the per-shard command dispatcher. The Store's Ready loop hands it
// committed RAFT log entries via Dispatch; the FSM applies each command to the
// underlying shard. With etcd/raft the FSM is no longer a library interface —
// it is a plain dispatcher invoked single-threaded from the Ready loop.
type FSM struct {
	className string
	shardName string
	nodeID    string
	log       logrus.FieldLogger

	// shard is the actual shard implementation that processes commands.
	// It's set via SetShard after the FSM is created.
	shard shard
	mu    sync.RWMutex

	// stateTransferer handles downloading shard data from the leader when
	// Restore() detects a foreign snapshot. Set via SetStateTransferer.
	stateTransferer StateTransferer

	// lastAppliedIndex tracks the last RAFT log index that was applied.
	// This is used for catch-up detection and snapshot consistency.
	lastAppliedIndex atomic.Uint64

	// indexMu and indexCond are used by WaitForIndex to allow callers to
	// block until the FSM has applied up to a target log index.
	indexMu   sync.Mutex
	indexCond *sync.Cond
}

// NewFSM creates a new FSM for a shard's RAFT cluster.
func NewFSM(className, shardName, nodeID string, log logrus.FieldLogger) *FSM {
	f := &FSM{
		className: className,
		shardName: shardName,
		nodeID:    nodeID,
		log: log.WithFields(logrus.Fields{
			"component": "shard_raft_fsm",
			"class":     className,
			"shard":     shardName,
		}),
	}
	f.indexCond = sync.NewCond(&f.indexMu)
	return f
}

// SetShard sets the shard operator that will process commands.
// This must be called before the RAFT cluster starts processing logs.
func (f *FSM) SetShard(shard shard) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shard = shard
}

// getShard returns the current shard operator, or nil if not set.
func (f *FSM) getShard() shard {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shard
}

// SetStateTransferer sets the state transferrer used by Restore() to download
// shard data from the leader when a foreign snapshot is detected.
func (f *FSM) SetStateTransferer(st StateTransferer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateTransferer = st
}

// setApplied records the last applied RAFT log index and wakes WaitForIndex
// waiters. The Store calls it directly for entries that carry no command
// (empty leader entries, conf changes); Dispatch calls it for command entries.
func (f *FSM) setApplied(index uint64) {
	f.lastAppliedIndex.Store(index)
	f.indexCond.Broadcast()
}

// Dispatch applies one committed command entry to the shard. payload is the
// marshalled shardproto.ApplyRequest (the Store has already stripped the
// request-ID prefix); index is the entry's RAFT log index. It must be
// deterministic and is invoked single-threaded from the Store's Ready loop.
func (f *FSM) Dispatch(payload []byte, index uint64) Response {
	defer f.setApplied(index)

	f.mu.RLock()
	shard := f.shard
	f.mu.RUnlock()

	if shard == nil {
		f.log.Error("shard not set, cannot apply log entry")
		return Response{Version: index, Error: fmt.Errorf("shard not set")}
	}

	// Parse the command
	var req shardproto.ApplyRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		f.log.WithError(err).Error("failed to unmarshal command")

		return Response{Version: index, Error: fmt.Errorf("unmarshal command: %w", err)}
	}

	// Decompress sub_command if the entry was compressed by the replicator.
	// Uncompressed entries (Compressed=false) pass through unchanged, which
	// ensures backwards compatibility during rolling upgrades.
	if req.Compressed {
		decompressed, err := s2.Decode(nil, req.SubCommand)
		if err != nil {
			f.log.WithError(err).Error("failed to decompress sub_command")
			return Response{Version: index, Error: fmt.Errorf("decompress: %w", err)}
		}
		req.SubCommand = decompressed
	}

	// Dispatch based on command type
	var applyErr error
	switch req.Type {
	case shardproto.ApplyRequest_TYPE_PUT_OBJECT:
		applyErr = f.putObject(shard, &req)
	case shardproto.ApplyRequest_TYPE_DELETE_OBJECT:
		applyErr = f.deleteObject(shard, &req)
	case shardproto.ApplyRequest_TYPE_MERGE_OBJECT:
		applyErr = f.mergeObject(shard, &req)
	case shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH:
		applyErr = f.putObjectsBatch(shard, &req)
	case shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH:
		applyErr = f.deleteObjectsBatch(shard, &req)
	case shardproto.ApplyRequest_TYPE_ADD_REFERENCES:
		applyErr = f.addReferences(shard, &req)
	default:
		applyErr = fmt.Errorf("unknown command type: %v", req.Type)
		f.log.WithField("type", req.Type).Error("unknown command type")
	}

	if applyErr != nil {
		// This should not happen after the retry changes — all handlers now
		// swallow errors to maintain FSM consistency. Log as defense-in-depth.
		f.log.WithError(applyErr).WithField("index", index).
			Error("unexpected error from FSM handler (should have been swallowed)")
	}

	return Response{Version: index}
}

// isRetryableInFSM returns true if the error is a transient infrastructure
// error that may resolve on retry (memory pressure, disk I/O, etc).
// Non-retryable errors (bad data, unknown formats) return false.
func isRetryableInFSM(err error) bool {
	if err == nil {
		return false
	}
	if enterrors.IsTransient(err) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EIO, syscall.ENOSPC, syscall.EROFS:
			return true
		default:
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "no space left") ||
		strings.Contains(msg, "read-only file system") ||
		strings.Contains(msg, "input/output error")
}

// retryApplyOp retries a shard operation within FSM.Apply() if the error
// is classified as transient. Non-retryable errors are swallowed immediately.
// If retries exhaust, the error is logged and nil is returned to keep all
// nodes' state machines consistent. The async replication hashbeater will
// detect and repair the divergence.
func (f *FSM) retryApplyOp(opName string, op func() error) error {
	var lastErr error
	for attempt := 0; attempt <= fsmRetryAttempts; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if !isRetryableInFSM(err) {
			f.log.WithError(err).WithField("op", opName).
				Error("FSM apply: permanent error, swallowing to maintain consistency")
			return nil
		}
		lastErr = err
		if attempt < fsmRetryAttempts {
			f.log.WithError(err).WithFields(logrus.Fields{
				"op":      opName,
				"attempt": attempt + 1,
			}).Warn("FSM apply: transient error, retrying")
			time.Sleep(fsmRetryInterval)
		}
	}
	f.log.WithError(lastErr).WithFields(logrus.Fields{
		"op":       opName,
		"attempts": fsmRetryAttempts + 1,
	}).Error("FSM apply: retries exhausted, swallowing error to maintain consistency; async replication will repair")
	return nil
}

// putObject applies a PUT_OBJECT command to the shard.
func (f *FSM) putObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.PutObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in putObject, swallowing")
		return nil
	}

	obj, err := storobj.FromBinary(subreq.Object)
	if err != nil {
		f.log.WithError(err).Error("FSM apply: permanent deserialize error in putObject, swallowing")
		return nil
	}

	ctx := context.Background()
	return f.retryApplyOp("put_object", func() error {
		return shard.PutObject(ctx, obj)
	})
}

// deleteObject applies a DELETE_OBJECT command to the shard.
func (f *FSM) deleteObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.DeleteObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in deleteObject, swallowing")
		return nil
	}

	id := strfmt.UUID(subreq.Id)

	var deletionTime time.Time
	if subreq.DeletionTimeUnix != 0 {
		deletionTime = time.Unix(0, subreq.DeletionTimeUnix)
	}

	ctx := context.Background()
	return f.retryApplyOp("delete_object", func() error {
		return shard.DeleteObject(ctx, id, deletionTime)
	})
}

// mergeObject applies a MERGE_OBJECT command to the shard.
func (f *FSM) mergeObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.MergeObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in mergeObject, swallowing")
		return nil
	}

	var doc objects.MergeDocument
	if err := json.Unmarshal(subreq.MergeDocumentJson, &doc); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in mergeObject, swallowing")
		return nil
	}

	ctx := context.Background()
	return f.retryApplyOp("merge_object", func() error {
		return shard.MergeObject(ctx, doc)
	})
}

// putObjectsBatch applies a PUT_OBJECTS_BATCH command to the shard.
func (f *FSM) putObjectsBatch(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.PutObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in putObjectsBatch, swallowing")
		return nil
	}

	objs := make([]*storobj.Object, len(subreq.Objects))
	for i, raw := range subreq.Objects {
		obj, err := storobj.FromBinary(raw)
		if err != nil {
			f.log.WithError(err).WithField("index", i).Error("FSM apply: permanent deserialize error in putObjectsBatch, swallowing")
			return nil
		}
		objs[i] = obj
	}

	ctx := context.Background()
	errs := shard.PutObjectBatch(ctx, objs)
	for i, err := range errs {
		if err != nil {
			f.log.WithError(err).WithField("index", i).Warn("batch put: item failed")
		}
	}

	// Return nil even on per-item errors: the RAFT log entry is already
	// committed, and partial success is acceptable since replay is idempotent.
	// Failed items can be retried by the client.
	return nil
}

// deleteObjectsBatch applies a DELETE_OBJECTS_BATCH command to the shard.
func (f *FSM) deleteObjectsBatch(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.DeleteObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in deleteObjectsBatch, swallowing")
		return nil
	}

	uuids := make([]strfmt.UUID, len(subreq.Uuids))
	for i, id := range subreq.Uuids {
		uuids[i] = strfmt.UUID(id)
	}

	var deletionTime time.Time
	if subreq.DeletionTimeUnix != 0 {
		deletionTime = time.Unix(0, subreq.DeletionTimeUnix)
	}

	ctx := context.Background()
	results := shard.DeleteObjectBatch(ctx, uuids, deletionTime, subreq.DryRun)
	for i, r := range results {
		if r.Err != nil {
			f.log.WithError(r.Err).WithField("index", i).Warn("batch delete: item failed")
		}
	}

	// Return nil even on per-item errors: the RAFT log entry is already
	// committed, and partial success is acceptable since replay is idempotent.
	return nil
}

// addReferences applies an ADD_REFERENCES command to the shard.
func (f *FSM) addReferences(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.AddReferencesRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in addReferences, swallowing")
		return nil
	}

	var refs objects.BatchReferences
	if err := json.Unmarshal(subreq.ReferencesJson, &refs); err != nil {
		f.log.WithError(err).Error("FSM apply: permanent unmarshal error in addReferences, swallowing")
		return nil
	}

	ctx := context.Background()
	errs := shard.AddReferencesBatch(ctx, refs)
	for i, err := range errs {
		if err != nil {
			f.log.WithError(err).WithField("index", i).Warn("add references: item failed")
		}
	}

	// Return nil even on per-item errors: the RAFT log entry is already
	// committed, and partial success is acceptable since replay is idempotent.
	return nil
}

// SnapshotMetadata returns a snapshot of the FSM's current identity and applied
// index. The Store uses it to build a SnapshotRequest for the Snapshotter pool.
func (f *FSM) SnapshotMetadata() shardSnapshotData {
	return shardSnapshotData{
		ClassName:        f.className,
		ShardName:        f.shardName,
		NodeID:           f.nodeID,
		LastAppliedIndex: f.lastAppliedIndex.Load(),
	}
}

// RestoreFromSnapshot restores FSM state from a RAFT snapshot's metadata. The
// Store's Ready loop calls it when etcd/raft delivers a non-empty rd.Snapshot.
// If the snapshot was created by a different node (foreign snapshot), it
// triggers an out-of-band state transfer to download shard data from the
// current leader. The StateTransferer determines the leader dynamically — we
// don't use meta.NodeID for leader determination since the leader may have
// changed between snapshot creation and restore.
func (f *FSM) RestoreFromSnapshot(meta shardSnapshotData) error {
	// Verify snapshot is for the correct shard
	if meta.ClassName != f.className || meta.ShardName != f.shardName {
		return fmt.Errorf("snapshot class/shard mismatch: expected %s/%s, got %s/%s",
			f.className, f.shardName, meta.ClassName, meta.ShardName)
	}

	// Trigger state transfer if this is a foreign snapshot (from another node).
	f.mu.RLock()
	st := f.stateTransferer
	f.mu.RUnlock()

	if meta.NodeID != f.nodeID && st != nil {
		f.log.WithFields(logrus.Fields{
			"snapshot_node_id": meta.NodeID,
			"local_node_id":    f.nodeID,
		}).Info("foreign snapshot detected, initiating state transfer")

		ctx := context.Background()
		if err := st.TransferState(ctx, f.className, f.shardName); err != nil {
			return fmt.Errorf("state transfer for %s/%s: %w", f.className, f.shardName, err)
		}
	}

	f.setApplied(meta.LastAppliedIndex)
	f.log.WithField("lastAppliedIndex", meta.LastAppliedIndex).Info("restored from snapshot")

	return nil
}

// LastAppliedIndex returns the last RAFT log index that was applied.
func (f *FSM) LastAppliedIndex() uint64 {
	return f.lastAppliedIndex.Load()
}

// WaitForIndex blocks until the FSM has applied at least targetIndex, or the
// context is cancelled. This is used by followers to ensure their local state
// has caught up to the leader before performing a local read.
func (f *FSM) WaitForIndex(ctx context.Context, targetIndex uint64) error {
	// Fast path: already caught up.
	if f.lastAppliedIndex.Load() >= targetIndex {
		return nil
	}

	// Spawn a goroutine that broadcasts on context cancellation so that
	// the cond.Wait loop can observe ctx.Done.
	stopWaker := make(chan struct{})
	defer close(stopWaker)
	enterrors.GoWrapper(func() {
		select {
		case <-ctx.Done():
			f.indexCond.Broadcast()
		case <-stopWaker:
		}
	}, f.log)

	if err := func() error {
		f.indexMu.Lock()
		defer f.indexMu.Unlock()
		for f.lastAppliedIndex.Load() < targetIndex {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.indexCond.Wait()
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

// shardSnapshotData is the JSON-serializable snapshot data structure. It is
// the payload of raftpb.Snapshot.Data and the content of each .snap file the
// Snapshotter writes.
type shardSnapshotData struct {
	ClassName        string `json:"class_name"`
	ShardName        string `json:"shard_name"`
	NodeID           string `json:"node_id"`
	LastAppliedIndex uint64 `json:"last_applied_index"`
}

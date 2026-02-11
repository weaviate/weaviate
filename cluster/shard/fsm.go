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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/hashicorp/raft"
	"github.com/klauspost/compress/s2"
	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"google.golang.org/protobuf/proto"
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

// FSM implements raft.FSM for per-shard object replication.
// Each physical shard has its own RAFT cluster with a dedicated FSM.
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
}

// NewFSM creates a new FSM for a shard's RAFT cluster.
func NewFSM(className, shardName, nodeID string, log logrus.FieldLogger) *FSM {
	return &FSM{
		className: className,
		shardName: shardName,
		nodeID:    nodeID,
		log: log.WithFields(logrus.Fields{
			"component": "shard_raft_fsm",
			"class":     className,
			"shard":     shardName,
		}),
	}
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

// SetStateTransferer sets the state transferer used by Restore() to download
// shard data from the leader when a foreign snapshot is detected.
func (f *FSM) SetStateTransferer(st StateTransferer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateTransferer = st
}

// Apply implements raft.FSM. It processes committed log entries and applies
// them to the shard. This method must be deterministic.
func (f *FSM) Apply(l *raft.Log) any {
	f.mu.RLock()
	shard := f.shard
	f.mu.RUnlock()

	if shard == nil {
		f.log.Error("shard not set, cannot apply log entry")
		return Response{Version: l.Index, Error: fmt.Errorf("shard not set")}
	}

	// Parse the command
	var req shardproto.ApplyRequest
	if err := proto.Unmarshal(l.Data, &req); err != nil {
		f.log.WithError(err).Error("failed to unmarshal command")
		return Response{Version: l.Index, Error: fmt.Errorf("unmarshal command: %w", err)}
	}

	// Decompress sub_command if the entry was compressed by the replicator.
	// Uncompressed entries (Compressed=false) pass through unchanged, which
	// ensures backwards compatibility during rolling upgrades.
	if req.Compressed {
		decompressed, err := s2.Decode(nil, req.SubCommand)
		if err != nil {
			f.log.WithError(err).Error("failed to decompress sub_command")
			return Response{Version: l.Index, Error: fmt.Errorf("decompress: %w", err)}
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

	// Update last applied index
	f.lastAppliedIndex.Store(l.Index)

	if applyErr != nil {
		f.log.WithError(applyErr).WithField("index", l.Index).Error("failed to apply command")
		return Response{Version: l.Index, Error: applyErr}
	}

	return Response{Version: l.Index}
}

// putObject applies a PUT_OBJECT command to the shard.
func (f *FSM) putObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.PutObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal PutObject subcommand: %w", err)
	}

	obj, err := storobj.FromBinary(subreq.Object)
	if err != nil {
		return fmt.Errorf("get object from command: %w", err)
	}

	// Use a background context since FSM.Apply should complete regardless
	// of the original request context.
	ctx := context.Background()

	if err := shard.PutObject(ctx, obj); err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	return nil
}

// deleteObject applies a DELETE_OBJECT command to the shard.
func (f *FSM) deleteObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.DeleteObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal DeleteObject subcommand: %w", err)
	}

	id := strfmt.UUID(subreq.Id)

	var deletionTime time.Time
	if subreq.DeletionTimeUnix != 0 {
		deletionTime = time.Unix(0, subreq.DeletionTimeUnix)
	}

	ctx := context.Background()
	if err := shard.DeleteObject(ctx, id, deletionTime); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	return nil
}

// mergeObject applies a MERGE_OBJECT command to the shard.
func (f *FSM) mergeObject(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.MergeObjectRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal MergeObject subcommand: %w", err)
	}

	var doc objects.MergeDocument
	if err := json.Unmarshal(subreq.MergeDocumentJson, &doc); err != nil {
		return fmt.Errorf("unmarshal merge document: %w", err)
	}

	ctx := context.Background()
	if err := shard.MergeObject(ctx, doc); err != nil {
		return fmt.Errorf("merge object: %w", err)
	}

	return nil
}

// putObjectsBatch applies a PUT_OBJECTS_BATCH command to the shard.
func (f *FSM) putObjectsBatch(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.PutObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal PutObjectsBatch subcommand: %w", err)
	}

	objs := make([]*storobj.Object, len(subreq.Objects))
	for i, raw := range subreq.Objects {
		obj, err := storobj.FromBinary(raw)
		if err != nil {
			return fmt.Errorf("deserialize object %d: %w", i, err)
		}
		objs[i] = obj
	}

	ctx := context.Background()
	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("put objects batch: %w", err)
		}
	}

	return nil
}

// deleteObjectsBatch applies a DELETE_OBJECTS_BATCH command to the shard.
func (f *FSM) deleteObjectsBatch(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.DeleteObjectsBatchRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal DeleteObjectsBatch subcommand: %w", err)
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
	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("delete objects batch: %w", r.Err)
		}
	}

	return nil
}

// addReferences applies an ADD_REFERENCES command to the shard.
func (f *FSM) addReferences(shard shard, req *shardproto.ApplyRequest) error {
	var subreq shardproto.AddReferencesRequest
	if err := proto.Unmarshal(req.SubCommand, &subreq); err != nil {
		return fmt.Errorf("unmarshal AddReferences subcommand: %w", err)
	}

	var refs objects.BatchReferences
	if err := json.Unmarshal(subreq.ReferencesJson, &refs); err != nil {
		return fmt.Errorf("unmarshal references: %w", err)
	}

	ctx := context.Background()
	errs := shard.AddReferencesBatch(ctx, refs)
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("add references: %w", err)
		}
	}

	return nil
}

// Snapshot implements raft.FSM. It captures a lightweight reference to the
// current FSM state and returns quickly. The expensive FlushMemtables call
// happens in FSMSnapshot.Persist(), which runs on the snapshot goroutine
// and does not block Apply().
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.log.Info("creating snapshot")

	f.mu.RLock()
	shard := f.shard
	f.mu.RUnlock()

	return &FSMSnapshot{
		className:        f.className,
		shardName:        f.shardName,
		nodeID:           f.nodeID,
		lastAppliedIndex: f.lastAppliedIndex.Load(),
		log:              f.log,
		shard:            shard,
	}, nil
}

// Restore implements raft.FSM. It restores the FSM state from a snapshot.
// If the snapshot was created by a different node (foreign snapshot), it
// triggers an out-of-band state transfer to download shard data from the
// current leader. The StateTransferer determines the leader dynamically —
// we don't use snap.NodeID for leader determination since the leader may
// have changed between snapshot creation and restore.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snap shardSnapshotData
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	// Verify snapshot is for the correct shard
	if snap.ClassName != f.className || snap.ShardName != f.shardName {
		return fmt.Errorf("snapshot class/shard mismatch: expected %s/%s, got %s/%s",
			f.className, f.shardName, snap.ClassName, snap.ShardName)
	}

	// Trigger state transfer if this is a foreign snapshot (from another node).
	f.mu.RLock()
	st := f.stateTransferer
	f.mu.RUnlock()

	if snap.NodeID != f.nodeID && st != nil {
		f.log.WithFields(logrus.Fields{
			"snapshot_node_id": snap.NodeID,
			"local_node_id":   f.nodeID,
		}).Info("foreign snapshot detected, initiating state transfer")

		ctx := context.Background()
		if err := st.TransferState(ctx, f.className, f.shardName); err != nil {
			return fmt.Errorf("state transfer for %s/%s: %w", f.className, f.shardName, err)
		}
	}

	f.lastAppliedIndex.Store(snap.LastAppliedIndex)
	f.log.WithField("lastAppliedIndex", snap.LastAppliedIndex).Info("restored from snapshot")

	return nil
}

// LastAppliedIndex returns the last RAFT log index that was applied.
func (f *FSM) LastAppliedIndex() uint64 {
	return f.lastAppliedIndex.Load()
}

// shardSnapshotData is the JSON-serializable snapshot data structure.
type shardSnapshotData struct {
	ClassName        string `json:"class_name"`
	ShardName        string `json:"shard_name"`
	NodeID           string `json:"node_id"`
	LastAppliedIndex uint64 `json:"last_applied_index"`
}

// FSMSnapshot implements raft.FSMSnapshot for shard snapshots.
type FSMSnapshot struct {
	className        string
	shardName        string
	nodeID           string
	lastAppliedIndex uint64
	log              logrus.FieldLogger
	shard            shard
}

// Persist implements raft.FSMSnapshot. It writes the snapshot to the sink.
// Before writing, all memtables are flushed to LSM segments. This ensures
// all applied entries up to lastAppliedIndex are durable before RAFT
// truncates the log. Persist() runs on the snapshot goroutine (not the
// FSM apply thread), so this does not block new Apply() calls.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	if s.shard != nil {
		ctx := context.Background()
		if err := s.shard.FlushMemtables(ctx); err != nil {
			sink.Cancel()
			return fmt.Errorf("flush memtables before snapshot persist: %w", err)
		}
	}

	snap := shardSnapshotData{
		ClassName:        s.className,
		ShardName:        s.shardName,
		NodeID:           s.nodeID,
		LastAppliedIndex: s.lastAppliedIndex,
	}

	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		sink.Cancel()
		return fmt.Errorf("encode snapshot: %w", err)
	}

	s.log.WithField("lastAppliedIndex", s.lastAppliedIndex).Info("snapshot persisted")
	return nil
}

// Release implements raft.FSMSnapshot. It's called when the snapshot is no
// longer needed.
func (s *FSMSnapshot) Release() {
	// No resources to release
}

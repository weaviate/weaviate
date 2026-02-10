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

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storobj"
	"google.golang.org/protobuf/proto"
)

// shard defines the operations that can be performed on a shard.
// This interface is implemented by the actual shard in adapters/repos/db.
type shard interface {
	// PutObject stores an object in the shard.
	PutObject(ctx context.Context, obj *storobj.Object) error

	// Name returns the shard name.
	Name() string
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
		return Response{Version: l.Index, Error: fmt.Errorf("unmarshal command: %v", err)}
	}

	// Dispatch based on command type
	var applyErr error
	switch req.Type {
	case shardproto.ApplyRequest_TYPE_PUT_OBJECT:
		applyErr = f.putObject(shard, &req)
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

// Snapshot implements raft.FSM. It returns a snapshot of the FSM state.
// For shard data, the actual objects are stored in the LSM store, so we only
// need to capture the last applied index for consistency verification.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.log.Info("creating snapshot")
	return &FSMSnapshot{
		className:        f.className,
		shardName:        f.shardName,
		nodeID:           f.nodeID,
		lastAppliedIndex: f.lastAppliedIndex.Load(),
		log:              f.log,
	}, nil
}

// Restore implements raft.FSM. It restores the FSM state from a snapshot.
// Since the actual object data is persisted in the LSM store, we only need
// to restore the last applied index.
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
}

// Persist implements raft.FSMSnapshot. It writes the snapshot to the sink.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

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

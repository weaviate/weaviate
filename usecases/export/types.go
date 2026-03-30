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

package export

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// BackendProvider provides access to storage backends for export.
type BackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
}

// Selector provides access to shards and classes for export operations.
type Selector interface {
	ListClasses(ctx context.Context) []string
	ShardOwnership(ctx context.Context, className string) (map[string][]string, error)
	IsMultiTenant(ctx context.Context, className string) bool
	IsAsyncReplicationEnabled(ctx context.Context, className string) bool

	// SnapshotShards creates point-in-time snapshots of the objects buckets
	// for the given shards of a class. For active (loaded) shards it flushes
	// and hard-links from the live bucket. For inactive (unloaded) shards it
	// hard-links directly from the on-disk bucket directory without loading
	// the shard. Skipped shards (e.g. offloaded tenants) are included in the
	// result with a skip reason and nil snapshot.
	SnapshotShards(ctx context.Context, className string, shardNames []string, exportID string) ([]ShardSnapshotResult, error)
}

// ShardSnapshotResult holds the outcome of snapshotting a single shard.
// If SkipReason is non-empty the shard was skipped and SnapshotDir is empty.
type ShardSnapshotResult struct {
	ShardName   string
	SnapshotDir string // path to the snapshot directory; empty if skipped
	Strategy    string // bucket strategy (e.g. StrategyReplace)
	SkipReason  string // non-empty if the shard was skipped
}

// newBytesReadCloser wraps data in a backup.ReadCloserWithError suitable for
// BackupBackend.Write calls that write small blobs (metadata, status JSON).
func newBytesReadCloser(data []byte) backup.ReadCloserWithError {
	return &bytesReadCloser{Reader: bytes.NewReader(data)}
}

type bytesReadCloser struct {
	*bytes.Reader
}

func (*bytesReadCloser) Close() error               { return nil }
func (*bytesReadCloser) CloseWithError(error) error { return nil }

// ShardProgress tracks the progress of exporting a single shard.
// Used internally and in the S3 NodeStatus format.
//
// objectsWritten is an atomic counter that workers increment lock-free.
// It is copied into ObjectsExported by SyncAndSnapshot before each
// JSON marshal.
type ShardProgress struct {
	objectsWritten  atomic.Int64
	Status          export.ShardStatus `json:"status"`
	ObjectsExported int64              `json:"objectsExported"`
	Error           string             `json:"error,omitempty"`
	SkipReason      string             `json:"skipReason,omitempty"`
}

// ExportMetadata is written to S3 alongside the parquet files.
// It is the single source of truth for an export's configuration and status.
type ExportMetadata struct {
	ID              string                                     `json:"id"`
	Backend         string                                     `json:"backend"`
	StartedAt       time.Time                                  `json:"startedAt"`
	CompletedAt     time.Time                                  `json:"completedAt"`
	Status          export.Status                              `json:"status"`
	Classes         []string                                   `json:"classes"`
	NodeAssignments map[string]map[string][]string             `json:"nodeAssignments,omitempty"` // node → className → []shardName
	Error           string                                     `json:"error,omitempty"`
	ShardStatus     map[string]map[string]models.ShardProgress `json:"shardStatus,omitempty"`
}

// exportNodeInfo holds per-node information during 2PC-like coordination.
type exportNodeInfo struct {
	req  *ExportRequest
	host string // empty for local node
}

// ExportRequest is sent from coordinator to participant nodes
type ExportRequest struct {
	ID           string              `json:"id"`
	Backend      string              `json:"backend"`
	Classes      []string            `json:"classes"`
	Shards       map[string][]string `json:"shards"` // className → []shardName
	Bucket       string              `json:"bucket"`
	Path         string              `json:"path"`
	NodeName     string              `json:"nodeName"`
	SiblingNodes []string            `json:"siblingNodes,omitempty"` // other node names in the same export

	// Fields below are set for multi-node exports
	StartedAt       time.Time                      `json:"startedAt,omitempty"`
	NodeAssignments map[string]map[string][]string `json:"nodeAssignments,omitempty"` // node → className → []shardName
}

// NodeStatus is written to S3 by each participant node.
// The embedded mutex protects the maps and non-atomic fields.
// ShardProgress.objectsWritten is updated lock-free via atomics.
type NodeStatus struct {
	mu            sync.Mutex
	NodeName      string                               `json:"nodeName"`
	Status        export.Status                        `json:"status"`
	ShardProgress map[string]map[string]*ShardProgress `json:"shardProgress,omitempty"` // className → shardName → progress
	Error         string                               `json:"error,omitempty"`
	CompletedAt   time.Time                            `json:"completedAt,omitempty"`
	Version       string                               `json:"version"`
}

// SetShardProgress updates a shard's export progress in a thread-safe manner.
// ObjectsExported is not set here; it is synced from the atomic counter
// by SyncAndSnapshot before each JSON marshal.
func (ns *NodeStatus) SetShardProgress(className, shardName string, status export.ShardStatus, errMsg, skipReason string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if ns.ShardProgress[className] == nil {
		ns.ShardProgress[className] = make(map[string]*ShardProgress)
	}
	sp := ns.ShardProgress[className][shardName]
	if sp == nil {
		sp = &ShardProgress{}
		ns.ShardProgress[className][shardName] = sp
	}
	sp.Status = status
	sp.Error = errMsg
	sp.SkipReason = skipReason

	if status == export.ShardFailed {
		ns.Status = export.Failed
		msg := fmt.Sprintf("failed to export class %s shard %s: %s", className, shardName, errMsg)
		if ns.Error != "" {
			ns.Error += "; " + msg
		} else {
			ns.Error = msg
		}
	}
}

// SetNodeError marks the node export as failed with an arbitrary message.
func (ns *NodeStatus) SetNodeError(msg string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Status = export.Failed
	if ns.Error != "" {
		ns.Error += "; " + msg
	} else {
		ns.Error = msg
	}
}

// SetFailed marks the node export as failed in a thread-safe manner.
func (ns *NodeStatus) SetFailed(className string, err error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Status = export.Failed
	msg := fmt.Sprintf("failed to export class %s: %v", className, err)
	if ns.Error != "" {
		ns.Error += "; " + msg
	} else {
		ns.Error = msg
	}
}

// SetSuccess marks the node export as succeeded in a thread-safe manner.
func (ns *NodeStatus) SetSuccess() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Status = export.Success
	ns.CompletedAt = time.Now().UTC()
}

// getShardProgress returns the ShardProgress for the given class/shard pair,
// or nil if it does not exist. The mutex is held only for the map lookup.
func (ns *NodeStatus) getShardProgress(className, shardName string) *ShardProgress {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.ShardProgress[className][shardName]
}

// AddShardExported atomically increments a shard's written-objects counter.
// The mutex is held only for the map lookup; the counter itself is lock-free.
func (ns *NodeStatus) AddShardExported(className, shardName string, delta int64) {
	if sp := ns.getShardProgress(className, shardName); sp != nil {
		sp.objectsWritten.Add(delta)
	}
}

// GetShardWritten returns the current value of a shard's atomic written-objects
// counter. The mutex is held only for the map lookup.
func (ns *NodeStatus) GetShardWritten(className, shardName string) int64 {
	if sp := ns.getShardProgress(className, shardName); sp != nil {
		return sp.objectsWritten.Load()
	}
	return 0
}

// SyncAndSnapshot syncs live atomic counters into ObjectsExported and returns
// a deep copy of the NodeStatus suitable for marshaling, all under a single
// lock acquisition.
func (ns *NodeStatus) SyncAndSnapshot() *NodeStatus {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	// Copy atomic counters into the JSON-visible field.
	for _, shards := range ns.ShardProgress {
		for _, sp := range shards {
			sp.ObjectsExported = sp.objectsWritten.Load()
		}
	}

	// Deep copy.
	cp := &NodeStatus{
		NodeName:    ns.NodeName,
		Status:      ns.Status,
		Error:       ns.Error,
		CompletedAt: ns.CompletedAt,
		Version:     ns.Version,
	}
	if len(ns.ShardProgress) > 0 {
		cp.ShardProgress = make(map[string]map[string]*ShardProgress, len(ns.ShardProgress))
		for className, shards := range ns.ShardProgress {
			cp.ShardProgress[className] = make(map[string]*ShardProgress, len(shards))
			for shardName, sp := range shards {
				cp.ShardProgress[className][shardName] = &ShardProgress{
					Status:          sp.Status,
					ObjectsExported: sp.ObjectsExported,
					Error:           sp.Error,
					SkipReason:      sp.SkipReason,
				}
			}
		}
	}
	return cp
}

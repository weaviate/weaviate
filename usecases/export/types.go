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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/models"
)

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
	objectsWritten  atomic.Int64       `json:"-"`
	Status          export.ShardStatus `json:"status"`
	ObjectsExported int64              `json:"objectsExported"`
	Error           string             `json:"error,omitempty"`
	SkipReason      string             `json:"skipReason,omitempty"`
}

// ExportMetadata is written to S3 alongside the parquet files
type ExportMetadata struct {
	ID          string                                     `json:"id"`
	Backend     string                                     `json:"backend"`
	StartedAt   time.Time                                  `json:"startedAt"`
	CompletedAt time.Time                                  `json:"completedAt"`
	Status      export.Status                              `json:"status"`
	Classes     []string                                   `json:"classes"`
	Error       string                                     `json:"error,omitempty"`
	ShardStatus map[string]map[string]models.ShardProgress `json:"shardStatus,omitempty"`
	Version     string                                     `json:"version"`
}

// exportNodeInfo holds per-node information during 2PC coordination.
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
}

// NodeStatus is written to S3 by each participant node.
// The embedded mutex protects all fields from concurrent access.
type NodeStatus struct {
	mu            sync.Mutex                           `json:"-"`
	NodeName      string                               `json:"nodeName"`
	Status        export.Status                        `json:"status"`
	ShardProgress map[string]map[string]*ShardProgress `json:"shardProgress,omitempty"` // className → shardName → progress
	Error         string                               `json:"error,omitempty"`
	CompletedAt   time.Time                            `json:"completedAt,omitempty"`
}

// SetShardProgress updates a shard's export progress in a thread-safe manner.
func (ns *NodeStatus) SetShardProgress(className, shardName string, status export.ShardStatus, objects int64, errMsg, skipReason string) {
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
	sp.ObjectsExported = objects
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

// AddShardExported atomically increments a shard's written-objects counter.
// The mutex is held only for the map lookup; the counter itself is lock-free.
func (ns *NodeStatus) AddShardExported(className, shardName string, delta int64) {
	ns.mu.Lock()
	sp := ns.ShardProgress[className][shardName]
	ns.mu.Unlock()
	if sp != nil {
		sp.objectsWritten.Add(delta)
	}
}

// GetShardWritten returns the current value of a shard's atomic written-objects
// counter. The mutex is held only for the map lookup.
func (ns *NodeStatus) GetShardWritten(className, shardName string) int64 {
	ns.mu.Lock()
	sp := ns.ShardProgress[className][shardName]
	ns.mu.Unlock()
	if sp != nil {
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

// snapshot returns a deep copy of the NodeStatus suitable for marshaling
// outside the lock.
func (ns *NodeStatus) snapshot() *NodeStatus {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	cp := &NodeStatus{
		NodeName:    ns.NodeName,
		Status:      ns.Status,
		Error:       ns.Error,
		CompletedAt: ns.CompletedAt,
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

// ExportPlan is written to S3 by the coordinator
type ExportPlan struct {
	ID              string                         `json:"id"`
	Backend         string                         `json:"backend"`
	Classes         []string                       `json:"classes"`
	NodeAssignments map[string]map[string][]string `json:"nodeAssignments"` // node → className → []shardName
	StartedAt       time.Time                      `json:"startedAt"`
}

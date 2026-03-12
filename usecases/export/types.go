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
type ShardProgress struct {
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
	sp := ns.ShardProgress[className][shardName]
	sp.Status = status
	sp.ObjectsExported = objects
	sp.Error = errMsg
	sp.SkipReason = skipReason
}

// SetFailed marks the node export as failed in a thread-safe manner.
func (ns *NodeStatus) SetFailed(className string, err error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Status = export.Failed
	ns.Error = fmt.Sprintf("failed to export class %s: %v", className, err)
}

// SetSuccess marks the node export as succeeded in a thread-safe manner.
func (ns *NodeStatus) SetSuccess() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.Status = export.Success
	ns.CompletedAt = time.Now().UTC()
}

// ExportPlan is written to S3 by the coordinator
type ExportPlan struct {
	ID              string                         `json:"id"`
	Backend         string                         `json:"backend"`
	Classes         []string                       `json:"classes"`
	NodeAssignments map[string]map[string][]string `json:"nodeAssignments"` // node → className → []shardName
	StartedAt       time.Time                      `json:"startedAt"`
}

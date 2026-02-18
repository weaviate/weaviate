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
	"time"

	"github.com/weaviate/weaviate/entities/export"
)

// ExportStatus represents the current status of an export
type ExportStatus struct {
	ID          string                                   `json:"id"`
	Backend     string                                   `json:"backend"`
	Path        string                                   `json:"path"`
	Status      export.Status                            `json:"status"`
	StartedAt   time.Time                                `json:"startedAt"`
	Error       string                                   `json:"error,omitempty"`
	Classes     []string                                 `json:"classes,omitempty"`
	ShardStatus map[string]map[string]*ShardExportStatus `json:"shardStatus,omitempty"` // className → shardName → progress
}

// ShardExportStatus tracks the progress of exporting a single shard
type ShardExportStatus struct {
	Status          export.Status `json:"status"`
	ObjectsExported int64         `json:"objectsExported"`
	Error           string        `json:"error,omitempty"`
}

// ClassProgress tracks the progress of exporting a single class
type ClassProgress struct {
	Status          export.Status `json:"status"`
	ObjectsExported int64         `json:"objectsExported"`
	FileSizeBytes   int64         `json:"fileSizeBytes,omitempty"`
	Error           string        `json:"error,omitempty"`
}

// ExportMetadata is written to S3 alongside the parquet files
type ExportMetadata struct {
	ID          string                    `json:"id"`
	Backend     string                    `json:"backend"`
	StartedAt   time.Time                 `json:"startedAt"`
	CompletedAt time.Time                 `json:"completedAt"`
	Status      export.Status             `json:"status"`
	Classes     []string                  `json:"classes"`
	Progress    map[string]*ClassProgress `json:"progress"`
	Error       string                    `json:"error,omitempty"`
	Version     string                    `json:"version"`
}

// ExportRequest is sent from coordinator to participant nodes
type ExportRequest struct {
	ID       string              `json:"id"`
	Backend  string              `json:"backend"`
	Classes  []string            `json:"classes"`
	Shards   map[string][]string `json:"shards"` // className → []shardName
	Bucket   string              `json:"bucket"`
	Path     string              `json:"path"`
	NodeName string              `json:"nodeName"`
}

// NodeStatus is written to S3 by each participant node
type NodeStatus struct {
	NodeName      string                                   `json:"nodeName"`
	Status        export.Status                            `json:"status"`
	ClassProgress map[string]*ClassProgress                `json:"classProgress,omitempty"`
	ShardProgress map[string]map[string]*ShardExportStatus `json:"shardProgress,omitempty"` // className → shardName → progress
	Error         string                                   `json:"error,omitempty"`
	CompletedAt   time.Time                                `json:"completedAt,omitempty"`
}

// ExportPlan is written to S3 by the coordinator
type ExportPlan struct {
	ID              string                         `json:"id"`
	Backend         string                         `json:"backend"`
	Classes         []string                       `json:"classes"`
	NodeAssignments map[string]map[string][]string `json:"nodeAssignments"` // node → className → []shardName
	StartedAt       time.Time                      `json:"startedAt"`
}

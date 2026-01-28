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
	ID        string                    `json:"id"`
	Backend   string                    `json:"backend"`
	Path      string                    `json:"path"`
	Status    export.Status             `json:"status"`
	StartedAt time.Time                 `json:"startedAt"`
	Error     string                    `json:"error,omitempty"`
	Classes   []string                  `json:"classes,omitempty"`
	Progress  map[string]*ClassProgress `json:"progress,omitempty"`
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

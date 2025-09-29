//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Structs matching the Python dataclasses

type Dimensionality struct {
	Dimensions *int `json:"dimensionality,omitempty"`
	Count      *int `json:"count,omitempty"`
}

type VectorUsage struct {
	Name                   *string          `json:"name,omitempty"`
	VectorIndexType        *string          `json:"vector_index_type,omitempty"`
	IsDynamic              *bool            `json:"is_dynamic,omitempty"`
	Compression            *string          `json:"compression,omitempty"`
	VectorCompressionRatio *float64         `json:"vector_compression_ratio,omitempty"`
	Bits                   *int             `json:"bits,omitempty"`
	Dimensionalities       []Dimensionality `json:"dimensionalities,omitempty"`
}

type ShardUsage struct {
	Name                *string       `json:"name,omitempty"`
	Status              *string       `json:"status,omitempty"`
	ObjectsCount        *int          `json:"objects_count,omitempty"`
	ObjectsStorageBytes *int64        `json:"objects_storage_bytes,omitempty"`
	VectorStorageBytes  *int64        `json:"vector_storage_bytes,omitempty"`
	NamedVectors        []VectorUsage `json:"named_vectors,omitempty"`
}

type CollectionUsage struct {
	Name              *string      `json:"name,omitempty"`
	ReplicationFactor *int         `json:"replication_factor,omitempty"`
	UniqueShardCount  *int         `json:"unique_shard_count,omitempty"`
	Shards            []ShardUsage `json:"shards,omitempty"`
}

type BackupUsage struct {
	ID             *string  `json:"id,omitempty"`
	CompletionTime *string  `json:"completion_time,omitempty"`
	SizeInGiB      *float64 `json:"size_in_gib,omitempty"`
	Type           *string  `json:"type,omitempty"`
	Collections    []string `json:"collections,omitempty"`
}

type Report struct {
	Version        *string                `json:"version,omitempty"`
	Node           *string                `json:"node,omitempty"`
	Collections    []CollectionUsage      `json:"collections,omitempty"`
	Backups        []BackupUsage          `json:"backups,omitempty"`
	CollectingTime *string                `json:"collecting_time,omitempty"`
	Schema         map[string]interface{} `json:"schema,omitempty"`
}

// Get the debug usage report from the endpoint
func GetDebugUsage() (*Report, error) {
	url := "http://localhost:6060/debug/usage?exactObjectCount=false"
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to call endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var report Report
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return &report, nil
}

// Get a specific collection by name
func GetDebugUsageForCollection(collection string) (*CollectionUsage, error) {
	report, err := GetDebugUsage()
	if err != nil {
		return nil, err
	}
	for _, col := range report.Collections {
		if col.Name != nil && *col.Name == collection {
			return &col, nil
		}
	}
	return nil, fmt.Errorf("collection %s not found in debug usage report", collection)
}

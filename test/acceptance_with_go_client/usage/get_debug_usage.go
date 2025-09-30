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
func getDebugUsage() (*Report, error) {
	url := "http://localhost:6060/debug/usage?exactObjectCount=false"
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to call endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, error: %s", resp.StatusCode, string(body))
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
func getDebugUsageForCollection(collection string) (*CollectionUsage, error) {
	report, err := getDebugUsage()
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

// Compare two Reports by value, not pointer
func ReportsAreDifferent(a, b *Report) bool {
	if a == nil || b == nil {
		return a != b
	}
	if valueStr(a.Version) != valueStr(b.Version) {
		return true
	}
	if valueStr(a.Node) != valueStr(b.Node) {
		return true
	}
	// CollectingTime is intentionally ignored
	if !collectionsEqual(a.Collections, b.Collections) {
		return true
	}
	if !backupsEqual(a.Backups, b.Backups) {
		return true
	}
	if !schemasEqual(a.Schema, b.Schema) {
		return true
	}
	return false
}

func valueStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func valueInt(i *int) int {
	if i == nil {
		return 0
	}
	return *i
}

func valueInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func valueFloat64(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

func valueBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func collectionsEqual(a, b []CollectionUsage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !collectionUsageEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func collectionUsageEqual(a, b CollectionUsage) bool {
	if valueStr(a.Name) != valueStr(b.Name) {
		return false
	}
	if valueInt(a.ReplicationFactor) != valueInt(b.ReplicationFactor) {
		return false
	}
	if valueInt(a.UniqueShardCount) != valueInt(b.UniqueShardCount) {
		return false
	}
	if !shardsEqual(a.Shards, b.Shards) {
		return false
	}
	return true
}

func shardsEqual(a, b []ShardUsage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !shardUsageEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func shardUsageEqual(a, b ShardUsage) bool {
	if valueStr(a.Name) != valueStr(b.Name) {
		return false
	}
	if valueStr(a.Status) != valueStr(b.Status) {
		return false
	}
	if valueInt(a.ObjectsCount) != valueInt(b.ObjectsCount) {
		return false
	}
	if valueInt64(a.ObjectsStorageBytes) != valueInt64(b.ObjectsStorageBytes) {
		return false
	}
	if valueInt64(a.VectorStorageBytes) != valueInt64(b.VectorStorageBytes) {
		return false
	}
	if !vectorsEqual(a.NamedVectors, b.NamedVectors) {
		return false
	}
	return true
}

func vectorsEqual(a, b []VectorUsage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !vectorUsageEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func vectorUsageEqual(a, b VectorUsage) bool {
	if valueStr(a.Name) != valueStr(b.Name) {
		return false
	}
	if valueStr(a.VectorIndexType) != valueStr(b.VectorIndexType) {
		return false
	}
	if valueBool(a.IsDynamic) != valueBool(b.IsDynamic) {
		return false
	}
	if valueStr(a.Compression) != valueStr(b.Compression) {
		return false
	}
	if valueFloat64(a.VectorCompressionRatio) != valueFloat64(b.VectorCompressionRatio) {
		return false
	}
	if valueInt(a.Bits) != valueInt(b.Bits) {
		return false
	}
	if !dimensionalitiesEqual(a.Dimensionalities, b.Dimensionalities) {
		return false
	}
	return true
}

func dimensionalitiesEqual(a, b []Dimensionality) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if valueInt(a[i].Dimensions) != valueInt(b[i].Dimensions) {
			return false
		}
		if valueInt(a[i].Count) != valueInt(b[i].Count) {
			return false
		}
	}
	return true
}

func backupsEqual(a, b []BackupUsage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !backupUsageEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func backupUsageEqual(a, b BackupUsage) bool {
	if valueStr(a.ID) != valueStr(b.ID) {
		return false
	}
	if valueStr(a.CompletionTime) != valueStr(b.CompletionTime) {
		return false
	}
	if valueFloat64(a.SizeInGiB) != valueFloat64(b.SizeInGiB) {
		return false
	}
	if valueStr(a.Type) != valueStr(b.Type) {
		return false
	}
	if !stringSlicesEqual(a.Collections, b.Collections) {
		return false
	}
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func schemasEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !interfaceDeepEqual(va, vb) {
			return false
		}
	}
	return true
}

// Deep equality for interface{} (for Schema field)
func interfaceDeepEqual(a, b interface{}) bool {
	switch va := a.(type) {
	case map[string]interface{}:
		vb, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		return schemasEqual(va, vb)
	case []interface{}:
		vb, ok := b.([]interface{})
		if !ok || len(va) != len(vb) {
			return false
		}
		for i := range va {
			if !interfaceDeepEqual(va[i], vb[i]) {
				return false
			}
		}
		return true
	default:
		return va == b
	}
}

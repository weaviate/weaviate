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
	"errors"
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

func getDebugUsage() (*Report, error) {
	return getDebugUsageWithPort("localhost:6060")
}

// Get the debug usage report from the endpoint
func getDebugUsageWithPort(host string) (*Report, error) {
	url := fmt.Sprintf("http://%s/debug/usage?exactObjectCount=true", host)
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

func getDebugUsageWithPortAndCollection(host, collection string) (CollectionUsage, error) {
	report, err := getDebugUsageWithPort(host)
	if err != nil {
		return CollectionUsage{}, err
	}
	for _, col := range report.Collections {
		if col.Name != nil && *col.Name == collection {
			return col, nil
		}
	}
	return CollectionUsage{}, fmt.Errorf("collection %s not found in debug usage report", collection)
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

// Compare two Reports by value, not pointer. Returns nil if equal, otherwise an error describing the difference.
func ReportsDifference(a, b *Report) error {
	if a == nil || b == nil {
		if a != b {
			return errors.New("one of the reports is nil while the other is not")
		}
		return nil
	}
	if valueStr(a.Version) != valueStr(b.Version) {
		return errors.New("Version differs: '" + valueStr(a.Version) + "' vs '" + valueStr(b.Version) + "'")
	}
	if valueStr(a.Node) != valueStr(b.Node) {
		return errors.New("Node differs: '" + valueStr(a.Node) + "' vs '" + valueStr(b.Node) + "'")
	}
	// CollectingTime is intentionally ignored
	if err := collectionsDifference(a.Collections, b.Collections); err != nil {
		return err
	}
	if err := backupsDifference(a.Backups, b.Backups); err != nil {
		return err
	}
	if err := schemasDifference(a.Schema, b.Schema); err != nil {
		return err
	}
	return nil
}

func collectionsDifference(a, b []CollectionUsage) error {
	if len(a) != len(b) {
		return errors.New("Collections length differs")
	}
	for i := range a {
		if err := collectionUsageDifference(a[i], b[i]); err != nil {
			return errors.New("Collections[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func collectionUsageDifference(a, b CollectionUsage) error {
	if valueStr(a.Name) != valueStr(b.Name) {
		return errors.New("Name differs: '" + valueStr(a.Name) + "' vs '" + valueStr(b.Name) + "'")
	}
	if valueInt(a.ReplicationFactor) != valueInt(b.ReplicationFactor) {
		return errors.New("ReplicationFactor differs: '" + itoa(valueInt(a.ReplicationFactor)) + "' vs '" + itoa(valueInt(b.ReplicationFactor)) + "'")
	}
	if valueInt(a.UniqueShardCount) != valueInt(b.UniqueShardCount) {
		return errors.New("UniqueShardCount differs: '" + itoa(valueInt(a.UniqueShardCount)) + "' vs '" + itoa(valueInt(b.UniqueShardCount)) + "'")
	}
	if err := shardsDifference(a.Shards, b.Shards); err != nil {
		return err
	}
	return nil
}

func shardsDifference(a, b []ShardUsage) error {
	if len(a) != len(b) {
		return errors.New("Shards length differs")
	}
	for i := range a {
		if err := shardUsageDifference(a[i], b[i]); err != nil {
			return errors.New("Shards[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func shardUsageDifference(a, b ShardUsage) error {
	if valueStr(a.Name) != valueStr(b.Name) {
		return errors.New("Shard Name differs: '" + valueStr(a.Name) + "' vs '" + valueStr(b.Name) + "'")
	}
	if valueStr(a.Status) != valueStr(b.Status) {
		return errors.New("Shard Status differs: '" + valueStr(a.Status) + "' vs '" + valueStr(b.Status) + "'")
	}
	if valueInt(a.ObjectsCount) != valueInt(b.ObjectsCount) {
		return errors.New("ObjectsCount differs: '" + itoa(valueInt(a.ObjectsCount)) + "' vs '" + itoa(valueInt(b.ObjectsCount)) + "'")
	}

	// these calculations are currently not stable enough to compare
	//if valueInt64(a.ObjectsStorageBytes) != valueInt64(b.ObjectsStorageBytes) {
	//	return errors.New("ObjectsStorageBytes differs: '" + itoa64(valueInt64(a.ObjectsStorageBytes)) + "' vs '" + itoa64(valueInt64(b.ObjectsStorageBytes)) + "'")
	//}
	//if valueInt64(a.VectorStorageBytes) != valueInt64(b.VectorStorageBytes) {
	//	return errors.New("VectorStorageBytes differs: '" + itoa64(valueInt64(a.VectorStorageBytes)) + "' vs '" + itoa64(valueInt64(b.VectorStorageBytes)) + "'")
	//}
	if err := vectorsDifference(a.NamedVectors, b.NamedVectors); err != nil {
		return err
	}
	return nil
}

func vectorsDifference(a, b []VectorUsage) error {
	if len(a) != len(b) {
		return errors.New("NamedVectors length differs")
	}
	for i := range a {
		if err := vectorUsageDifference(a[i], b[i]); err != nil {
			return errors.New("NamedVectors[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func vectorUsageDifference(a, b VectorUsage) error {
	if valueStr(a.Name) != valueStr(b.Name) {
		return errors.New("Vector Name differs: '" + valueStr(a.Name) + "' vs '" + valueStr(b.Name) + "'")
	}
	if valueStr(a.VectorIndexType) != valueStr(b.VectorIndexType) {
		return errors.New("VectorIndexType differs: '" + valueStr(a.VectorIndexType) + "' vs '" + valueStr(b.VectorIndexType) + "'")
	}
	if valueBool(a.IsDynamic) != valueBool(b.IsDynamic) {
		return errors.New("IsDynamic differs: '" + boolStr(valueBool(a.IsDynamic)) + "' vs '" + boolStr(valueBool(b.IsDynamic)) + "'")
	}
	if valueStr(a.Compression) != valueStr(b.Compression) {
		return errors.New("Compression differs: '" + valueStr(a.Compression) + "' vs '" + valueStr(b.Compression) + "'")
	}
	if valueFloat64(a.VectorCompressionRatio) != valueFloat64(b.VectorCompressionRatio) {
		return errors.New("VectorCompressionRatio differs: '" + floatStr(valueFloat64(a.VectorCompressionRatio)) + "' vs '" + floatStr(valueFloat64(b.VectorCompressionRatio)) + "'")
	}
	if valueInt(a.Bits) != valueInt(b.Bits) {
		return errors.New("Bits differs: '" + itoa(valueInt(a.Bits)) + "' vs '" + itoa(valueInt(b.Bits)) + "'")
	}
	if err := dimensionalitiesDifference(a.Dimensionalities, b.Dimensionalities); err != nil {
		return err
	}
	return nil
}

func dimensionalitiesDifference(a, b []Dimensionality) error {
	if len(a) != len(b) {
		return errors.New("Dimensionalities length differs")
	}
	for i := range a {
		if valueInt(a[i].Dimensions) != valueInt(b[i].Dimensions) {
			return errors.New("Dimensionalities[" + itoa(i) + "] Dimensions differs: '" + itoa(valueInt(a[i].Dimensions)) + "' vs '" + itoa(valueInt(b[i].Dimensions)) + "'")
		}
		if valueInt(a[i].Count) != valueInt(b[i].Count) {
			return errors.New("Dimensionalities[" + itoa(i) + "] Count differs: '" + itoa(valueInt(a[i].Count)) + "' vs '" + itoa(valueInt(b[i].Count)) + "'")
		}
	}
	return nil
}

func backupsDifference(a, b []BackupUsage) error {
	if len(a) != len(b) {
		return errors.New("Backups length differs")
	}
	for i := range a {
		if err := backupUsageDifference(a[i], b[i]); err != nil {
			return errors.New("Backups[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func backupUsageDifference(a, b BackupUsage) error {
	if valueStr(a.ID) != valueStr(b.ID) {
		return errors.New("Backup ID differs: '" + valueStr(a.ID) + "' vs '" + valueStr(b.ID) + "'")
	}
	if valueStr(a.CompletionTime) != valueStr(b.CompletionTime) {
		return errors.New("CompletionTime differs: '" + valueStr(a.CompletionTime) + "' vs '" + valueStr(b.CompletionTime) + "'")
	}
	if valueFloat64(a.SizeInGiB) != valueFloat64(b.SizeInGiB) {
		return errors.New("SizeInGiB differs: '" + floatStr(valueFloat64(a.SizeInGiB)) + "' vs '" + floatStr(valueFloat64(b.SizeInGiB)) + "'")
	}
	if valueStr(a.Type) != valueStr(b.Type) {
		return errors.New("Type differs: '" + valueStr(a.Type) + "' vs '" + valueStr(b.Type) + "'")
	}
	if err := stringSlicesDifference(a.Collections, b.Collections); err != nil {
		return err
	}
	return nil
}

func stringSlicesDifference(a, b []string) error {
	if len(a) != len(b) {
		return errors.New("string slice length differs")
	}
	for i := range a {
		if a[i] != b[i] {
			return errors.New("string slice[" + itoa(i) + "] differs: '" + a[i] + "' vs '" + b[i] + "'")
		}
	}
	return nil
}

func schemasDifference(a, b map[string]interface{}) error {
	if len(a) != len(b) {
		return errors.New("Schema map length differs")
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return errors.New("Schema key '" + k + "' missing in second report")
		}
		if err := interfaceDifference(va, vb, "Schema['"+k+"']"); err != nil {
			return err
		}
	}
	return nil
}

func interfaceDifference(a, b interface{}, path string) error {
	switch va := a.(type) {
	case map[string]interface{}:
		vb, ok := b.(map[string]interface{})
		if !ok {
			return errors.New(path + " type mismatch: map vs non-map")
		}
		return schemasDifference(va, vb)
	case []interface{}:
		vb, ok := b.([]interface{})
		if !ok || len(va) != len(vb) {
			return errors.New(path + " slice length/type mismatch")
		}
		for i := range va {
			if err := interfaceDifference(va[i], vb[i], path+"["+itoa(i)+"]"); err != nil {
				return err
			}
		}
		return nil
	default:
		if va != b {
			return errors.New(path + " value differs: '" + toString(va) + "' vs '" + toString(b) + "'")
		}
		return nil
	}
}

// Helper functions for string conversion
func itoa(i int) string         { return fmt.Sprintf("%d", i) }
func itoa64(i int64) string     { return fmt.Sprintf("%d", i) }
func floatStr(f float64) string { return fmt.Sprintf("%g", f) }
func boolStr(b bool) string {
	if b {
		return "true"
	} else {
		return "false"
	}
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case int:
		return itoa(t)
	case int64:
		return itoa64(t)
	case float64:
		return floatStr(t)
	case bool:
		return boolStr(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// Helper functions to safely dereference pointers
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

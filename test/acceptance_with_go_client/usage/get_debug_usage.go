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

package usage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
)

// CollectionUsage is a type alias for backward compatibility with external callers.
type CollectionUsage = usagetypes.CollectionUsage

func getDebugUsage() (*usagetypes.Report, error) {
	return getDebugUsageWithPort("localhost:6060")
}

// Get the debug usage report from the endpoint
func getDebugUsageWithPort(host string) (*usagetypes.Report, error) {
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

	var report usagetypes.Report
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return &report, nil
}

func getDebugUsageWithPortAndCollection(host, collection string) (usagetypes.CollectionUsage, error) {
	report, err := getDebugUsageWithPort(host)
	if err != nil {
		return usagetypes.CollectionUsage{}, err
	}
	for _, col := range report.Collections {
		if col != nil && col.Name == collection {
			return *col, nil
		}
	}
	return usagetypes.CollectionUsage{}, fmt.Errorf("collection %s not found in debug usage report", collection)
}

// Get a specific collection by name
func GetDebugUsageForCollection(collection string) (*usagetypes.CollectionUsage, error) {
	report, err := getDebugUsage()
	if err != nil {
		return nil, err
	}
	for _, col := range report.Collections {
		if col != nil && col.Name == collection {
			return col, nil
		}
	}
	return nil, fmt.Errorf("collection %s not found in debug usage report", collection)
}

// Compare two Reports by value, not pointer. Returns nil if equal, otherwise an error describing the difference.
func ReportsDifference(a, b *usagetypes.Report) error {
	if a == nil || b == nil {
		if a != b {
			return errors.New("one of the reports is nil while the other is not")
		}
		return nil
	}
	if a.Version != b.Version {
		return errors.New("Version differs: '" + a.Version + "' vs '" + b.Version + "'")
	}
	if a.Node != b.Node {
		return errors.New("Node differs: '" + a.Node + "' vs '" + b.Node + "'")
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

func collectionsDifference(a, b usagetypes.CollectionsUsage) error {
	if len(a) != len(b) {
		return errors.New("Collections length differs")
	}
	for i := range a {
		if err := CollectionUsageDifference(*a[i], *b[i]); err != nil {
			return errors.New("Collections[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func CollectionUsageDifference(a, b usagetypes.CollectionUsage) error {
	if a.Name != b.Name {
		return errors.New("Name differs: '" + a.Name + "' vs '" + b.Name + "'")
	}
	if a.ReplicationFactor != b.ReplicationFactor {
		return errors.New("ReplicationFactor differs: '" + itoa(a.ReplicationFactor) + "' vs '" + itoa(b.ReplicationFactor) + "'")
	}
	if a.UniqueShardCount != b.UniqueShardCount {
		return errors.New("UniqueShardCount differs: '" + itoa(a.UniqueShardCount) + "' vs '" + itoa(b.UniqueShardCount) + "'")
	}
	if err := shardsDifference(a.Shards, b.Shards); err != nil {
		return err
	}
	return nil
}

func shardsDifference(a, b usagetypes.ShardsUsage) error {
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

func shardUsageDifference(a, b *usagetypes.ShardUsage) error {
	if a.Name != b.Name {
		return errors.New("Shard Name differs: '" + a.Name + "' vs '" + b.Name + "'")
	}
	if a.Status != b.Status {
		return errors.New("Shard Status differs: '" + a.Status + "' vs '" + b.Status + "'")
	}
	if a.ObjectsCount != b.ObjectsCount {
		return errors.New("ObjectsCount differs: '" + itoa64(a.ObjectsCount) + "' vs '" + itoa64(b.ObjectsCount) + "'")
	}

	// these calculations are currently not stable enough to compare
	//if a.ObjectsStorageBytes != b.ObjectsStorageBytes {
	//	return errors.New("ObjectsStorageBytes differs: '" + itoa64(int64(a.ObjectsStorageBytes)) + "' vs '" + itoa64(int64(b.ObjectsStorageBytes)) + "'")
	//}
	//if a.VectorStorageBytes != b.VectorStorageBytes {
	//	return errors.New("VectorStorageBytes differs: '" + itoa64(int64(a.VectorStorageBytes)) + "' vs '" + itoa64(int64(b.VectorStorageBytes)) + "'")
	//}
	if err := vectorsDifference(a.NamedVectors, b.NamedVectors); err != nil {
		return err
	}
	return nil
}

func vectorsDifference(a, b usagetypes.VectorsUsage) error {
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

func vectorUsageDifference(a, b *usagetypes.VectorUsage) error {
	if a.Name != b.Name {
		return errors.New("Vector Name differs: '" + a.Name + "' vs '" + b.Name + "'")
	}
	if a.VectorIndexType != b.VectorIndexType {
		return errors.New("VectorIndexType differs: '" + a.VectorIndexType + "' vs '" + b.VectorIndexType + "'")
	}
	if a.IsDynamic != b.IsDynamic {
		return errors.New("IsDynamic differs: '" + boolStr(a.IsDynamic) + "' vs '" + boolStr(b.IsDynamic) + "'")
	}
	if a.Compression != b.Compression {
		return errors.New("Compression differs: '" + a.Compression + "' vs '" + b.Compression + "'")
	}
	if a.VectorCompressionRatio != b.VectorCompressionRatio {
		return errors.New("VectorCompressionRatio differs: '" + floatStr(a.VectorCompressionRatio) + "' vs '" + floatStr(b.VectorCompressionRatio) + "'")
	}
	if a.Bits != b.Bits {
		return errors.New("Bits differs: '" + itoa(int(a.Bits)) + "' vs '" + itoa(int(b.Bits)) + "'")
	}
	if err := dimensionalitiesDifference(a.Dimensionalities, b.Dimensionalities); err != nil {
		return err
	}
	return nil
}

func dimensionalitiesDifference(a, b []*usagetypes.Dimensionality) error {
	if len(a) != len(b) {
		return errors.New("Dimensionalities length differs")
	}
	for i := range a {
		if a[i].Dimensions != b[i].Dimensions {
			return errors.New("Dimensionalities[" + itoa(i) + "] Dimensions differs: '" + itoa(a[i].Dimensions) + "' vs '" + itoa(b[i].Dimensions) + "'")
		}
		if a[i].Count != b[i].Count {
			return errors.New("Dimensionalities[" + itoa(i) + "] Count differs: '" + itoa(a[i].Count) + "' vs '" + itoa(b[i].Count) + "'")
		}
	}
	return nil
}

func backupsDifference(a, b []*usagetypes.BackupUsage) error {
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

func backupUsageDifference(a, b *usagetypes.BackupUsage) error {
	if a.ID != b.ID {
		return errors.New("Backup ID differs: '" + a.ID + "' vs '" + b.ID + "'")
	}
	if a.CompletionTime != b.CompletionTime {
		return errors.New("CompletionTime differs: '" + a.CompletionTime + "' vs '" + b.CompletionTime + "'")
	}
	if a.SizeInGib != b.SizeInGib {
		return errors.New("SizeInGiB differs: '" + floatStr(a.SizeInGib) + "' vs '" + floatStr(b.SizeInGib) + "'")
	}
	if a.Type != b.Type {
		return errors.New("Type differs: '" + a.Type + "' vs '" + b.Type + "'")
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

func schemasDifference(a, b interface{}) error {
	aJSON, errA := json.Marshal(a)
	bJSON, errB := json.Marshal(b)
	if errA != nil || errB != nil {
		return errors.New("Schema: failed to marshal for comparison")
	}
	if string(aJSON) != string(bJSON) {
		return errors.New("Schema differs")
	}
	return nil
}

// Helper functions for string conversion
func itoa(i int) string         { return fmt.Sprintf("%d", i) }
func itoa64(i int64) string     { return fmt.Sprintf("%d", i) }
func floatStr(f float64) string { return fmt.Sprintf("%g", f) }
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

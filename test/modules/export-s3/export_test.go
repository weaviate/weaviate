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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/exporttest"
	pqexport "github.com/weaviate/weaviate/usecases/export"
)

func TestExport_SingleShard(t *testing.T) {
	className := t.Name()
	exportID := strings.ToLower(t.Name())

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 20)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	verifyParquetExport(t, exportID, className, objects)
}

func TestExport_MultiShard(t *testing.T) {
	className := t.Name()
	exportID := strings.ToLower(t.Name())

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(3),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 50)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	verifyParquetExport(t, exportID, className, objects)
}

func TestExport_MultiTenant_SingleShard(t *testing.T) {
	className := t.Name()
	exportID := strings.ToLower(t.Name())

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	})
	defer helper.DeleteClass(t, className)

	tenants := []*models.Tenant{
		{Name: "tenantA"},
		{Name: "tenantB"},
		{Name: "tenantC"},
	}
	helper.CreateTenants(t, className, tenants)

	var allObjects []*models.Object
	for _, tenant := range tenants {
		objects := makeObjects(className, tenant.Name, 10)
		helper.CreateObjectsBatch(t, objects)
		allObjects = append(allObjects, objects...)
	}

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	verifyParquetExport(t, exportID, className, allObjects)
}

func TestExport_MultiTenant_MultiShard(t *testing.T) {
	className := t.Name()
	exportID := strings.ToLower(t.Name())

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	})
	defer helper.DeleteClass(t, className)

	tenants := []*models.Tenant{
		{Name: "tenantA"},
		{Name: "tenantB"},
		{Name: "tenantC"},
		{Name: "tenantD"},
		{Name: "tenantE"},
	}
	helper.CreateTenants(t, className, tenants)

	var allObjects []*models.Object
	for _, tenant := range tenants {
		objects := makeObjects(className, tenant.Name, 10)
		helper.CreateObjectsBatch(t, objects)
		allObjects = append(allObjects, objects...)
	}

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	verifyParquetExport(t, exportID, className, allObjects)
}

func TestExport_NamedVectorAndMultiVector(t *testing.T) {
	className := t.Name()
	exportID := strings.ToLower(t.Name())

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		VectorConfig: map[string]models.VectorConfig{
			"regular": {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
			},
			"colbert": {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexConfig: map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
					},
				},
				VectorIndexType: "hnsw",
			},
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeNamedVectorObjects(className, 10)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	verifyNamedVectorParquetExport(t, exportID, className, objects)
}

func makeNamedVectorObjects(className string, count int) []*models.Object {
	objects := make([]*models.Object, count)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("object %d", i),
			},
			Vectors: models.Vectors{
				"regular": []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3},
				"colbert": [][]float32{
					{float32(i) * 0.01, float32(i) * 0.02},
					{float32(i) * 0.03, float32(i) * 0.04},
				},
			},
		}
	}
	return objects
}

func verifyNamedVectorParquetExport(t *testing.T, exportID, className string, expectedObjects []*models.Object) {
	t.Helper()

	allRows := fetchParquetRows(t, exportID, className)
	require.Len(t, allRows, len(expectedObjects), "parquet row count mismatch")

	type expectedObj struct {
		text         string
		namedVectors map[string][]float32
		multiVectors map[string][][]float32
	}

	expected := make(map[string]expectedObj, len(expectedObjects))
	for _, obj := range expectedObjects {
		props := obj.Properties.(map[string]interface{})
		eo := expectedObj{
			text:         props["text"].(string),
			namedVectors: make(map[string][]float32),
			multiVectors: make(map[string][][]float32),
		}
		for name, vec := range obj.Vectors {
			switch v := vec.(type) {
			case []float32:
				eo.namedVectors[name] = v
			case [][]float32:
				eo.multiVectors[name] = v
			}
		}
		expected[string(obj.ID)] = eo
	}

	for _, row := range allRows {
		require.Equal(t, className, row.ClassName)

		eo, ok := expected[row.ID]
		require.True(t, ok, "unexpected object ID in parquet: %s", row.ID)

		// Verify properties
		require.NotNil(t, row.Properties, "expected properties for object %s", row.ID)
		var props map[string]interface{}
		require.NoError(t, json.Unmarshal(row.Properties, &props))
		require.Equal(t, eo.text, props["text"], "text property mismatch for object %s", row.ID)

		// Verify named vectors (single vectors)
		if len(eo.namedVectors) > 0 {
			require.NotNil(t, row.NamedVectors, "expected named_vectors for object %s", row.ID)
			var namedVecs map[string][]float32
			require.NoError(t, json.Unmarshal(row.NamedVectors, &namedVecs))
			for name, expectedVec := range eo.namedVectors {
				actualVec, ok := namedVecs[name]
				require.True(t, ok, "missing named vector %s for object %s", name, row.ID)
				require.InDeltaSlice(t, expectedVec, actualVec, 1e-6, "named vector %s mismatch for object %s", name, row.ID)
			}
		}

		// Verify multi vectors
		if len(eo.multiVectors) > 0 {
			require.NotNil(t, row.MultiVectors, "expected multi_vectors for object %s", row.ID)
			var multiVecs map[string][][]float32
			require.NoError(t, json.Unmarshal(row.MultiVectors, &multiVecs))
			for name, expectedMV := range eo.multiVectors {
				actualMV, ok := multiVecs[name]
				require.True(t, ok, "missing multi vector %s for object %s", name, row.ID)
				require.Len(t, actualMV, len(expectedMV), "multi vector %s length mismatch for object %s", name, row.ID)
				for j := range expectedMV {
					require.InDeltaSlice(t, expectedMV[j], actualMV[j], 1e-6, "multi vector %s[%d] mismatch for object %s", name, j, row.ID)
				}
			}
		}

		delete(expected, row.ID)
	}

	require.Empty(t, expected, "some objects were not found in parquet export")
}

func makeObjects(className, tenant string, count int) []*models.Object {
	objects := make([]*models.Object, count)
	for i := range objects {
		obj := &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("object %d", i),
			},
		}
		if tenant != "" {
			obj.Tenant = tenant
		}
		objects[i] = obj
	}
	return objects
}

// verifyParquetExport downloads exported Parquet files from MinIO and verifies
// that they contain all expected objects with correct IDs, class names, and properties.
// In multi-node mode, each shard produces a separate file ({Class}_{Shard}.parquet),
// so we list all parquet files matching the class prefix and aggregate rows.
func verifyParquetExport(t *testing.T, exportID, className string, expectedObjects []*models.Object) {
	t.Helper()

	allRows := fetchParquetRows(t, exportID, className)
	require.Len(t, allRows, len(expectedObjects), "parquet row count mismatch")

	// Build a set of expected UUIDs
	expectedIDs := make(map[string]string, len(expectedObjects)) // id -> text
	for _, obj := range expectedObjects {
		props := obj.Properties.(map[string]interface{})
		expectedIDs[string(obj.ID)] = props["text"].(string)
	}

	// Verify each row
	for _, row := range allRows {
		require.Equal(t, className, row.ClassName, "class_name mismatch in parquet row")

		text, ok := expectedIDs[row.ID]
		require.True(t, ok, "unexpected object ID in parquet: %s", row.ID)

		// Verify properties contain the expected text
		require.NotNil(t, row.Properties, "expected properties for object %s", row.ID)
		var props map[string]interface{}
		require.NoError(t, json.Unmarshal(row.Properties, &props))
		require.Equal(t, text, props["text"], "text property mismatch for object %s", row.ID)

		delete(expectedIDs, row.ID)
	}

	require.Empty(t, expectedIDs, "some objects were not found in parquet export")
}

// fetchParquetRows downloads all parquet files for a class from S3 and returns
// the aggregated rows. Shared by all verify* functions.
func fetchParquetRows(t *testing.T, exportID, className string) []pqexport.ParquetRow {
	t.Helper()

	keys := listParquetKeys(t, exportID, className)
	require.NotEmpty(t, keys, "no parquet files found for class %s", className)

	var allRows []pqexport.ParquetRow
	for _, key := range keys {
		data := downloadS3Object(t, s3Bucket, key)
		rows := readParquetRows(t, data)
		allRows = append(allRows, rows...)
	}
	return allRows
}

// listParquetKeys lists all S3 keys under the export that match the class name.
// Handles both single-node ({Class}.parquet) and multi-node ({Class}_{Shard}.parquet).
func listParquetKeys(t *testing.T, exportID, className string) []string {
	t.Helper()

	prefix := fmt.Sprintf("%s/%s", exportID, className)
	resp, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err, "failed to list objects with prefix %s", prefix)

	var keys []string
	for _, obj := range resp.Contents {
		key := aws.ToString(obj.Key)
		if strings.HasSuffix(key, ".parquet") {
			keys = append(keys, key)
		}
	}
	return keys
}

func downloadS3Object(t *testing.T, bucket, key string) []byte {
	t.Helper()

	resp, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "failed to download s3://%s/%s", bucket, key)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read s3 object body")
	return data
}

func readParquetRows(t *testing.T, data []byte) []pqexport.ParquetRow {
	t.Helper()

	reader := parquet.NewGenericReader[pqexport.ParquetRow](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]pqexport.ParquetRow, reader.NumRows())
	n, err := reader.Read(rows)
	// Read returns io.EOF when all rows have been consumed
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err, "failed to read parquet rows")
	}
	require.Equal(t, int(reader.NumRows()), n, "did not read all parquet rows")

	return rows[:n]
}

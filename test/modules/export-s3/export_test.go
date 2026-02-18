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
	require.Contains(t, resp.Payload.Progress, className)
	require.Equal(t, int64(20), resp.Payload.Progress[className].ObjectsExported)

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
	require.Contains(t, resp.Payload.Progress, className)
	require.Equal(t, int64(50), resp.Payload.Progress[className].ObjectsExported)

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
	require.Contains(t, resp.Payload.Progress, className)
	require.Equal(t, int64(30), resp.Payload.Progress[className].ObjectsExported)

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
	require.Contains(t, resp.Payload.Progress, className)
	require.Equal(t, int64(50), resp.Payload.Progress[className].ObjectsExported)

	verifyParquetExport(t, exportID, className, allObjects)
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

	keys := listParquetKeys(t, exportID, className)
	require.NotEmpty(t, keys, "no parquet files found for class %s", className)

	var allRows []pqexport.ParquetRow
	for _, key := range keys {
		data := downloadS3Object(t, s3Bucket, key)
		rows := readParquetRows(t, data)
		allRows = append(allRows, rows...)
	}

	require.Len(t, allRows, len(expectedObjects), "parquet row count mismatch (across %d files)", len(keys))

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
		if row.Properties != nil {
			var props map[string]interface{}
			require.NoError(t, json.Unmarshal(row.Properties, &props))
			require.Equal(t, text, props["text"], "text property mismatch for object %s", row.ID)
		}

		delete(expectedIDs, row.ID)
	}

	require.Empty(t, expectedIDs, "some objects were not found in parquet export")
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

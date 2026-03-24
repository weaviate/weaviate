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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/exporttest"
	"github.com/weaviate/weaviate/usecases/byteops"
	pqexport "github.com/weaviate/weaviate/usecases/export"

	swaggerexport "github.com/weaviate/weaviate/client/export"
)

func TestExport_SingleShard(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := make([]*models.Object, 200)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("object %d", i),
			},
			Vector: models.C11yVector{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3},
		}
	}
	startedAt := time.Now()
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	// Concurrently start the export and import more objects.
	// These objects race with the snapshot; they MAY or MAY NOT appear.
	duringObjects := makeObjects(className, "", 100)
	exportCh := make(chan error, 1)
	go func() {
		_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
		exportCh <- err
	}()
	helper.CreateObjectsBatchCL(t, duringObjects, types.ConsistencyLevelAll)
	requireExportCreated(t, exportCh)

	// objects imported after export call completed MUST NOT appear in the export.
	var postObjects []*models.Object
	statusResp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	if statusResp.Payload.Status != "SUCCESS" {
		postObjects = makeObjects(className, "", 100)
		helper.CreateObjectsBatchCL(t, postObjects, types.ConsistencyLevelAll)
	} else {
		t.Log("export completed before post-import phase could begin")
	}

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	// Verify status response metadata (timestamps, classes, path, etc.)
	exporttest.VerifyStatusResponse(t, resp.Payload, "s3", exportID, []string{className}, startedAt)

	// Verify shard-level progress — during-objects may or may not be included
	require.NotNil(t, resp.Payload.ShardStatus)
	shardStatus := resp.Payload.ShardStatus[className]
	require.NotNil(t, shardStatus, "expected shard status for class %s", className)
	var totalExported int64
	for _, progress := range shardStatus {
		require.Equal(t, "SUCCESS", progress.Status)
		totalExported += progress.ObjectsExported
	}
	require.GreaterOrEqual(t, totalExported, int64(len(objects)),
		"total exported must include at least all pre-export objects")
	require.LessOrEqual(t, totalExported, int64(len(objects)+len(duringObjects)),
		"total exported must not exceed pre-export + during-export objects")

	verifyParquetMetadata(t, exportID, className, false)

	// Verify legacy (unnamed) vectors and row timestamps survive the parquet
	// round-trip (only for pre-export objects that are guaranteed present).
	allRows := fetchParquetRows(t, exportID, className)
	expectedVecs := make(map[string]models.C11yVector, len(objects))
	for _, obj := range objects {
		expectedVecs[string(obj.ID)] = obj.Vector
	}
	for _, row := range allRows {
		expVec, ok := expectedVecs[row.ID]
		if !ok {
			continue // during/post object — skip vector check
		}
		require.NotNil(t, row.Vector, "expected vector for object %s", row.ID)
		actualVec := byteops.Fp32SliceFromBytes(row.Vector)
		require.InDeltaSlice(t, []float32(expVec), actualVec, 1e-6,
			"vector mismatch for object %s", row.ID)
		verifyRowTimestamps(t, row, startedAt)
	}

	verifyConcurrentExport(t, exportID, className, len(objects), duringObjects, postObjects)
}

func TestExport_MultiShard(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(3),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 500)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	// Concurrently start the export and import more objects.
	duringObjects := makeObjects(className, "", 100)
	exportCh := make(chan error, 1)
	go func() {
		_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
		exportCh <- err
	}()
	helper.CreateObjectsBatchCL(t, duringObjects, types.ConsistencyLevelAll)
	requireExportCreated(t, exportCh)

	// Import objects after the snapshot is complete.
	var postObjects []*models.Object
	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	if resp.Payload.Status != "SUCCESS" {
		postObjects = makeObjects(className, "", 100)
		helper.CreateObjectsBatchCL(t, postObjects, types.ConsistencyLevelAll)
	} else {
		t.Log("export completed before post-import phase could begin")
	}

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	verifyParquetMetadata(t, exportID, className, false)
	verifyConcurrentExport(t, exportID, className, len(objects), duringObjects, postObjects)
}

func TestExport_DeletedAndUpdatedObjects(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "textProp", DataType: []string{"text"}},
			{Name: "intProp", DataType: []string{"int"}},
			{Name: "numberProp", DataType: []string{"number"}},
			{Name: "boolProp", DataType: []string{"boolean"}},
			{Name: "dateProp", DataType: []string{"date"}},
			{Name: "textArrayProp", DataType: []string{"text[]"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	startedAt := time.Now()
	objects := make([]*models.Object, 20)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"textProp":      fmt.Sprintf("text-%d", i),
				"intProp":       float64(i * 100),
				"numberProp":    float64(i) * 1.5,
				"boolProp":      i%2 == 0,
				"dateProp":      "2026-01-15T12:00:00Z",
				"textArrayProp": []string{fmt.Sprintf("a-%d", i), fmt.Sprintf("b-%d", i)},
			},
			Vector: models.C11yVector{float32(i) * 0.1, float32(i) * 0.2},
		}
	}
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	// Delete every other object, update the rest with new values
	var surviving []*models.Object
	for i, obj := range objects {
		if i%2 == 0 {
			helper.DeleteObject(t, obj)
		} else {
			obj.Properties = map[string]interface{}{
				"textProp":      fmt.Sprintf("updated-%d", i),
				"intProp":       float64(i * 999),
				"numberProp":    float64(i) * 3.14,
				"boolProp":      i%3 == 0,
				"dateProp":      "2026-06-01T00:00:00Z",
				"textArrayProp": []string{fmt.Sprintf("x-%d", i)},
			}
			require.NoError(t, helper.UpdateObject(t, obj))
			surviving = append(surviving, obj)
		}
	}

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	// Only surviving objects should appear — no tombstones, no stale data.
	// Verify every property type survived the parquet round-trip with updated values.
	allRows := fetchParquetRows(t, exportID, className)
	require.Len(t, allRows, len(surviving))

	expected := make(map[string]map[string]interface{}, len(surviving))
	for _, obj := range surviving {
		expected[string(obj.ID)] = obj.Properties.(map[string]interface{})
	}

	for _, row := range allRows {
		exp, ok := expected[row.ID]
		require.True(t, ok, "unexpected object ID: %s", row.ID)

		var props map[string]interface{}
		require.NoError(t, json.Unmarshal(row.Properties, &props))

		assert.Equal(t, exp["textProp"], props["textProp"], "text mismatch for %s", row.ID)
		assert.InDelta(t, exp["intProp"], props["intProp"], 0.1, "int mismatch for %s", row.ID)
		assert.InDelta(t, exp["numberProp"], props["numberProp"], 1e-9, "number mismatch for %s", row.ID)
		assert.Equal(t, exp["boolProp"], props["boolProp"], "bool mismatch for %s", row.ID)
		assert.Equal(t, exp["dateProp"], props["dateProp"], "date mismatch for %s", row.ID)

		actualArr, ok := props["textArrayProp"].([]interface{})
		require.True(t, ok, "textArrayProp should be array for %s", row.ID)
		expectedArr := exp["textArrayProp"].([]string)
		require.Len(t, actualArr, len(expectedArr))
		for j, v := range actualArr {
			assert.Equal(t, expectedArr[j], v, "textArrayProp[%d] mismatch for %s", j, row.ID)
		}

		verifyRowTimestamps(t, row, startedAt)

		delete(expected, row.ID)
	}

	require.Empty(t, expected, "some objects were not found in parquet export")
}

func TestExport_EmptyCollection(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	// No objects inserted — export an empty collection
	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	// Should produce a parquet file with zero data rows
	verifyParquetExport(t, exportID, className, nil)
}

func TestExport_MultipleClasses(t *testing.T) {
	classNameA := sanitizeClassName(t.Name()) + "A"
	classNameB := sanitizeClassName(t.Name()) + "B"
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	for _, cls := range []string{classNameA, classNameB} {
		helper.CreateClass(t, &models.Class{
			Class: cls,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ReplicationConfig: &models.ReplicationConfig{
				AsyncEnabled: true,
				Factor:       3,
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
	}
	defer helper.DeleteClass(t, classNameA)
	defer helper.DeleteClass(t, classNameB)

	objectsA := makeObjects(classNameA, "", 15)
	helper.CreateObjectsBatchCL(t, objectsA, types.ConsistencyLevelAll)
	objectsB := makeObjects(classNameB, "", 10)
	helper.CreateObjectsBatchCL(t, objectsB, types.ConsistencyLevelAll)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{classNameA, classNameB})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	// Verify each class independently
	verifyParquetExport(t, exportID, classNameA, objectsA)
	verifyParquetExport(t, exportID, classNameB, objectsB)
}

func TestExport_MultiTenant_SingleShard(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig:  &models.ReplicationConfig{AsyncEnabled: true, Factor: 3},
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
		objects := makeObjects(className, tenant.Name, 100)
		helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)
		allObjects = append(allObjects, objects...)
	}

	// Concurrently start the export and import more objects per tenant.
	var duringObjects []*models.Object
	for _, tenant := range tenants {
		duringObjects = append(duringObjects, makeObjects(className, tenant.Name, 30)...)
	}
	exportCh := make(chan error, 1)
	go func() {
		_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
		exportCh <- err
	}()
	helper.CreateObjectsBatchCL(t, duringObjects, types.ConsistencyLevelAll)
	requireExportCreated(t, exportCh)

	// Import objects after the snapshot is complete.
	var postObjects []*models.Object
	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	if resp.Payload.Status != "SUCCESS" {
		for _, tenant := range tenants {
			postObjects = append(postObjects, makeObjects(className, tenant.Name, 30)...)
		}
		helper.CreateObjectsBatchCL(t, postObjects, types.ConsistencyLevelAll)
	} else {
		t.Log("export completed before post-import phase could begin")
	}

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)
	verifyConcurrentExport(t, exportID, className, len(allObjects), duringObjects, postObjects)
}

func TestExport_MultiTenant_MultiShard(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig:  &models.ReplicationConfig{AsyncEnabled: true, Factor: 3},
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
		helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)
		allObjects = append(allObjects, objects...)
	}

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	// Create a new tenant after the export has started. The shard assignment is
	// snapshotted at export creation time, so this tenant's data should NOT
	// appear in the export.
	helper.CreateTenants(t, className, []*models.Tenant{{Name: "tenantLate"}})
	lateObjects := makeObjects(className, "tenantLate", 10)
	helper.CreateObjectsBatchCL(t, lateObjects, types.ConsistencyLevelAll)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	// Only the original 50 objects should be exported — not the late tenant's 10
	verifyParquetExport(t, exportID, className, allObjects)
}

func TestExport_NamedVectorAndMultiVector(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
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
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

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

// TestExport_MultiTenant_ActiveAndInactive verifies that exports handle
// tenants in different activity states correctly:
//   - HOT tenants: exported from the live (loaded) bucket
//   - COLD tenants: exported directly from disk without loading the shard
//   - OFFLOADED tenants: skipped
//
// AutoTenantActivation has no effect on exports because cold tenants are
// snapshotted from disk without loading.
func TestExport_MultiTenant_ActiveAndInactive(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{AsyncEnabled: true, Factor: 3},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: false,
			AutoTenantCreation:   false,
		},
	})
	defer helper.DeleteClass(t, className)

	tenants := []*models.Tenant{
		{Name: "tenantA"},
		{Name: "tenantB"},
		{Name: "tenantC"},
		{Name: "tenantD"},
	}
	helper.CreateTenants(t, className, tenants)

	var exportableObjects []*models.Object
	for _, tenant := range tenants {
		objects := makeObjects(className, tenant.Name, 10)
		helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)
		// tenantD will be OFFLOADED and skipped; all others are exported.
		if tenant.Name != "tenantD" {
			exportableObjects = append(exportableObjects, objects...)
		}
	}

	// tenantB → COLD: exported from disk without loading.
	// tenantC → COLD: exported from disk without loading.
	// tenantD → OFFLOADED: skipped entirely.
	helper.UpdateTenants(t, className, []*models.Tenant{
		{Name: "tenantB", ActivityStatus: models.TenantActivityStatusCOLD},
		{Name: "tenantC", ActivityStatus: models.TenantActivityStatusCOLD},
		{Name: "tenantD", ActivityStatus: models.TenantActivityStatusOFFLOADED},
	})

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	resp, err := exporttest.ExportStatus(t, "s3", exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)

	// tenantA (HOT) + tenantB (COLD) + tenantC (COLD) = 30 objects.
	verifyParquetExport(t, exportID, className, exportableObjects)

	// tenantD (OFFLOADED) should be skipped.
	require.NotNil(t, resp.Payload.ShardStatus)
	shardStatus := resp.Payload.ShardStatus[className]
	require.NotNil(t, shardStatus, "expected shard status for class %s", className)

	progressD, ok := shardStatus["tenantD"]
	require.True(t, ok, "expected shard status for tenantD")
	assert.Equal(t, "SKIPPED", progressD.Status)
	assert.True(t,
		strings.Contains(progressD.SkipReason, "FROZEN") || strings.Contains(progressD.SkipReason, "FREEZING"),
		"expected FROZEN or FREEZING in skip reason for tenantD, got: %s", progressD.SkipReason)

	// Cold tenants must remain COLD — the export must not activate them.
	tenantsResp, err := helper.GetTenants(t, className)
	require.NoError(t, err)
	for _, tenant := range tenantsResp.Payload {
		if tenant.Name == "tenantB" || tenant.Name == "tenantC" {
			require.Equal(t, models.TenantActivityStatusCOLD, tenant.ActivityStatus,
				"tenant %s should still be COLD after export", tenant.Name)
		}
	}

	// Verify parquet metadata
	verifyParquetMetadata(t, exportID, className, true)
}

// verifyParquetMetadata checks that all parquet files for a class contain
// the "collection" metadata key, and optionally "tenant" for MT classes.
func verifyParquetMetadata(t *testing.T, exportID, className string, expectTenant bool) {
	t.Helper()

	keys := listParquetKeys(t, exportID, className)
	require.NotEmpty(t, keys, "no parquet files found for metadata check")

	for _, key := range keys {
		data := downloadS3Object(t, s3Bucket, key)

		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err, "failed to open parquet file %s", key)

		collection, ok := file.Lookup("collection")
		require.True(t, ok, "missing 'collection' metadata in %s", key)
		require.Equal(t, className, collection, "collection metadata mismatch in %s", key)

		if expectTenant {
			tenant, ok := file.Lookup("tenant")
			require.True(t, ok, "missing 'tenant' metadata in %s", key)
			require.NotEmpty(t, tenant, "empty 'tenant' metadata in %s", key)
		}
	}
}

// requireExportCreated waits for the CreateExport result from exportCh and
// fails the test with the full validation message if it is a 422 error.
func requireExportCreated(t *testing.T, exportCh <-chan error) {
	t.Helper()
	if err := <-exportCh; err != nil {
		var ue *swaggerexport.ExportCreateUnprocessableEntity
		if errors.As(err, &ue) && ue.Payload != nil {
			for _, e := range ue.Payload.Error {
				t.Logf("  validation error: %s", e.Message)
			}
		}
		t.Fatalf("CreateExport failed: %v", err)
	}
}

// sanitizeClassName replaces characters that are invalid in Weaviate class
// names (e.g. the '/' inserted by t.Name() for subtests) with underscores.
func sanitizeClassName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
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
// verifyRowTimestamps checks that CreationTime and UpdateTime are within
// [earliest, now] and that UpdateTime >= CreationTime.
func verifyRowTimestamps(t *testing.T, row pqexport.ParquetRow, earliest time.Time) {
	t.Helper()
	// Allow 2s tolerance for clock skew between test process and Weaviate containers.
	lower := earliest.Add(-2 * time.Second)
	now := time.Now()
	ct := time.UnixMilli(row.CreationTime)
	ut := time.UnixMilli(row.UpdateTime)
	require.WithinRange(t, ct, lower, now,
		"creation_time out of range for object %s", row.ID)
	require.WithinRange(t, ut, ct, now,
		"update_time out of range for object %s", row.ID)
}

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

func TestExport_Validation(t *testing.T) {
	// Create a class that several subtests will reference.
	className := sanitizeClassName(t.Name())
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 5)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	t.Run("invalid export ID format", func(t *testing.T) {
		fileFormat := models.ExportCreateRequestFileFormatParquet
		for _, badID := range []string{"HAS-UPPERCASE", "has spaces", "has.dots", "has/slash", "has@symbol"} {
			_, err := exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
				ID:         &badID,
				Include:    []string{className},
				FileFormat: &fileFormat,
			})
			require.Error(t, err, "expected error for ID %q", badID)
			require.Contains(t, err.Error(), "422",
				"invalid export ID should be rejected with 422, got: %v", err)
		}
	})

	t.Run("both include and exclude", func(t *testing.T) {
		exportID := strings.ToLower(sanitizeClassName(t.Name()))
		fileFormat := models.ExportCreateRequestFileFormatParquet
		_, err := exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
			ID:         &exportID,
			Include:    []string{className},
			Exclude:    []string{"SomeOtherClass"},
			FileFormat: &fileFormat,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "422",
			"specifying both include and exclude should be rejected with 422, got: %v", err)
	})

	t.Run("non-existent class in include list", func(t *testing.T) {
		exportID := strings.ToLower(sanitizeClassName(t.Name()))
		fileFormat := models.ExportCreateRequestFileFormatParquet
		// Only non-existent classes → "no exportable classes" → 422
		_, err := exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
			ID:         &exportID,
			Include:    []string{"ClassThatDoesNotExist"},
			FileFormat: &fileFormat,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "422",
			"include with only non-existent classes should be rejected with 422, got: %v", err)
	})

	t.Run("empty include exports all classes", func(t *testing.T) {
		exportID := strings.ToLower(sanitizeClassName(t.Name()))
		// nil include → export all classes in the instance
		_, err := exporttest.CreateExport(t, "s3", exportID, nil)
		require.NoError(t, err)

		exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

		// Verify the class we created is in the export
		verifyParquetExport(t, exportID, className, objects)
	})

	t.Run("status of non-existent export", func(t *testing.T) {
		_, err := exporttest.ExportStatus(t, "s3", "does-not-exist-at-all")
		require.Error(t, err)
		require.Contains(t, err.Error(), "404",
			"status of non-existent export should return 404, got: %v", err)
	})
}

func TestExport_ExcludeFilter(t *testing.T) {
	includedClass := sanitizeClassName(t.Name()) + "Included"
	excludedClass := sanitizeClassName(t.Name()) + "Excluded"

	for _, cls := range []string{includedClass, excludedClass} {
		helper.CreateClass(t, &models.Class{
			Class: cls,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ReplicationConfig: &models.ReplicationConfig{
				AsyncEnabled: true,
				Factor:       3,
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
	}
	defer helper.DeleteClass(t, includedClass)
	defer helper.DeleteClass(t, excludedClass)

	includedObjects := makeObjects(includedClass, "", 10)
	helper.CreateObjectsBatchCL(t, includedObjects, types.ConsistencyLevelAll)
	excludedObjects := makeObjects(excludedClass, "", 10)
	helper.CreateObjectsBatchCL(t, excludedObjects, types.ConsistencyLevelAll)

	t.Run("exclude one class", func(t *testing.T) {
		exportID := strings.ToLower(sanitizeClassName(t.Name()))

		fileFormat := models.ExportCreateRequestFileFormatParquet
		_, err := exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
			ID:         &exportID,
			Exclude:    []string{excludedClass},
			FileFormat: &fileFormat,
		})
		require.NoError(t, err)

		exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

		verifyParquetExport(t, exportID, includedClass, includedObjects)

		excludedKeys := listParquetKeys(t, exportID, excludedClass)
		require.Empty(t, excludedKeys, "excluded class should have no parquet files")
	})

	t.Run("non-existent class in exclude is silently ignored", func(t *testing.T) {
		exportID := strings.ToLower(sanitizeClassName(t.Name()))

		fileFormat := models.ExportCreateRequestFileFormatParquet
		_, err := exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
			ID:         &exportID,
			Exclude:    []string{"ClassThatDoesNotExist"},
			FileFormat: &fileFormat,
		})
		require.NoError(t, err)

		exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

		// Both classes should be exported since the excluded class doesn't exist
		verifyParquetExport(t, exportID, includedClass, includedObjects)
		verifyParquetExport(t, exportID, excludedClass, excludedObjects)
	})
}

func TestExport_CustomConfig(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))
	customBucket := "custom-export-bucket"

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 10)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	// Create a custom bucket in MinIO
	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(customBucket),
	})
	require.NoError(t, err)

	// Export with both a custom bucket and a custom path prefix
	fileFormat := models.ExportCreateRequestFileFormatParquet
	_, err = exporttest.CreateExportRaw(t, "s3", &models.ExportCreateRequest{
		ID:         &exportID,
		Include:    []string{className},
		FileFormat: &fileFormat,
		Config: &models.ExportCreateRequestConfig{
			Bucket: customBucket,
			Path:   "custom/prefix",
		},
	})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceededWithConfig(t, "s3", exportID, customBucket, "custom/prefix")

	// Verify files landed in the custom bucket under the custom prefix
	prefix := fmt.Sprintf("custom/prefix/%s/%s", exportID, className)
	listResp, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(customBucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err)

	var keys []string
	for _, obj := range listResp.Contents {
		key := aws.ToString(obj.Key)
		if strings.HasSuffix(key, ".parquet") {
			keys = append(keys, key)
		}
	}
	require.NotEmpty(t, keys, "expected parquet files under %s in bucket %s", prefix, customBucket)

	// Default bucket should have no files for this export
	defaultResp, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(fmt.Sprintf("%s/%s", exportID, className)),
	})
	require.NoError(t, err)
	require.Empty(t, defaultResp.Contents, "default bucket should have no files for this export")
}

func TestExport_DuplicateID(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 5)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	// First export — should succeed
	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	// Second export with the same ID — should get 409
	_, err = exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.Error(t, err)
	require.Contains(t, err.Error(), "409",
		"reusing an export ID should be rejected with 409, got: %v", err)
}

func TestExport_ConcurrentExport(t *testing.T) {
	classNameA := sanitizeClassName(t.Name()) + "A"
	classNameB := sanitizeClassName(t.Name()) + "B"
	exportIDA := strings.ToLower(sanitizeClassName(t.Name())) + "a"
	exportIDB := strings.ToLower(sanitizeClassName(t.Name())) + "b"

	for _, cls := range []string{classNameA, classNameB} {
		helper.CreateClass(t, &models.Class{
			Class: cls,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ReplicationConfig: &models.ReplicationConfig{
				AsyncEnabled: true,
				Factor:       3,
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
	}
	defer helper.DeleteClass(t, classNameA)
	defer helper.DeleteClass(t, classNameB)

	// Insert enough objects so the first export takes a while
	objectsA := makeObjects(classNameA, "", 200)
	helper.CreateObjectsBatchCL(t, objectsA, types.ConsistencyLevelAll)
	objectsB := makeObjects(classNameB, "", 5)
	helper.CreateObjectsBatchCL(t, objectsB, types.ConsistencyLevelAll)

	// Start first export
	_, err := exporttest.CreateExport(t, "s3", exportIDA, []string{classNameA})
	require.NoError(t, err)

	// Immediately start a second export — should get 409 (already active)
	_, err = exporttest.CreateExport(t, "s3", exportIDB, []string{classNameB})
	if err != nil {
		require.Contains(t, err.Error(), "409",
			"concurrent export should be rejected with 409, got: %v", err)
	} else {
		// If the first export finished fast enough, both could succeed.
		// Verify both reach a terminal state.
		exporttest.ExpectExportEventuallySucceeded(t, "s3", exportIDB)
	}

	// Make sure the first export still completes
	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportIDA)
	verifyParquetExport(t, exportIDA, classNameA, objectsA)
}

func TestExport_Cancel(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		className := sanitizeClassName(t.Name())
		exportID := strings.ToLower(className)

		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ReplicationConfig: &models.ReplicationConfig{
				AsyncEnabled: true,
				Factor:       3,
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
		defer helper.DeleteClass(t, className)

		// Create enough objects to make the export take some time
		objects := makeObjects(className, "", 200)
		helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

		_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
		require.NoError(t, err)

		// Immediately try to cancel
		_, cancelErr := exporttest.CancelExport(t, "s3", exportID)
		if cancelErr != nil {
			// 409 means the export finished before we could cancel — that's OK
			require.Contains(t, cancelErr.Error(), "409",
				"expected either success or 409, got: %v", cancelErr)
			// Verify it actually succeeded
			exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)
		} else {
			// Cancel succeeded — verify it reaches CANCELED status
			exporttest.ExpectExportEventuallyCanceled(t, "s3", exportID)
		}
	})

	t.Run("re_export_after_cancel", func(t *testing.T) {
		className := sanitizeClassName(t.Name())
		firstID := strings.ToLower(className) + "-first"
		secondID := strings.ToLower(className) + "-second"

		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ReplicationConfig: &models.ReplicationConfig{
				AsyncEnabled: true,
				Factor:       3,
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
		defer helper.DeleteClass(t, className)

		objects := makeObjects(className, "", 200)
		helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

		// Start and cancel the first export
		_, err := exporttest.CreateExport(t, "s3", firstID, []string{className})
		require.NoError(t, err)

		_, cancelErr := exporttest.CancelExport(t, "s3", firstID)
		if cancelErr != nil {
			require.Contains(t, cancelErr.Error(), "409")
		} else {
			exporttest.ExpectExportEventuallyCanceled(t, "s3", firstID)
		}

		// Re-export the same class with a different ID — no leftover state should interfere
		_, err = exporttest.CreateExport(t, "s3", secondID, []string{className})
		require.NoError(t, err)

		exporttest.ExpectExportEventuallySucceeded(t, "s3", secondID)
		verifyParquetExport(t, secondID, className, objects)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := exporttest.CancelExport(t, "s3", "nonexistent-export-id")
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})
}

func TestExport_Cancel_AlreadyFinished(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			AsyncEnabled: true,
			Factor:       3,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 5)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.NoError(t, err)

	// Wait for the export to finish
	exporttest.ExpectExportEventuallySucceeded(t, "s3", exportID)

	// Now try to cancel — should get 409
	_, cancelErr := exporttest.CancelExport(t, "s3", exportID)
	require.Error(t, cancelErr)
	require.Contains(t, cancelErr.Error(), "409")
}

func TestExport_RejectsWithoutAsyncReplication(t *testing.T) {
	className := sanitizeClassName(t.Name())
	exportID := strings.ToLower(sanitizeClassName(t.Name()))

	// Create a class with RF > 1 but async replication disabled.
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor:       3,
			AsyncEnabled: false,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(1),
		},
	})
	defer helper.DeleteClass(t, className)

	objects := makeObjects(className, "", 5)
	helper.CreateObjectsBatchCL(t, objects, types.ConsistencyLevelAll)

	_, err := exporttest.CreateExport(t, "s3", exportID, []string{className})
	require.Error(t, err)
	require.Contains(t, err.Error(), "422",
		"export without async replication should be rejected with 422, got: %v", err)
}

// verifyConcurrentExport checks the point-in-time semantics of an export
// that ran concurrently with object imports:
//   - duringObjects MAY or may not be present (race with snapshot)
//   - No postObjects may be in the export
//   - Total row count is bounded by [preCount, preCount+duringCount]
func verifyConcurrentExport(
	t *testing.T,
	exportID, className string,
	preCount int,
	duringObjects, postObjects []*models.Object,
) {
	t.Helper()

	allRows := fetchParquetRows(t, exportID, className)
	exportedIDs := make(map[string]struct{}, len(allRows))
	for _, row := range allRows {
		exportedIDs[row.ID] = struct{}{}
	}

	var duringPresent int
	for _, obj := range duringObjects {
		if _, ok := exportedIDs[string(obj.ID)]; ok {
			duringPresent++
		}
	}
	t.Logf("during-export objects present: %d/%d", duringPresent, len(duringObjects))

	for _, obj := range postObjects {
		assert.NotContains(t, exportedIDs, string(obj.ID),
			"post-export object %s must NOT be in export", obj.ID)
	}

	assert.GreaterOrEqual(t, len(allRows), preCount,
		"export must contain at least all pre-export objects")
	assert.LessOrEqual(t, len(allRows), preCount+len(duringObjects),
		"export must not contain more than pre+during objects")
}

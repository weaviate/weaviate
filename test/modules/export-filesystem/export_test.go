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
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/exporttest"
)

const backend = "filesystem"

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

	objects := makeObjects(className, 20)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, backend, exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)

	resp, err := exporttest.ExportStatus(t, backend, exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)
}

func TestExport_DeletedAndUpdatedObjects(t *testing.T) {
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

	objects := makeObjects(className, 20)
	helper.CreateObjectsBatch(t, objects)

	// Delete every other object, update the rest with new text
	for i, obj := range objects {
		if i%2 == 0 {
			helper.DeleteObject(t, obj)
		} else {
			obj.Properties = map[string]interface{}{
				"text": fmt.Sprintf("updated-%d", i),
			}
			require.NoError(t, helper.UpdateObject(t, obj))
		}
	}

	_, err := exporttest.CreateExport(t, backend, exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)

	resp, err := exporttest.ExportStatus(t, backend, exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)
}

func TestExport_MultipleClasses(t *testing.T) {
	classNameA := t.Name() + "A"
	classNameB := t.Name() + "B"
	exportID := strings.ToLower(t.Name())

	for _, cls := range []string{classNameA, classNameB} {
		helper.CreateClass(t, &models.Class{
			Class: cls,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
	}
	defer helper.DeleteClass(t, classNameA)
	defer helper.DeleteClass(t, classNameB)

	objectsA := makeObjects(classNameA, 15)
	helper.CreateObjectsBatch(t, objectsA)
	objectsB := makeObjects(classNameB, 10)
	helper.CreateObjectsBatch(t, objectsB)

	_, err := exporttest.CreateExport(t, backend, exportID, []string{classNameA, classNameB})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)

	resp, err := exporttest.ExportStatus(t, backend, exportID)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Payload.Status)
}

func TestExport_DuplicateID(t *testing.T) {
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

	objects := makeObjects(className, 5)
	helper.CreateObjectsBatch(t, objects)

	// First export — should succeed
	_, err := exporttest.CreateExport(t, backend, exportID, []string{className})
	require.NoError(t, err)

	exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)

	// Second export with the same ID — should get 409
	_, err = exporttest.CreateExport(t, backend, exportID, []string{className})
	require.Error(t, err)
	require.Contains(t, err.Error(), "409",
		"reusing an export ID should be rejected with 409, got: %v", err)
}

func TestExport_ConcurrentExport(t *testing.T) {
	classNameA := t.Name() + "A"
	classNameB := t.Name() + "B"
	exportIDA := strings.ToLower(t.Name()) + "a"
	exportIDB := strings.ToLower(t.Name()) + "b"

	for _, cls := range []string{classNameA, classNameB} {
		helper.CreateClass(t, &models.Class{
			Class: cls,
			Properties: []*models.Property{
				{Name: "text", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": float64(1),
			},
		})
	}
	defer helper.DeleteClass(t, classNameA)
	defer helper.DeleteClass(t, classNameB)

	// Insert enough objects so the first export takes a while
	objectsA := makeObjects(classNameA, 200)
	helper.CreateObjectsBatch(t, objectsA)
	objectsB := makeObjects(classNameB, 5)
	helper.CreateObjectsBatch(t, objectsB)

	// Start first export
	_, err := exporttest.CreateExport(t, backend, exportIDA, []string{classNameA})
	require.NoError(t, err)

	// Immediately start a second export — should get 409 (already active)
	_, err = exporttest.CreateExport(t, backend, exportIDB, []string{classNameB})
	if err != nil {
		require.Contains(t, err.Error(), "409",
			"concurrent export should be rejected with 409, got: %v", err)
	} else {
		// If the first export finished fast enough, both could succeed.
		// Verify both reach a terminal state.
		exporttest.ExpectExportEventuallySucceeded(t, backend, exportIDB)
	}

	// Make sure the first export still completes
	exporttest.ExpectExportEventuallySucceeded(t, backend, exportIDA)
}

func TestExport_Cancel_Running(t *testing.T) {
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

	// Create enough objects to make the export take some time
	objects := makeObjects(className, 200)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, backend, exportID, []string{className})
	require.NoError(t, err)

	// Immediately try to cancel
	_, cancelErr := exporttest.CancelExport(t, backend, exportID)
	if cancelErr != nil {
		// 409 means the export finished before we could cancel — that's OK
		require.Contains(t, cancelErr.Error(), "409",
			"expected either success or 409, got: %v", cancelErr)
		// Verify it actually succeeded
		exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)
	} else {
		// Cancel succeeded — verify it reaches CANCELED status
		exporttest.ExpectExportEventuallyCanceled(t, backend, exportID)
	}
}

func TestExport_Cancel_NotFound(t *testing.T) {
	_, err := exporttest.CancelExport(t, backend, "nonexistent-export-id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "404")
}

func TestExport_Cancel_AlreadyFinished(t *testing.T) {
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

	objects := makeObjects(className, 5)
	helper.CreateObjectsBatch(t, objects)

	_, err := exporttest.CreateExport(t, backend, exportID, []string{className})
	require.NoError(t, err)

	// Wait for the export to finish
	exporttest.ExpectExportEventuallySucceeded(t, backend, exportID)

	// Now try to cancel — should get 409
	_, cancelErr := exporttest.CancelExport(t, backend, exportID)
	require.Error(t, cancelErr)
	require.Contains(t, cancelErr.Error(), "409")
}

func makeObjects(className string, count int) []*models.Object {
	objects := make([]*models.Object, count)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("object %d", i),
			},
		}
	}
	return objects
}

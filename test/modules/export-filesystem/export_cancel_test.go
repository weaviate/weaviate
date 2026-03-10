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

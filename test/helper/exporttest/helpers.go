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

package exporttest

import (
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func CreateExport(t *testing.T, backend, exportID string, include []string) (*export.ExportCreateOK, error) {
	t.Helper()
	return CreateExportWithAuth(t, backend, exportID, include, nil)
}

func CreateExportWithAuth(t *testing.T, backend, exportID string, include []string, auth runtime.ClientAuthInfoWriter) (*export.ExportCreateOK, error) {
	t.Helper()
	fileFormat := models.ExportCreateRequestFileFormatParquet
	params := export.NewExportCreateParams().
		WithBackend(backend).
		WithBody(&models.ExportCreateRequest{
			ID:         &exportID,
			Include:    include,
			FileFormat: &fileFormat,
		})
	t.Logf("Creating export with ID: %s, backend: %s, include: %v", exportID, backend, include)
	return helper.Client(t).Export.ExportCreate(params, auth)
}

// CreateExportRaw creates an export using a raw ExportCreateRequest body,
// allowing tests to set arbitrary combinations of fields (e.g. include+exclude).
func CreateExportRaw(t *testing.T, backend string, body *models.ExportCreateRequest) (*export.ExportCreateOK, error) {
	t.Helper()
	params := export.NewExportCreateParams().
		WithBackend(backend).
		WithBody(body)
	return helper.Client(t).Export.ExportCreate(params, nil)
}

func ExportStatus(t *testing.T, backend, exportID string) (*export.ExportStatusOK, error) {
	t.Helper()
	return ExportStatusWithConfig(t, backend, exportID, "", "")
}

func ExportStatusWithConfig(t *testing.T, backend, exportID, bucket, path string) (*export.ExportStatusOK, error) {
	t.Helper()
	params := export.NewExportStatusParams().
		WithBackend(backend).
		WithID(exportID)
	if bucket != "" {
		params = params.WithBucket(&bucket)
	}
	if path != "" {
		params = params.WithPath(&path)
	}
	return helper.Client(t).Export.ExportStatus(params, nil)
}

func ExportStatusWithAuth(t *testing.T, backend, exportID string, auth runtime.ClientAuthInfoWriter) (*export.ExportStatusOK, error) {
	t.Helper()
	params := export.NewExportStatusParams().
		WithBackend(backend).
		WithID(exportID)
	return helper.Client(t).Export.ExportStatus(params, auth)
}

func CancelExport(t *testing.T, backend, exportID string) (*export.ExportCancelNoContent, error) {
	t.Helper()
	return CancelExportWithAuth(t, backend, exportID, nil)
}

func CancelExportWithAuth(t *testing.T, backend, exportID string, auth runtime.ClientAuthInfoWriter) (*export.ExportCancelNoContent, error) {
	t.Helper()
	params := export.NewExportCancelParams().
		WithBackend(backend).
		WithID(exportID)
	t.Logf("Cancelling export with ID: %s, backend: %s", exportID, backend)
	return helper.Client(t).Export.ExportCancel(params, auth)
}

// VerifyStatusResponse checks that a successful export status response has
// reasonable metadata: timestamps, duration, ID, backend, path, and classes.
// calledAt should be captured just before the CreateExport call.
func VerifyStatusResponse(t *testing.T, resp *models.ExportStatusResponse, backend, exportID string, expectedClasses []string, calledAt time.Time) {
	t.Helper()

	now := time.Now()

	assert.Equal(t, exportID, resp.ID, "ID mismatch")
	assert.Equal(t, backend, resp.Backend, "backend mismatch")
	assert.NotEmpty(t, resp.Path, "path should not be empty")

	// Classes should match (order-independent)
	gotClasses := make([]string, len(resp.Classes))
	copy(gotClasses, resp.Classes)
	wantClasses := make([]string, len(expectedClasses))
	copy(wantClasses, expectedClasses)
	sort.Strings(gotClasses)
	sort.Strings(wantClasses)
	assert.Equal(t, wantClasses, gotClasses, "classes mismatch")

	// Timestamps: calledAt <= startedAt <= completedAt <= now
	startedAt := time.Time(resp.StartedAt)
	completedAt := time.Time(resp.CompletedAt)
	assert.WithinRange(t, startedAt, calledAt, now, "startedAt out of range")
	assert.WithinRange(t, completedAt, startedAt, now, "completedAt out of range")
	assert.InDelta(t, completedAt.Sub(startedAt).Milliseconds(), resp.TookInMs, 1, "tookInMs should equal completedAt - startedAt")
}

func ExpectExportEventuallySucceeded(t *testing.T, backend, exportID string) {
	t.Helper()
	ExpectExportEventuallySucceededWithConfig(t, backend, exportID, "", "")
}

func ExpectExportEventuallySucceededWithConfig(t *testing.T, backend, exportID, bucket, path string) {
	t.Helper()

	deadline := 60 * time.Second
	interval := 500 * time.Millisecond

	require.EventuallyWithTf(t, func(c *assert.CollectT) {
		resp, err := ExportStatusWithConfig(t, backend, exportID, bucket, path)
		require.NoError(c, err, "fetch export status")
		require.NotNil(c, resp.Payload, "empty response")

		status := resp.Payload.Status
		require.Equalf(c, "SUCCESS", status, "export status (error: %s)", resp.Payload.Error)
	}, deadline, interval, "export %s not succeeded after %s", exportID, deadline)
}

func ExpectExportEventuallyCanceled(t *testing.T, backend, exportID string) {
	t.Helper()

	deadline := 60 * time.Second
	interval := 500 * time.Millisecond

	require.EventuallyWithTf(t, func(c *assert.CollectT) {
		resp, err := ExportStatus(t, backend, exportID)
		require.NoError(c, err, "fetch export status")
		require.NotNil(c, resp.Payload, "empty response")

		status := resp.Payload.Status
		require.Equalf(c, "CANCELED", status, "export status (error: %s)", resp.Payload.Error)
	}, deadline, interval, "export %s not canceled after %s", exportID, deadline)
}

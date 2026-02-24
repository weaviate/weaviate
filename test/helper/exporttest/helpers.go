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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/exports"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func CreateExport(t *testing.T, backend, exportID string, include []string) (*exports.ExportsCreateOK, error) {
	t.Helper()
	params := exports.NewExportsCreateParams().
		WithBackend(backend).
		WithBody(&models.ExportCreateRequest{
			ID:      &exportID,
			Include: include,
		})
	t.Logf("Creating export with ID: %s, backend: %s, include: %v", exportID, backend, include)
	return helper.Client(t).Exports.ExportsCreate(params, nil)
}

func ExportStatus(t *testing.T, backend, exportID string) (*exports.ExportsStatusOK, error) {
	t.Helper()
	params := exports.NewExportsStatusParams().
		WithBackend(backend).
		WithID(exportID)
	return helper.Client(t).Exports.ExportsStatus(params, nil)
}

func ExpectExportEventuallySucceeded(t *testing.T, backend, exportID string) {
	t.Helper()

	deadline := 60 * time.Second
	interval := 500 * time.Millisecond

	require.EventuallyWithTf(t, func(c *assert.CollectT) {
		resp, err := ExportStatus(t, backend, exportID)
		require.NoError(c, err, "fetch export status")
		require.NotNil(c, resp.Payload, "empty response")

		status := resp.Payload.Status
		require.Equalf(c, "SUCCESS", status, "export status (error: %s)", resp.Payload.Error)
	}, deadline, interval, "export %s not succeeded after %s", exportID, deadline)
}

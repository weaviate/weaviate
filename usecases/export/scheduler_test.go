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

package export

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestScheduler_StatusFallsBackToMetadataWhenPlanMissing(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// Write metadata with FAILED status but no plan file — simulates a
	// multi-node export that failed before/during plan writing.
	meta := &ExportMetadata{
		ID:          "test-export",
		Backend:     "s3",
		StartedAt:   time.Now().UTC().Add(-10 * time.Second),
		CompletedAt: time.Now().UTC(),
		Status:      export.Failed,
		Classes:     []string{"TestClass"},
		Error:       "failed to write export plan: connection reset",
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "test-export", exportMetadataFile, "", "", newBytesReadCloser(data))
	require.NoError(t, err)

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		backends:     &fakeBackendProvider{backend: backend},
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
	}

	status, err := s.Status(context.Background(), nil, "s3", "test-export", "", "")
	require.NoError(t, err)

	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "failed to write export plan")
}

func TestScheduler_StatusFallsBackToMetadataAndForcesFailed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// Write metadata with a non-failed status but no plan file — this should
	// never happen in practice, but Status() must still report FAILED because
	// the plan is missing.
	meta := &ExportMetadata{
		ID:          "test-export",
		Backend:     "s3",
		StartedAt:   time.Now().UTC().Add(-10 * time.Second),
		CompletedAt: time.Now().UTC(),
		Status:      export.Success,
		Classes:     []string{"TestClass"},
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "test-export", exportMetadataFile, "", "", newBytesReadCloser(data))
	require.NoError(t, err)

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		backends:     &fakeBackendProvider{backend: backend},
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
	}

	status, err := s.Status(context.Background(), nil, "s3", "test-export", "", "")
	require.NoError(t, err)

	// Even though metadata says Success, missing plan forces FAILED
	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "export plan not found")
}

func TestScheduler_StatusReturnsNotFoundWhenNothingExists(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		backends:     &fakeBackendProvider{backend: backend},
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
	}

	_, err := s.Status(context.Background(), nil, "s3", "test-export", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// errorBackend embeds fakeBackend but overrides GetObject to always return
// a non-not-found error, simulating transient backend failures.
type errorBackend struct {
	fakeBackend
	err error
}

func (b *errorBackend) GetObject(context.Context, string, string, string, string) ([]byte, error) {
	return nil, b.err
}

func TestScheduler_ResolveClasses(t *testing.T) {
	selector := &emptySelector{classList: []string{"Article", "Product", "Author"}}

	s := &Scheduler{selector: selector}
	ctx := context.Background()

	t.Run("include valid", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, []string{"Article", "Product"}, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"Article", "Product"}, classes)
	})

	t.Run("include nonexistent silently ignored", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, []string{"DoesNotExist"}, nil)
		require.NoError(t, err)
		assert.Empty(t, classes)
	})

	t.Run("include mix of existent and nonexistent", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, []string{"Article", "DoesNotExist"}, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"Article"}, classes)
	})

	t.Run("exclude valid", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, nil, []string{"Product"})
		require.NoError(t, err)
		assert.Equal(t, []string{"Article", "Author"}, classes)
	})

	t.Run("exclude nonexistent silently ignored", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, nil, []string{"DoesNotExist"})
		require.NoError(t, err)
		assert.Equal(t, []string{"Article", "Product", "Author"}, classes)
	})

	t.Run("both include and exclude", func(t *testing.T) {
		_, err := s.resolveClasses(ctx, []string{"Article"}, []string{"Product"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot specify both")
	})

	t.Run("neither returns all", func(t *testing.T) {
		classes, err := s.resolveClasses(ctx, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"Article", "Product", "Author"}, classes)
	})
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

func TestScheduler_StatusPropagatesBackendError(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &errorBackend{err: fmt.Errorf("connection refused")}

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
	assert.Contains(t, err.Error(), "connection refused")
	assert.NotContains(t, err.Error(), "not found")
}

func TestScheduler_CancelPropagatesBackendError(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &errorBackend{err: fmt.Errorf("permission denied")}

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		backends:     &fakeBackendProvider{backend: backend},
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
	}

	err := s.Cancel(context.Background(), nil, "s3", "test-export", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
	assert.NotErrorIs(t, err, ErrExportNotFound)
}

func TestScheduler_CancelReturnsNotFoundWhenMetadataMissing(t *testing.T) {
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

	err := s.Cancel(context.Background(), nil, "s3", "test-export", "", "")
	require.ErrorIs(t, err, ErrExportNotFound)
}

// TestScheduler_CancelReturnsAlreadyFinishedWhenAllNodesFailed verifies that
// Cancel() returns ErrExportAlreadyFinished when all nodes have genuinely
// reported a terminal (Failed) status, rather than overwriting the FAILED
// status with CANCELED metadata.
func TestScheduler_CancelReturnsAlreadyFinishedWhenAllNodesFailed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{
		written: map[string][]byte{},
	}

	// Store initial metadata so Cancel() can find it.
	initialMeta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Status:  export.Started,
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
			"node2": {"TestClass": {"shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}
	metaData, err := json.Marshal(initialMeta)
	require.NoError(t, err)
	backend.written[exportMetadataFile] = metaData

	// Both nodes reported terminal Failed status.
	for _, nodeName := range []string{"node1", "node2"} {
		ns := &NodeStatus{
			NodeName: nodeName,
			Status:   export.Failed,
			Error:    "disk full",
		}
		data, err := json.Marshal(ns)
		require.NoError(t, err)
		backend.written[fmt.Sprintf("node_%s_status.json", nodeName)] = data
	}

	s := &Scheduler{
		shutdownCtx: context.Background(),
		logger:      logger,
		authorizer:  mocks.NewMockAuthorizer(),
		backends:    &fakeBackendProvider{backend: backend},
		client:      &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{
			"node1": "host1:8080",
			"node2": "host2:8080",
		}},
	}

	err = s.Cancel(context.Background(), nil, "s3", "test-export", "", "")
	require.ErrorIs(t, err, ErrExportAlreadyFinished)

	// Verify the status is still FAILED, not overwritten with CANCELED.
	resp, err := s.Status(context.Background(), nil, "s3", "test-export", "", "")
	require.NoError(t, err)
	assert.Equal(t, string(export.Failed), resp.Status)
}

func TestScheduler_StatusPromotesTerminalMetadata(t *testing.T) {
	for _, tc := range []struct {
		name           string
		nodeStatus     export.Status
		expectedStatus export.Status
	}{
		{"success", export.Success, export.Success},
		{"failed", export.Failed, export.Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			backend := &fakeBackend{}

			// Write STARTED metadata with NodeAssignments — the stale state we want promoted.
			startedMeta := &ExportMetadata{
				ID:      "test-export",
				Backend: "s3",
				Status:  export.Started,
				Classes: []string{"TestClass"},
				NodeAssignments: map[string]map[string][]string{
					"node1": {"TestClass": {"shard0"}},
				},
				StartedAt: time.Now().UTC().Add(-10 * time.Second),
			}
			metaData, err := json.Marshal(startedMeta)
			require.NoError(t, err)
			_, err = backend.Write(context.Background(), "test-export", exportMetadataFile, "", "", newBytesReadCloser(metaData))
			require.NoError(t, err)

			// Write node status showing terminal state.
			ns := &NodeStatus{
				NodeName:    "node1",
				Status:      tc.nodeStatus,
				CompletedAt: time.Now().UTC(),
				ShardProgress: map[string]map[string]*ShardProgress{
					"TestClass": {
						"shard0": {Status: export.ShardSuccess, ObjectsExported: 100},
					},
				},
			}
			if tc.nodeStatus == export.Failed {
				ns.Error = "disk full"
				ns.ShardProgress["TestClass"]["shard0"].Status = export.ShardFailed
			}
			nsData, err := json.Marshal(ns)
			require.NoError(t, err)
			_, err = backend.Write(context.Background(), "test-export", "node_node1_status.json", "", "", newBytesReadCloser(nsData))
			require.NoError(t, err)

			resolver := &fakeNodeResolver{nodes: map[string]string{"node1": "host1:8080"}}
			client := &fakeExportClient{
				isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
					return false, nil
				},
			}

			s := &Scheduler{
				shutdownCtx:  context.Background(),
				logger:       logger,
				authorizer:   mocks.NewMockAuthorizer(),
				backends:     &fakeBackendProvider{backend: backend},
				client:       client,
				nodeResolver: resolver,
			}

			status, err := s.Status(context.Background(), nil, "s3", "test-export", "", "")
			require.NoError(t, err)
			assert.Equal(t, string(tc.expectedStatus), status.Status)

			// Verify that terminal metadata was promoted (written to backend).
			written := backend.getWritten(exportMetadataFile)
			require.NotNil(t, written, "expected metadata to be promoted")

			var meta ExportMetadata
			require.NoError(t, json.Unmarshal(written, &meta))
			assert.Equal(t, tc.expectedStatus, meta.Status)
		})
	}
}

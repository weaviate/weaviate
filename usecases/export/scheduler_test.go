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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
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

// failInitBackend embeds fakeBackend but overrides Initialize to return an error.
type failInitBackend struct {
	fakeBackend
	err error
}

func (b *failInitBackend) Initialize(context.Context, string, string, string) error {
	return b.err
}

func TestScheduler_ResolveClasses(t *testing.T) {
	selector := &fakeSelector{classList: []string{"Article", "Product", "Author"}}

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

func TestScheduler_ExportIDValidation(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		backends:     &fakeBackendProvider{backend: &fakeBackend{}},
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
		selector:     &fakeSelector{classList: []string{"Article"}},
	}

	tests := []struct {
		name         string
		id           string
		wantErr      bool
		wantContains string
	}{
		{name: "valid short id", id: "my-export", wantErr: false},
		{name: "valid 128 chars", id: strings.Repeat("a", 128), wantErr: false},
		{name: "too long 129 chars", id: strings.Repeat("a", 129), wantErr: true, wantContains: "too long"},
		{name: "too long 256 chars", id: strings.Repeat("a", 256), wantErr: true, wantContains: "too long"},
		{name: "invalid chars uppercase", id: "MyExport", wantErr: true, wantContains: "invalid export id"},
		{name: "invalid chars space", id: "my export", wantErr: true, wantContains: "invalid export id"},
		{name: "empty id", id: "", wantErr: true, wantContains: "invalid export id"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, method := range []string{"export", "status", "cancel"} {
				t.Run(method, func(t *testing.T) {
					var err error
					switch method {
					case "export":
						_, err = s.Export(context.Background(), nil, tc.id, "s3", nil, nil, "", "")
					case "status":
						_, err = s.Status(context.Background(), nil, "s3", tc.id, "", "")
					case "cancel":
						err = s.Cancel(context.Background(), nil, "s3", tc.id, "", "")
					}
					if tc.wantErr {
						require.Error(t, err)
						assert.ErrorIs(t, err, ErrExportValidation)
						assert.Contains(t, err.Error(), tc.wantContains)
					} else if err != nil {
						// Valid IDs may still fail downstream (e.g. no metadata found),
						// but they must NOT fail on validation.
						assert.NotErrorIs(t, err, ErrExportValidation)
					}
				})
			}
		})
	}
}

func TestScheduler_ErrorPaths(t *testing.T) {
	tests := []struct {
		name         string
		backend      modulecapabilities.BackupBackend
		method       string // "status" or "cancel"
		wantIs       error  // nil means no ErrorIs check
		wantNotIs    error  // nil means no NotErrorIs check
		wantContains string // substring the error must contain
	}{
		{
			name:         "status returns not found when nothing exists",
			backend:      &fakeBackend{},
			method:       "status",
			wantContains: "not found",
		},
		{
			name:    "status init failure wraps validation",
			backend: &failInitBackend{err: fmt.Errorf("permission denied")},
			method:  "status",
			wantIs:  ErrExportValidation,
		},
		{
			name:    "cancel init failure wraps validation",
			backend: &failInitBackend{err: fmt.Errorf("permission denied")},
			method:  "cancel",
			wantIs:  ErrExportValidation,
		},
		{
			name:         "status propagates backend error",
			backend:      &errorBackend{err: fmt.Errorf("connection refused")},
			method:       "status",
			wantContains: "connection refused",
		},
		{
			name:      "cancel propagates backend error",
			backend:   &errorBackend{err: fmt.Errorf("permission denied")},
			method:    "cancel",
			wantNotIs: ErrExportNotFound,
		},
		{
			name:    "cancel returns not found when metadata missing",
			backend: &fakeBackend{},
			method:  "cancel",
			wantIs:  ErrExportNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			s := &Scheduler{
				logger:       logger,
				authorizer:   mocks.NewMockAuthorizer(),
				backends:     &fakeBackendProvider{backend: tc.backend},
				client:       &fakeExportClient{},
				nodeResolver: &fakeNodeResolver{nodes: map[string]string{}},
			}

			var err error
			switch tc.method {
			case "status":
				_, err = s.Status(context.Background(), nil, "s3", "test-export", "", "")
			case "cancel":
				err = s.Cancel(context.Background(), nil, "s3", "test-export", "", "")
			}

			require.Error(t, err)
			if tc.wantIs != nil {
				assert.ErrorIs(t, err, tc.wantIs)
			}
			if tc.wantNotIs != nil {
				assert.NotErrorIs(t, err, tc.wantNotIs)
			}
			if tc.wantContains != "" {
				assert.Contains(t, err.Error(), tc.wantContains)
			}
		})
	}
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
		logger:     logger,
		authorizer: mocks.NewMockAuthorizer(),
		backends:   &fakeBackendProvider{backend: backend},
		client:     &fakeExportClient{},
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

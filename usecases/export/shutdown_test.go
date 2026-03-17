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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestParticipant_ShutdownWritesFailedNodeStatusViaPrepareCommit(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	// blockingSelector blocks in AcquireShardForExport until released
	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	participant := NewParticipant(shutdownCtx, selector, &fakeBackendProvider{backend: backend}, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")

	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, participant.Prepare(context.Background(), req))
	require.NoError(t, participant.Commit(context.Background(), "test-export"))

	// Wait for AcquireShardForExport to be called
	selector.waitForCall(t)

	// Simulate shutdown
	shutdownCancel()

	// Unblock the selector so it can return the context error
	close(selector.blockCh)

	// Wait for the export goroutine to finish by polling for the status file
	require.Eventually(t, func() bool {
		return backend.getWritten("node_node1_status.json") != nil
	}, 5*time.Second, 10*time.Millisecond, "expected node status to be written")

	written := backend.getWritten("node_node1_status.json")
	var nodeStatus NodeStatus
	require.NoError(t, json.Unmarshal(written, &nodeStatus))
	assert.Equal(t, export.Failed, nodeStatus.Status)
	assert.Contains(t, nodeStatus.Error, "context canceled")
}

func TestParticipant_ShutdownWritesFailedNodeStatus(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := &Participant{
		shutdownCtx: shutdownCtx,
		selector:    selector,
		backends:    &fakeBackendProvider{backend: backend},
		logger:      logger,
	}

	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	done := make(chan struct{})
	go func() {
		p.executeExport(shutdownCtx, backend, req)
		close(done)
	}()

	// Wait for AcquireShardForExport to be called
	selector.waitForCall(t)

	// Simulate shutdown
	shutdownCancel()

	// Unblock the selector
	close(selector.blockCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeExport did not return after shutdown")
	}

	// Verify a failed node status was written
	written := backend.getWritten("node_node1_status.json")
	require.NotNil(t, written, "expected node status to be written")

	var nodeStatus NodeStatus
	require.NoError(t, json.Unmarshal(written, &nodeStatus))
	assert.Equal(t, export.Failed, nodeStatus.Status)
	assert.Contains(t, nodeStatus.Error, "context canceled")
}

func TestScheduler_DeadNodeMarkedAsFailed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// node1 is alive, node2 is dead (not in the cluster)
	resolver := &fakeNodeResolver{
		nodes: map[string]string{
			"node1": "host1:8080",
		},
	}

	// node1 is alive and still running the export
	client := &fakeExportClient{
		isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
			return true, nil
		},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
			"node2": {"TestClass": {"shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, _, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	// node2 is dead and has no status file → overall status should be FAILED
	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "node2")
	assert.Contains(t, status.Error, "no longer part of the cluster")
}

func TestScheduler_RestartedNodeMarkedAsFailed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// node1 is alive but not running the export (simulates restart)
	resolver := &fakeNodeResolver{
		nodes: map[string]string{
			"node1": "host1:8080",
		},
	}

	client := &fakeExportClient{
		isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
			return false, nil // not running — node restarted
		},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, _, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "node1")
	assert.Contains(t, status.Error, "no longer running")
}

// TestScheduler_LivenessReReadResolvesRace verifies that when IsRunning
// returns false but the status file was updated to Success between the
// initial read and the liveness check, the re-read resolves the race and
// the export is reported as successful (not failed).
func TestScheduler_LivenessReReadResolvesRace(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	resolver := &fakeNodeResolver{
		nodes: map[string]string{
			"node1": "host1:8080",
		},
	}

	// Simulate the race: the first GetObject (initial read) returns
	// ErrNotFound, the second GetObject (re-read after liveness failure)
	// returns a Success status. IsRunning returns false because the
	// participant cleared its active export between the two reads.
	var readCount atomic.Int32
	successStatus := &NodeStatus{
		NodeName:    "node1",
		Status:      export.Success,
		CompletedAt: time.Now().UTC(),
		ShardProgress: map[string]map[string]*ShardProgress{
			"TestClass": {
				"shard0": {Status: export.ShardSuccess, ObjectsExported: 5},
			},
		},
	}
	successData, err := json.Marshal(successStatus)
	require.NoError(t, err)

	// Intercept reads: first call → not found, second call → Success.
	backend.interceptGetObject = func(key string) ([]byte, error, bool) {
		if key != "node_node1_status.json" {
			return nil, nil, false // pass through
		}
		n := readCount.Add(1)
		if n == 1 {
			return nil, backup.NewErrNotFound(fmt.Errorf("not found")), true
		}
		return successData, nil, true
	}

	client := &fakeExportClient{
		isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
			return false, nil // not running — goroutine finished
		},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, allTerminal, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	assert.Equal(t, string(export.Success), status.Status, "re-read should resolve the race to Success")
	assert.True(t, allTerminal, "node reached terminal state via re-read")
	assert.Empty(t, status.Error, "no error expected for successful re-read")
}

func TestScheduler_TransferringNodeStaysTransferring(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// Write a Transferring node status with per-shard progress to the backend
	nodeStatus := &NodeStatus{
		NodeName: "node1",
		Status:   export.Transferring,
		ShardProgress: map[string]map[string]*ShardProgress{
			"TestClass": {
				"shard0": {Status: export.ShardSuccess, ObjectsExported: 300},
				"shard1": {Status: export.ShardTransferring, ObjectsExported: 200},
			},
		},
	}
	data, err := json.Marshal(nodeStatus)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "", "node_node1_status.json", "", "", newBytesReadCloser(data))
	require.NoError(t, err)

	resolver := &fakeNodeResolver{
		nodes: map[string]string{
			"node1": "host1:8080",
		},
	}

	client := &fakeExportClient{
		isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
			return true, nil // still running
		},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0", "shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, _, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	// Overall status stays Transferring
	assert.Equal(t, string(export.Transferring), status.Status)
	assert.Empty(t, status.Error)

	// Per-shard progress is passed through
	require.NotNil(t, status.ShardStatus["TestClass"])
	assert.Equal(t, string(export.Success), status.ShardStatus["TestClass"]["shard0"].Status)
	assert.Equal(t, int64(300), status.ShardStatus["TestClass"]["shard0"].ObjectsExported)
	assert.Equal(t, string(export.Transferring), status.ShardStatus["TestClass"]["shard1"].Status)
	assert.Equal(t, int64(200), status.ShardStatus["TestClass"]["shard1"].ObjectsExported)
}

func TestScheduler_DeadNodeShardProgress(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// Node wrote partial progress before dying: shard0 completed, shard1 still transferring
	nodeStatus := &NodeStatus{
		NodeName: "node1",
		Status:   export.Transferring,
		ShardProgress: map[string]map[string]*ShardProgress{
			"TestClass": {
				"shard0": {Status: export.ShardSuccess, ObjectsExported: 300},
				"shard1": {Status: export.ShardTransferring, ObjectsExported: 100},
			},
		},
	}
	data, err := json.Marshal(nodeStatus)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "", "node_node1_status.json", "", "", newBytesReadCloser(data))
	require.NoError(t, err)

	// node1 is no longer in the cluster
	resolver := &fakeNodeResolver{
		nodes: map[string]string{},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       &fakeExportClient{},
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0", "shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, _, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "node1")
	assert.Contains(t, status.Error, "no longer part of the cluster")

	// shard0 was already Success — stays Success
	require.NotNil(t, status.ShardStatus["TestClass"])
	assert.Equal(t, string(export.Success), status.ShardStatus["TestClass"]["shard0"].Status)
	assert.Equal(t, int64(300), status.ShardStatus["TestClass"]["shard0"].ObjectsExported)

	// shard1 was Transferring — overridden to Failed because node is dead
	assert.Equal(t, string(export.Failed), status.ShardStatus["TestClass"]["shard1"].Status)
	assert.Equal(t, int64(100), status.ShardStatus["TestClass"]["shard1"].ObjectsExported)
}

func TestParticipant_NodeStatusWrittenWithSuccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// fakeSelector returns no shards, so export succeeds immediately
	selector := &fakeSelector{classList: []string{"TestClass"}}
	backends := &fakeBackendProvider{backend: backend}
	participant := NewParticipant(context.Background(), selector, backends, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")

	req := &ExportRequest{
		ID:       "test-export",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": nil},
		NodeName: "node1",
	}

	require.NoError(t, participant.Prepare(context.Background(), req))
	require.NoError(t, participant.Commit(context.Background(), "test-export"))

	// Wait for the export goroutine to finish by polling for the status file
	require.Eventually(t, func() bool {
		return backend.getWritten("node_node1_status.json") != nil
	}, 5*time.Second, 10*time.Millisecond, "expected node status to be written")

	written := backend.getWritten("node_node1_status.json")
	var nodeStatus NodeStatus
	require.NoError(t, json.Unmarshal(written, &nodeStatus))
	assert.Equal(t, export.Success, nodeStatus.Status)
	assert.Empty(t, nodeStatus.Error)
}

func TestScheduler_CancelAndExportRace(t *testing.T) {
	// Fires Cancel() while a participant export is running concurrently
	// 100 times to verify that Cancel correctly aborts or detects that
	// the export already finished.
	for i := range 100 {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			logger, _ := test.NewNullLogger()
			backend := &fakeBackend{}

			selector := &fakeSelector{classList: []string{"TestClass"}}
			backends := &fakeBackendProvider{backend: backend}
			participant := NewParticipant(context.Background(), selector, backends, logger, &fakeExportClient{}, &fakeNodeResolver{}, "node1")

			resolver := &fakeNodeResolver{
				nodes: map[string]string{
					"node1": "host1:8080",
				},
			}

			client := &fakeExportClient{
				isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
					return participant.IsRunning("test-export"), nil
				},
			}

			s := &Scheduler{
				logger:       logger,
				authorizer:   mocks.NewMockAuthorizer(),
				selector:     selector,
				backends:     backends,
				participant:  participant,
				client:       client,
				nodeResolver: resolver,
				localNode:    "node1",
			}

			// Write initial metadata so Cancel() can find it.
			initialMeta := &ExportMetadata{
				ID:      "test-export",
				Backend: "s3",
				Status:  export.Started,
				Classes: []string{"TestClass"},
				NodeAssignments: map[string]map[string][]string{
					"node1": {"TestClass": nil},
				},
				StartedAt: time.Now().UTC(),
			}
			metaData, err := json.Marshal(initialMeta)
			require.NoError(t, err)
			_, err = backend.Write(context.Background(), "test-export", exportMetadataFile, "", "", newBytesReadCloser(metaData))
			require.NoError(t, err)

			// Start the export via Prepare/Commit
			req := &ExportRequest{
				ID:       "test-export",
				Backend:  "s3",
				Classes:  []string{"TestClass"},
				Shards:   map[string][]string{"TestClass": nil},
				NodeName: "node1",
			}
			require.NoError(t, participant.Prepare(context.Background(), req))
			require.NoError(t, participant.Commit(context.Background(), "test-export"))

			// Fire Cancel concurrently.
			cancelErr := s.Cancel(context.Background(), nil, "s3", "test-export", "", "")

			// Wait for the export goroutine to finish
			require.Eventually(t, func() bool {
				return !participant.IsRunning("test-export")
			}, 5*time.Second, 10*time.Millisecond)

			written := backend.getWritten(exportMetadataFile)
			require.NotNil(t, written, "expected metadata to be written")

			var meta ExportMetadata
			require.NoError(t, json.Unmarshal(written, &meta))

			switch meta.Status {
			case export.Success, export.Failed:
				// Export finished before cancel took effect — cancel should
				// return AlreadyFinished or have written the metadata itself.
				// Both are acceptable race outcomes.
			case export.Canceled:
				assert.NoError(t, cancelErr)
			case export.Started:
				// Cancel may not have promoted yet — acceptable race outcome.
			default:
				t.Fatalf("unexpected terminal status: %s", meta.Status)
			}
		})
	}
}

func TestScheduler_SkippedShardInStatusAssembly(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// Node completed export: shard0 succeeded, shard1 was cold and skipped.
	nodeStatus := &NodeStatus{
		NodeName: "node1",
		Status:   export.Success,
		ShardProgress: map[string]map[string]*ShardProgress{
			"TestClass": {
				"shard0": {Status: export.ShardSuccess, ObjectsExported: 500},
				"shard1": {Status: export.ShardSkipped, ObjectsExported: 0},
			},
		},
	}
	data, err := json.Marshal(nodeStatus)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "", "node_node1_status.json", "", "", newBytesReadCloser(data))
	require.NoError(t, err)

	resolver := &fakeNodeResolver{
		nodes: map[string]string{
			"node1": "host1:8080",
		},
	}

	client := &fakeExportClient{
		isRunningFn: func(_ context.Context, _ string, _ string) (bool, error) {
			return false, nil // finished
		},
	}

	s := &Scheduler{
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	meta := &ExportMetadata{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0", "shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, _, err := s.assembleStatusFromMetadata(context.Background(), backend, "test-export", "", "", meta)
	require.NoError(t, err)

	// Overall status is Success — skipped shards don't block completion
	assert.Equal(t, string(export.Success), status.Status)
	assert.Empty(t, status.Error)

	// Per-shard status is correctly reported
	require.NotNil(t, status.ShardStatus["TestClass"])
	assert.Equal(t, string(export.ShardSuccess), status.ShardStatus["TestClass"]["shard0"].Status)
	assert.Equal(t, int64(500), status.ShardStatus["TestClass"]["shard0"].ObjectsExported)
	assert.Equal(t, string(export.ShardSkipped), status.ShardStatus["TestClass"]["shard1"].Status)
	assert.Equal(t, int64(0), status.ShardStatus["TestClass"]["shard1"].ObjectsExported)
}

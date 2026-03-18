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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/export"
)

func TestParticipant_PrepareValidation(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	t.Run("nil request", func(t *testing.T) {
		err := p.Prepare(context.Background(), nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrExportValidation)
	})

	t.Run("empty ID", func(t *testing.T) {
		err := p.Prepare(context.Background(), &ExportRequest{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrExportValidation)
	})
}

func TestParticipant_RejectsSecondExport(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	req1 := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err := p.Prepare(context.Background(), req1)
	require.NoError(t, err)

	req2 := &ExportRequest{
		ID:       "export-2",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err = p.Prepare(context.Background(), req2)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrExportAlreadyActive)
	assert.Contains(t, err.Error(), "already in progress")

	// Clean up
	p.Abort("export-1")
}

func TestParticipant_IsRunning(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	// Nothing running yet
	assert.False(t, p.IsRunning("export-1"))

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err := p.Prepare(context.Background(), req)
	require.NoError(t, err)

	// After Prepare the slot is reserved — IsRunning should be true
	assert.True(t, p.IsRunning("export-1"))
	// Different ID should not match
	assert.False(t, p.IsRunning("export-other"))

	// Clean up
	p.Abort("export-1")
}

func TestParticipant_ConcurrentPrepareOnlyOneSucceeds(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	const n = 50
	results := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(id int) {
			defer wg.Done()
			req := &ExportRequest{
				ID:       fmt.Sprintf("export-%d", id),
				Backend:  "s3",
				Classes:  []string{"TestClass"},
				Shards:   map[string][]string{"TestClass": {"shard0"}},
				NodeName: "node1",
			}
			results <- p.Prepare(context.Background(), req)
		}(i)
	}

	wg.Wait()
	close(results)

	var successes int
	for err := range results {
		if err == nil {
			successes++
		} else {
			assert.ErrorIs(t, err, ErrExportAlreadyActive)
		}
	}

	assert.Equal(t, 1, successes, "exactly one Prepare should succeed")

	// Clean up
	p.mu.Lock()
	activeID := p.activeExport
	p.mu.Unlock()
	p.Abort(activeID)
}

func TestParticipant_PrepareAfterAbort(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	req1 := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req1))
	p.Abort("export-1")

	// Slot should be free now
	assert.False(t, p.IsRunning("export-1"))

	req2 := &ExportRequest{
		ID:       "export-2",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req2))
	assert.True(t, p.IsRunning("export-2"))

	// Clean up
	p.Abort("export-2")
}

func TestParticipant_PrepareAfterCommitCompletes(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// fakeSelector with no shards configured — executeExport completes immediately
	selector := &fakeSelector{classList: []string{"TestClass"}}

	p := NewParticipant(
		selector,
		&fakeBackendProvider{backend: backend},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	req1 := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req1))
	require.NoError(t, p.Commit(context.Background(), "export-1"))

	// Wait for executeExport goroutine to finish and release the slot
	require.Eventually(t, func() bool {
		return !p.IsRunning("export-1")
	}, 5*time.Second, 10*time.Millisecond)

	// Now a new Prepare should succeed
	req2 := &ExportRequest{
		ID:       "export-2",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req2))
	assert.True(t, p.IsRunning("export-2"))

	p.Abort("export-2")
}

func TestParticipant_ReservationTimeoutReleasesSlot(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := &Participant{
		shutdownCtx:  context.Background(),
		selector:     &blockingSelector{blockCh: make(chan struct{})},
		backends:     &fakeBackendProvider{backend: &fakeBackend{}},
		logger:       logger,
		client:       &fakeExportClient{},
		nodeResolver: &fakeNodeResolver{},
	}

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	// Manually prepare with a short timer instead of using the const
	p.mu.Lock()
	p.activeExport = req.ID
	p.preparedReq = req
	p.abortTimer = time.AfterFunc(50*time.Millisecond, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.clearAndRelease()
	})
	p.mu.Unlock()

	assert.True(t, p.IsRunning("export-1"))

	// Wait for the timer to fire
	require.Eventually(t, func() bool {
		return !p.IsRunning("export-1")
	}, 5*time.Second, 10*time.Millisecond)

	// New Prepare should succeed
	req2 := &ExportRequest{
		ID:       "export-2",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req2))
	p.Abort("export-2")
}

func TestParticipant_AbortWrongIDIsNoop(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req))

	// Abort with wrong ID — should be a no-op
	p.Abort("wrong-id")

	// Original export should still be active
	assert.True(t, p.IsRunning("export-1"))

	// Clean up
	p.Abort("export-1")
}

func TestParticipant_CommitErrors(t *testing.T) {
	tests := []struct {
		name         string
		prepareID    string // empty means no Prepare call
		commitID     string
		wantContains string
		slotReleased bool // whether the slot should be released after the failed commit
	}{
		{
			name:         "without prepare",
			commitID:     "nonexistent",
			wantContains: "No export prepared",
		},
		{
			name:         "wrong ID",
			prepareID:    "export-1",
			commitID:     "wrong-id",
			wantContains: "mismatch",
			slotReleased: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewParticipant(
				&blockingSelector{blockCh: make(chan struct{})},
				&fakeBackendProvider{backend: &fakeBackend{}},
				logger,
				&fakeExportClient{}, &fakeNodeResolver{}, "node1",
			)

			if tc.prepareID != "" {
				req := &ExportRequest{
					ID:       tc.prepareID,
					Backend:  "s3",
					Classes:  []string{"TestClass"},
					Shards:   map[string][]string{"TestClass": {"shard0"}},
					NodeName: "node1",
				}
				require.NoError(t, p.Prepare(context.Background(), req))
			}

			err := p.Commit(context.Background(), tc.commitID)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantContains)

			if tc.slotReleased {
				assert.False(t, p.IsRunning(tc.prepareID))
			}
		})
	}
}

func TestParticipant_AbortRunningExport(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// blockingSelector blocks forever until context is canceled
	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := NewParticipant(
		selector,
		&fakeBackendProvider{backend: backend},
		logger,
		&fakeExportClient{}, &fakeNodeResolver{}, "node1",
	)

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req))
	require.NoError(t, p.Commit(context.Background(), "export-1"))

	// Wait for the export goroutine to actually start
	selector.waitForCall(t)

	// Abort the running export
	p.Abort("export-1")

	// The goroutine should detect cancellation and release the slot
	require.Eventually(t, func() bool {
		return !p.IsRunning("export-1")
	}, 5*time.Second, 10*time.Millisecond)

	// Verify failed status was written
	written := backend.getWritten("node_node1_status.json")
	require.NotNil(t, written, "expected node status to be written")

	var nodeStatus NodeStatus
	require.NoError(t, json.Unmarshal(written, &nodeStatus))
	assert.Equal(t, export.Failed, nodeStatus.Status)
}

func TestParticipant_FailedExportAbortsSiblings(t *testing.T) {
	tests := []struct {
		name               string
		siblingNodes       []string
		resolverNodes      map[string]string
		failExport         bool // true: cancel shutdownCtx; false: empty shards → success
		expectedAbortHosts []string
	}{
		{
			name:               "failed export aborts all siblings",
			siblingNodes:       []string{"node2", "node3"},
			resolverNodes:      map[string]string{"node2": "host2:8080", "node3": "host3:8080"},
			failExport:         true,
			expectedAbortHosts: []string{"host2:8080", "host3:8080"},
		},
		{
			name:       "failed export without siblings sends no aborts",
			failExport: true,
		},
		{
			name:          "successful export does not abort siblings",
			siblingNodes:  []string{"node2"},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			failExport:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			backend := &fakeBackend{}

			var abortMu sync.Mutex
			var abortedHosts []string
			client := &fakeExportClient{
				abortFn: func(_ context.Context, host, _ string) error {
					abortMu.Lock()
					abortedHosts = append(abortedHosts, host)
					abortMu.Unlock()
					return nil
				},
			}

			var sel Selector
			if tc.failExport {
				sel = &blockingSelector{blockCh: make(chan struct{})}
			} else {
				sel = &fakeSelector{classList: []string{"TestClass"}}
			}

			p := NewParticipant(sel, &fakeBackendProvider{backend: backend}, logger,
				client, &fakeNodeResolver{nodes: tc.resolverNodes}, "node1")

			shards := map[string][]string{"TestClass": {"shard0"}}
			if !tc.failExport {
				shards = map[string][]string{}
			}

			req := &ExportRequest{
				ID:           "test-export",
				Backend:      "s3",
				Classes:      []string{"TestClass"},
				Shards:       shards,
				NodeName:     "node1",
				SiblingNodes: tc.siblingNodes,
			}

			require.NoError(t, p.Prepare(context.Background(), req))
			require.NoError(t, p.Commit(context.Background(), "test-export"))

			if tc.failExport {
				sel.(*blockingSelector).waitForCall(t)
				p.StartShutdown()
			}

			require.Eventually(t, func() bool {
				return !p.IsRunning("test-export")
			}, 5*time.Second, 10*time.Millisecond)

			if len(tc.expectedAbortHosts) > 0 {
				require.Eventually(t, func() bool {
					abortMu.Lock()
					defer abortMu.Unlock()
					return len(abortedHosts) == len(tc.expectedAbortHosts)
				}, 5*time.Second, 10*time.Millisecond, "expected %d abort calls", len(tc.expectedAbortHosts))

				abortMu.Lock()
				defer abortMu.Unlock()
				assert.ElementsMatch(t, tc.expectedAbortHosts, abortedHosts)
			} else {
				assert.Never(t, func() bool {
					abortMu.Lock()
					defer abortMu.Unlock()
					return len(abortedHosts) > 0
				}, 200*time.Millisecond, 10*time.Millisecond, "expected no abort calls")
			}
		})
	}
}

func TestParticipant_SiblingFailureCancelsAndSetsStatusFailed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := &Participant{
		shutdownCtx:          context.Background(),
		selector:             selector,
		backends:             &fakeBackendProvider{backend: backend},
		logger:               logger,
		client:               &fakeExportClient{},
		nodeResolver:         &fakeNodeResolver{},
		siblingCheckInterval: 50 * time.Millisecond,
	}

	// Write a Failed status for the sibling node
	siblingStatus := &NodeStatus{
		NodeName: "node2",
		Status:   export.Failed,
		Error:    "disk full",
	}
	sibData, err := json.Marshal(siblingStatus)
	require.NoError(t, err)
	_, err = backend.Write(context.Background(), "test-export", "node_node2_status.json", "", "", newBytesReadCloser(sibData))
	require.NoError(t, err)

	req := &ExportRequest{
		ID:           "test-export",
		Backend:      "s3",
		Classes:      []string{"TestClass"},
		Shards:       map[string][]string{"TestClass": {"shard0"}},
		NodeName:     "node1",
		SiblingNodes: []string{"node2"},
	}

	require.NoError(t, p.Prepare(context.Background(), req))
	require.NoError(t, p.Commit(context.Background(), "test-export"))

	// Wait for the export goroutine to start
	selector.waitForCall(t)

	// The status writer goroutine should detect the sibling failure and
	// auto-cancel the export without an explicit Abort call.
	require.Eventually(t, func() bool {
		return !p.IsRunning("test-export")
	}, 5*time.Second, 10*time.Millisecond, "expected export to stop after sibling failure")

	// Verify the persisted node status has BOTH Status=Failed AND the
	// sibling error message. Before the fix, Status would remain
	// Transferring while Error was set — an inconsistent state.
	written := backend.getWritten("node_node1_status.json")
	require.NotNil(t, written, "expected node status to be written")

	var nodeStatus NodeStatus
	require.NoError(t, json.Unmarshal(written, &nodeStatus))
	assert.Equal(t, export.Failed, nodeStatus.Status, "status must be Failed, not Transferring")
	assert.Contains(t, nodeStatus.Error, "sibling node")
	assert.Contains(t, nodeStatus.Error, "disk full")
}

func TestParticipant_CheckSiblingHealth(t *testing.T) {
	tests := []struct {
		name          string
		siblingNodes  []string
		siblingStatus *NodeStatus // nil means don't write a sibling status file
		resolverNodes map[string]string
		isRunningFn   func(context.Context, string, string) (bool, error) // nil defaults to (false, nil)
		cancelCtx     bool                                                // if true, pass an already-canceled context
		expectFailed  bool
	}{
		{
			name:          "healthy sibling continues",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Transferring},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			isRunningFn: func(context.Context, string, string) (bool, error) {
				return true, nil
			},
			expectFailed: false,
		},
		{
			name:          "failed sibling detected via status file",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Failed, Error: "disk full"},
			expectFailed:  true,
		},
		{
			name:          "successful sibling continues",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Success},
			expectFailed:  false,
		},
		{
			name:         "no siblings skips check",
			siblingNodes: nil,
			expectFailed: false,
		},
		{
			name:          "missing status but node alive and running",
			siblingNodes:  []string{"node2"},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			isRunningFn: func(context.Context, string, string) (bool, error) {
				return true, nil
			},
			expectFailed: false,
		},
		{
			name:         "missing status and node left cluster",
			siblingNodes: []string{"node2"},
			// resolver has no entry for node2 → NodeHostname returns false
			expectFailed: true,
		},
		{
			name:          "non-terminal status and node not running",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Transferring},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			isRunningFn: func(context.Context, string, string) (bool, error) {
				return false, nil
			},
			expectFailed: true,
		},
		{
			name:          "IsRunning error treated as inconclusive",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Transferring},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			isRunningFn: func(context.Context, string, string) (bool, error) {
				return false, fmt.Errorf("connection refused")
			},
			expectFailed: false,
		},
		{
			name:          "canceled context does not blame sibling",
			siblingNodes:  []string{"node2"},
			siblingStatus: &NodeStatus{NodeName: "node2", Status: export.Transferring},
			resolverNodes: map[string]string{"node2": "host2:8080"},
			cancelCtx:     true,
			expectFailed:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			backend := &fakeBackend{}

			client := &fakeExportClient{
				isRunningFn: tc.isRunningFn,
			}

			p := NewParticipant(
				&blockingSelector{blockCh: make(chan struct{})},
				&fakeBackendProvider{backend: backend},
				logger,
				client, &fakeNodeResolver{nodes: tc.resolverNodes}, "node1",
			)

			if tc.siblingStatus != nil {
				sibData, err := json.Marshal(tc.siblingStatus)
				require.NoError(t, err)
				key := fmt.Sprintf("node_%s_status.json", tc.siblingStatus.NodeName)
				_, err = backend.Write(context.Background(), "test-export", key, "", "", newBytesReadCloser(sibData))
				require.NoError(t, err)
			}

			req := &ExportRequest{
				ID:           "test-export",
				Backend:      "s3",
				NodeName:     "node1",
				SiblingNodes: tc.siblingNodes,
			}

			ctx := context.Background()
			if tc.cancelCtx {
				cctx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cctx
			}

			_, _, failed := p.siblingHasFailed(ctx, backend, req)
			assert.Equal(t, tc.expectFailed, failed)
		})
	}
}

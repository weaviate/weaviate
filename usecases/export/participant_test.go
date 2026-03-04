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

func TestParticipant_RejectsSecondExport(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
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
	assert.Contains(t, err.Error(), "already in progress")

	// Clean up
	p.Abort("export-1")
}

func TestParticipant_IsRunning(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
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
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
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
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
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

	// emptySelector returns no shards so executeExport completes immediately
	selector := &emptySelector{classList: []string{"TestClass"}}

	p := NewParticipant(
		context.Background(),
		selector,
		&fakeBackendProvider{backend: backend},
		logger,
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
		shutdownCtx: context.Background(),
		selector:    &blockingSelector{blockCh: make(chan struct{})},
		backends:    &fakeBackendProvider{backend: &fakeBackend{}},
		logger:      logger,
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
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
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

func TestParticipant_CommitWithoutPrepare(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
	)

	err := p.Commit(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "No export prepared")
}

func TestParticipant_CommitWrongID(t *testing.T) {
	logger, _ := test.NewNullLogger()

	p := NewParticipant(
		context.Background(),
		&blockingSelector{blockCh: make(chan struct{})},
		&fakeBackendProvider{backend: &fakeBackend{}},
		logger,
	)

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	require.NoError(t, p.Prepare(context.Background(), req))

	err := p.Commit(context.Background(), "wrong-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatch")

	// The failed commit should have released the slot
	assert.False(t, p.IsRunning("export-1"))
}

func TestParticipant_AbortRunningExport(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	// blockingSelector blocks forever until context is cancelled
	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := NewParticipant(
		context.Background(),
		selector,
		&fakeBackendProvider{backend: backend},
		logger,
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

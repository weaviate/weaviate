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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestScheduler_ShutdownWritesFailedMetadata(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	// blockingSelector blocks in GetShardsForClass until released
	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	s := &Scheduler{
		shutdownCtx: shutdownCtx,
		logger:      logger,
		selector:    selector,
		backends:    &fakeBackendProvider{backend: backend},
	}

	status := &models.ExportStatusResponse{
		ID:      "test-export",
		Backend: "s3",
		Status:  string(export.Transferring),
		Classes: []string{"TestClass"},
	}

	done := make(chan struct{})
	go func() {
		s.performSingleNodeExport(shutdownCtx, backend, "test-export", status, []string{"TestClass"}, "", "")
		close(done)
	}()

	// Wait for GetShardsForClass to be called
	selector.waitForCall(t)

	// Simulate shutdown
	shutdownCancel()

	// Unblock the selector so it can return the context error
	close(selector.blockCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("performSingleNodeExport did not return after shutdown")
	}

	// Verify a failed metadata was written
	require.Equal(t, string(export.Failed), status.Status)

	written := backend.getWritten(exportMetadataFile)
	require.NotNil(t, written, "expected metadata to be written")

	var meta ExportMetadata
	require.NoError(t, json.Unmarshal(written, &meta))
	assert.Equal(t, export.Failed, meta.Status)
	assert.Contains(t, meta.Error, "context canceled")
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

	// Wait for GetShardsForClass to be called
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

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		nodeResolver: resolver,
	}

	plan := &ExportPlan{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
			"node2": {"TestClass": {"shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, err := s.assembleStatusFromPlan(context.Background(), backend, nil, "test-export", "", "", plan)
	require.NoError(t, err)

	// node2 is dead and has no status file → overall status should be FAILED
	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "node2")
	assert.Contains(t, status.Error, "no longer part of the cluster")
}

// fakeNodeResolver returns hostnames only for nodes in its map.
// Nodes not in the map are considered dead.
type fakeNodeResolver struct {
	nodes map[string]string
}

func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r.nodes[nodeName]
	return host, ok
}

// blockingSelector blocks GetShardsForClass until blockCh is closed.
type blockingSelector struct {
	blockCh   chan struct{}
	classList []string
	called    bool
	calledMu  sync.Mutex
	calledCh  chan struct{}
	once      sync.Once
}

func (s *blockingSelector) initCalledCh() {
	s.once.Do(func() {
		s.calledCh = make(chan struct{})
	})
}

func (s *blockingSelector) GetShardsForClass(ctx context.Context, _ string) ([]ShardLike, error) {
	s.initCalledCh()
	s.calledMu.Lock()
	if !s.called {
		s.called = true
		close(s.calledCh)
	}
	s.calledMu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.blockCh:
		return nil, ctx.Err()
	}
}

func (s *blockingSelector) waitForCall(t *testing.T) {
	t.Helper()
	s.initCalledCh()
	select {
	case <-s.calledCh:
	case <-time.After(5 * time.Second):
		t.Fatal("GetShardsForClass was not called")
	}
}

func (s *blockingSelector) ListClasses(_ context.Context) []string {
	return s.classList
}

func (s *blockingSelector) ShardOwnership(_ context.Context, _ string) (map[string][]string, error) {
	return nil, nil
}

// fakeBackend captures Write calls so tests can verify what was written.
type fakeBackend struct {
	mu      sync.Mutex
	written map[string][]byte
}

func (b *fakeBackend) Write(_ context.Context, _, key, _, _ string, r io.ReadCloser) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.written == nil {
		b.written = make(map[string][]byte)
	}
	b.written[key] = data
	return int64(len(data)), nil
}

func (b *fakeBackend) getWritten(key string) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written[key]
}

func (b *fakeBackend) HomeDir(_, _, _ string) string { return "" }
func (b *fakeBackend) Initialize(context.Context, string, string, string) error {
	return nil
}

func (b *fakeBackend) GetObject(_ context.Context, _ string, key string, _, _ string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if data, ok := b.written[key]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("not found: %s", key)
}

func (b *fakeBackend) IsExternal() bool       { return true }
func (b *fakeBackend) Name() string           { return "fake" }
func (b *fakeBackend) SourceDataPath() string { return "" }
func (b *fakeBackend) PutObject(context.Context, string, string, string, string, []byte) error {
	return nil
}

func (b *fakeBackend) WriteToFile(context.Context, string, string, string, string, string) error {
	return nil
}

func (b *fakeBackend) Read(context.Context, string, string, string, string, io.WriteCloser) (int64, error) {
	return 0, nil
}

func (b *fakeBackend) AllBackups(context.Context) ([]*backup.DistributedBackupDescriptor, error) {
	return nil, nil
}

// fakeBackendProvider returns the same fakeBackend for any backend name.
type fakeBackendProvider struct {
	backend modulecapabilities.BackupBackend
}

func (p *fakeBackendProvider) BackupBackend(_ string) (modulecapabilities.BackupBackend, error) {
	return p.backend, nil
}

// fakeExportClient implements ExportClient for tests.
type fakeExportClient struct {
	isRunningFn func(ctx context.Context, host, exportID string) (bool, error)
}

func (c *fakeExportClient) Execute(_ context.Context, _ string, _ *ExportRequest) error {
	return nil
}

func (c *fakeExportClient) IsRunning(ctx context.Context, host, exportID string) (bool, error) {
	if c.isRunningFn != nil {
		return c.isRunningFn(ctx, host, exportID)
	}
	return false, nil
}

func TestParticipant_RejectsSecondExport(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := &Participant{
		shutdownCtx: context.Background(),
		selector:    selector,
		backends:    &fakeBackendProvider{backend: backend},
		logger:      logger,
	}

	req1 := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err := p.OnExecute(context.Background(), req1)
	require.NoError(t, err)

	req2 := &ExportRequest{
		ID:       "export-2",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err = p.OnExecute(context.Background(), req2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")

	// Clean up
	close(selector.blockCh)
}

func TestParticipant_IsRunning(t *testing.T) {
	logger, _ := test.NewNullLogger()
	backend := &fakeBackend{}

	selector := &blockingSelector{
		blockCh: make(chan struct{}),
	}

	p := &Participant{
		shutdownCtx: context.Background(),
		selector:    selector,
		backends:    &fakeBackendProvider{backend: backend},
		logger:      logger,
	}

	// Nothing running yet
	assert.False(t, p.IsRunning("export-1"))

	req := &ExportRequest{
		ID:       "export-1",
		Backend:  "s3",
		Classes:  []string{"TestClass"},
		Shards:   map[string][]string{"TestClass": {"shard0"}},
		NodeName: "node1",
	}

	err := p.OnExecute(context.Background(), req)
	require.NoError(t, err)

	// Now it should be running
	assert.True(t, p.IsRunning("export-1"))
	// Different ID should not match
	assert.False(t, p.IsRunning("export-other"))

	// Clean up
	close(selector.blockCh)
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
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	plan := &ExportPlan{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, err := s.assembleStatusFromPlan(context.Background(), backend, nil, "test-export", "", "", plan)
	require.NoError(t, err)

	assert.Equal(t, string(export.Failed), status.Status)
	assert.Contains(t, status.Error, "node1")
	assert.Contains(t, status.Error, "no longer running")
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
				"shard0": {Status: export.Success, ObjectsExported: 300},
				"shard1": {Status: export.Transferring, ObjectsExported: 200},
			},
		},
	}
	data, err := json.Marshal(nodeStatus)
	require.NoError(t, err)
	backend.Write(context.Background(), "", "node_node1_status.json", "", "", io.NopCloser(bytes.NewReader(data)))

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
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		client:       client,
		nodeResolver: resolver,
	}

	plan := &ExportPlan{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0", "shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, err := s.assembleStatusFromPlan(context.Background(), backend, nil, "test-export", "", "", plan)
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
				"shard0": {Status: export.Success, ObjectsExported: 300},
				"shard1": {Status: export.Transferring, ObjectsExported: 100},
			},
		},
	}
	data, err := json.Marshal(nodeStatus)
	require.NoError(t, err)
	backend.Write(context.Background(), "", "node_node1_status.json", "", "", io.NopCloser(bytes.NewReader(data)))

	// node1 is no longer in the cluster
	resolver := &fakeNodeResolver{
		nodes: map[string]string{},
	}

	s := &Scheduler{
		shutdownCtx:  context.Background(),
		logger:       logger,
		authorizer:   mocks.NewMockAuthorizer(),
		nodeResolver: resolver,
	}

	plan := &ExportPlan{
		ID:      "test-export",
		Backend: "s3",
		Classes: []string{"TestClass"},
		NodeAssignments: map[string]map[string][]string{
			"node1": {"TestClass": {"shard0", "shard1"}},
		},
		StartedAt: time.Now().UTC(),
	}

	status, err := s.assembleStatusFromPlan(context.Background(), backend, nil, "test-export", "", "", plan)
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

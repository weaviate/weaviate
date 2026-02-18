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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
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

	status := &ExportStatus{
		ID:      "test-export",
		Backend: "s3",
		Status:  export.Transferring,
		Classes: []string{"TestClass"},
		Progress: map[string]*ClassProgress{
			"TestClass": {Status: export.Started},
		},
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
	require.Equal(t, export.Failed, status.Status)

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

// blockingSelector blocks GetShardsForClass until blockCh is closed.
type blockingSelector struct {
	blockCh  chan struct{}
	called   bool
	calledMu sync.Mutex
	calledCh chan struct{}
	once     sync.Once
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
	return nil
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

func (b *fakeBackend) GetObject(context.Context, string, string, string, string) ([]byte, error) {
	return nil, nil
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

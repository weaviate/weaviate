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
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// fakeNodeResolver resolves node names to hostnames from a static map.
type fakeNodeResolver struct {
	nodes     map[string]string
	nodeCount int
}

func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r.nodes[nodeName]
	return host, ok
}

func (r *fakeNodeResolver) NodeCount() int {
	if r.nodeCount > 0 {
		return r.nodeCount
	}
	return len(r.nodes) + 1 // +1 for local node
}

// blockingSelector blocks AcquireShardForExport until blockCh is closed.
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

func (s *blockingSelector) waitForCall(t *testing.T) {
	t.Helper()
	s.initCalledCh()
	select {
	case <-s.calledCh:
	case <-time.After(5 * time.Second):
		t.Fatal("AcquireShardForExport was not called")
	}
}

func (s *blockingSelector) ListClasses(_ context.Context) []string {
	return s.classList
}

func (s *blockingSelector) ShardOwnership(_ context.Context, _ string) (map[string][]string, error) {
	return nil, nil
}

func (s *blockingSelector) IsMultiTenant(_ context.Context, _ string) bool {
	return false
}

func (s *blockingSelector) IsAsyncReplicationEnabled(_ context.Context, _ string) bool {
	return true
}

func (s *blockingSelector) AcquireShardForExport(ctx context.Context, _, _ string) (ShardLike, func(), string, error) {
	s.initCalledCh()
	s.calledMu.Lock()
	if !s.called {
		s.called = true
		close(s.calledCh)
	}
	s.calledMu.Unlock()

	select {
	case <-ctx.Done():
		return nil, nil, "", ctx.Err()
	case <-s.blockCh:
		return nil, nil, "", ctx.Err()
	}
}

// fakeSelector is a configurable test Selector. With no shards configured it
// acts as a no-op (export completes immediately when the request has no
// shards). With shards/skipped/mt configured it returns pre-configured
// testShard instances from AcquireShardForExport.
type fakeSelector struct {
	classList []string
	shards    map[string]map[string]*testShard // className → shardName → testShard
	skipped   map[string]map[string]string     // className → shardName → skipReason
	mt        map[string]bool                  // className → isMultiTenant
}

func (s *fakeSelector) ListClasses(context.Context) []string {
	return s.classList
}

func (s *fakeSelector) ShardOwnership(context.Context, string) (map[string][]string, error) {
	return nil, nil
}

func (s *fakeSelector) IsMultiTenant(_ context.Context, className string) bool {
	return s.mt[className]
}

func (s *fakeSelector) IsAsyncReplicationEnabled(_ context.Context, _ string) bool {
	return true
}

func (s *fakeSelector) AcquireShardForExport(_ context.Context, className, shardName string) (ShardLike, func(), string, error) {
	if s.skipped != nil {
		if reasons, ok := s.skipped[className]; ok {
			if reason, ok := reasons[shardName]; ok {
				return nil, nil, reason, nil
			}
		}
	}
	if s.shards != nil {
		if classShards, ok := s.shards[className]; ok {
			if shard, ok := classShards[shardName]; ok {
				return shard, func() {}, "", nil
			}
		}
	}
	return nil, nil, "shard not found", nil
}

// fakeBackend captures Write calls so tests can verify what was written.
type fakeBackend struct {
	mu                 sync.Mutex
	written            map[string][]byte
	interceptGetObject func(key string) ([]byte, error, bool) // if set, called before default logic; return (data, err, handled)
}

func (b *fakeBackend) Write(_ context.Context, _, key, _, _ string, r backup.ReadCloserWithError) (int64, error) {
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
	if b.interceptGetObject != nil {
		if data, err, handled := b.interceptGetObject(key); handled {
			return data, err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if data, ok := b.written[key]; ok {
		return data, nil
	}
	return nil, backup.NewErrNotFound(fmt.Errorf("not found: %s", key))
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
	abortFn     func(ctx context.Context, host, exportID string) error
}

func (c *fakeExportClient) Prepare(_ context.Context, _ string, _ *ExportRequest) error {
	return nil
}

func (c *fakeExportClient) Commit(_ context.Context, _, _ string) error {
	return nil
}

func (c *fakeExportClient) Abort(ctx context.Context, host, exportID string) error {
	if c.abortFn != nil {
		return c.abortFn(ctx, host, exportID)
	}
	return nil
}

func (c *fakeExportClient) IsRunning(ctx context.Context, host, exportID string) (bool, error) {
	if c.isRunningFn != nil {
		return c.isRunningFn(ctx, host, exportID)
	}
	return false, nil
}

// shardSpec defines a shard with its object count for table-driven tests.
type shardSpec struct {
	name       string
	numObjects int
}

// expectedFile defines an expected parquet output file and its row count.
type expectedFile struct {
	key     string
	numRows int
}

// testShard is a minimal ShardLike backed by an lsmkv.Store for tests.
type testShard struct {
	store *lsmkv.Store
	name  string
}

func (s *testShard) Store() *lsmkv.Store { return s.store }
func (s *testShard) Name() string        { return s.name }

// newTestNodeStatus creates a NodeStatus in Transferring state for tests.
func newTestNodeStatus(nodeName string) *NodeStatus {
	return &NodeStatus{
		NodeName:      nodeName,
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
	}
}

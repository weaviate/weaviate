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

package selfrecovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// --- Stubs ---

type stubSchema struct {
	replicas []string
	err      error
}

func (s stubSchema) ShardReplicas(class, shard string) ([]string, error) {
	return s.replicas, s.err
}

type stubPathResolver struct{ root string }

func (p stubPathResolver) ShardPath(collection, shard string) string {
	return p.root + "/" + collection + "/" + shard
}

// stubNodeSelector implements just enough of cluster.NodeSelector for
// tests. The orchestrator only uses NodeAddress + NodeGRPCPort.
type stubNodeSelector struct {
	addrs map[string]string
	ports map[string]int
}

func (s *stubNodeSelector) NodeAddress(id string) string                      { return s.addrs[id] }
func (s *stubNodeSelector) NodeGRPCPort(id string) (int, error)               { return s.ports[id], nil }
func (s *stubNodeSelector) NodeHostname(id string) (string, bool)             { return s.addrs[id], true }
func (s *stubNodeSelector) AllHostnames() []string                            { return nil }
func (s *stubNodeSelector) AllOtherClusterMembers(port int) map[string]string { return nil }
func (s *stubNodeSelector) NodeCount() int                                    { return len(s.addrs) }
func (s *stubNodeSelector) StorageCandidates() []string                       { return nil }
func (s *stubNodeSelector) NonStorageNodes() []string                         { return nil }
func (s *stubNodeSelector) SortCandidates(nodes []string) []string            { return nodes }
func (s *stubNodeSelector) ClusterHealthScore() int                           { return 0 }
func (s *stubNodeSelector) LocalName() string                                 { return "" }
func (s *stubNodeSelector) Leave() error                                      { return nil }
func (s *stubNodeSelector) Shutdown() error                                   { return nil }

// Compile-time guard.
var _ cluster.NodeSelector = (*stubNodeSelector)(nil)

// stubFileReplicationClient lets each peer's ListFiles return whatever
// the test wants. Other methods are unimplemented (the orchestrator
// only calls ListFiles for probes).
type stubFileReplicationClient struct {
	listFiles func(ctx context.Context, in *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error)
}

func (c *stubFileReplicationClient) PauseFileActivity(ctx context.Context, in *protocol.PauseFileActivityRequest, _ ...grpc.CallOption) (*protocol.PauseFileActivityResponse, error) {
	return nil, errors.New("PauseFileActivity not stubbed")
}

func (c *stubFileReplicationClient) ResumeFileActivity(ctx context.Context, in *protocol.ResumeFileActivityRequest, _ ...grpc.CallOption) (*protocol.ResumeFileActivityResponse, error) {
	return nil, errors.New("ResumeFileActivity not stubbed")
}

func (c *stubFileReplicationClient) ListFiles(ctx context.Context, in *protocol.ListFilesRequest, _ ...grpc.CallOption) (*protocol.ListFilesResponse, error) {
	return c.listFiles(ctx, in)
}

func (c *stubFileReplicationClient) GetFileMetadata(ctx context.Context, in *protocol.GetFileMetadataRequest, _ ...grpc.CallOption) (*protocol.FileMetadata, error) {
	return nil, errors.New("GetFileMetadata not stubbed")
}

func (c *stubFileReplicationClient) GetFile(ctx context.Context, in *protocol.GetFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[protocol.FileChunk], error) {
	return nil, errors.New("GetFile not stubbed")
}

// stubRaft records ops registered and serves a programmable response
// for GetReplicationDetailsByReplicationId.
type stubRaft struct {
	mu                  sync.Mutex
	registeredCalls     []registeredCall
	registerErr         error
	detailsByUUID       map[strfmt.UUID]api.ReplicationDetailsResponse
	registerHook        func(uuid strfmt.UUID, sourceNode, collection, shard, targetNode string)
	getDetailsCallCount atomic.Int32

	// Restart-related: ops indexed by (collection, shard) and the list
	// of UUIDs cancelled via CancelReplication, populated by the test
	// to drive Restart-path assertions.
	opsByCollShard map[string][]api.ReplicationDetailsResponse
	cancelled      []strfmt.UUID
}

type registeredCall struct {
	uuid                                      strfmt.UUID
	sourceNode, collection, shard, targetNode string
}

func (r *stubRaft) RegisterSelfRecovery(ctx context.Context, sourceNode, collection, shard, targetNode string) (strfmt.UUID, error) {
	if r.registerErr != nil {
		return "", r.registerErr
	}
	uuid := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", len(r.registeredCalls)+1))
	r.mu.Lock()
	r.registeredCalls = append(r.registeredCalls, registeredCall{uuid, sourceNode, collection, shard, targetNode})
	r.mu.Unlock()
	if r.registerHook != nil {
		r.registerHook(uuid, sourceNode, collection, shard, targetNode)
	}
	return uuid, nil
}

func (r *stubRaft) GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error) {
	r.getDetailsCallCount.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	resp, ok := r.detailsByUUID[uuid]
	if !ok {
		// Mirror the production error so the orchestrator's
		// errors.Is(...ErrReplicationOperationNotFound) check fires.
		return api.ReplicationDetailsResponse{}, replicationtypes.ErrReplicationOperationNotFound
	}
	return resp, nil
}

func (r *stubRaft) GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection, shard string) ([]api.ReplicationDetailsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.opsByCollShard == nil {
		return nil, fmt.Errorf("op for %s/%s: %w", collection, shard, replicationtypes.ErrReplicationOperationNotFound)
	}
	return r.opsByCollShard[collection+"/"+shard], nil
}

func (r *stubRaft) CancelReplication(ctx context.Context, uuid strfmt.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelled = append(r.cancelled, uuid)
	return nil
}

// --- Tests ---

// newOrchestratorForTest wires an Orchestrator without the runtime
// config plumbing — the feature flag is bypassed by directly calling
// runOne / probeAndDecide.
func newOrchestratorForTest(t *testing.T, raft RaftEntryPoint, schemaR SchemaReader, ns *stubNodeSelector,
	clientFactory copier.FileReplicationServiceClientFactory, paths PathResolver,
) *Orchestrator {
	t.Helper()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	o := New(Config{
		Raft:          raft,
		Schema:        schemaR,
		PathResolver:  paths,
		ClientFactory: clientFactory,
		NodeSelector:  ns,
		NodeName:      "self",
		Logger:        logger,
		PollInterval:  10_000_000, // 10ms in nanoseconds
		ProbeTimeout:  100_000_000,
	})
	o.probeBackoffMin = 5_000_000 // 5ms
	o.probeBackoffMax = 20_000_000
	o.restartTimeout = 200 * time.Millisecond
	o.vanishedGracePeriod = 10 * time.Millisecond
	return o
}

// TestProbeAndDecide_PeerHasData covers the happy path: at least one
// peer reports a non-empty file list, so the decision is to register
// a SELF_RECOVERY op against that peer.
func TestProbeAndDecide_PeerHasData(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			listFiles: func(ctx context.Context, in *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error) {
				return &protocol.ListFilesResponse{FileNames: []string{"a/b/c"}}, nil
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionRegisterOp, dec.action)
	require.NotEmpty(t, dec.sourceNode)
	require.NotEqual(t, "self", dec.sourceNode)
}

// TestProbeAndDecide_AllEmpty covers the catastrophic-wipe case: all
// peers reachable, all return empty file lists. Decision is empty
// fallback (the orchestrator will MkdirAll and create empty + metric).
func TestProbeAndDecide_AllEmpty(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			listFiles: func(ctx context.Context, in *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error) {
				return &protocol.ListFilesResponse{FileNames: nil}, nil
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionEmptyFallback, dec.action)
	require.Len(t, dec.probedPeers, 2)
}

// TestProbeAndDecide_AllUnreachable covers the "wait for peers" case:
// all peers return transport errors. We must NOT silently fall through
// to empty; we must signal retry.
func TestProbeAndDecide_AllUnreachable(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return nil, errors.New("connection refused")
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionRetry, dec.action, "all peers unreachable must yield retry, never empty fallback")
}

// TestProbeAndDecide_MixedUnreachableAndEmpty covers the conservative
// rule: as long as ANY peer was unreachable AND no peer reports data,
// stay in retry. Don't silently create empty when we might just be
// hitting transient networking issues against the only-other-replica.
func TestProbeAndDecide_MixedUnreachableAndEmpty(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		// peer1 (10.0.0.1) reports no data; peer2 (10.0.0.2) is unreachable.
		if addr == "10.0.0.1:50051" {
			return &stubFileReplicationClient{
				listFiles: func(ctx context.Context, in *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error) {
					return &protocol.ListFilesResponse{}, nil
				},
			}, nil
		}
		return nil, errors.New("connection refused")
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionRetry, dec.action, "any unreachable peer + no data anywhere must yield retry, not empty fallback")
}

// TestProbeAndDecide_NoOtherReplicas degenerate case: schema lists
// only this node as a replica. There is nothing to probe; decision
// is empty fallback (consistent with "we have no peer to recover from").
func TestProbeAndDecide_NoOtherReplicas(t *testing.T) {
	ns := &stubNodeSelector{addrs: map[string]string{}, ports: map[string]int{}}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self"}}, ns, nil, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionEmptyFallback, dec.action)
}

// TestEmptyFallback_CreatesDir verifies that the empty-fallback path
// actually creates the live shard directory.
func TestEmptyFallback_CreatesDir(t *testing.T) {
	tmp := t.TempDir()
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})
	err := o.emptyFallback(ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	livePath := tmp + "/C/S"
	info, err := os.Stat(livePath)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestRestart_CancelsInflightAndErasesRecoveryDir verifies the
// "cancel and start from scratch" path: any in-flight SELF_RECOVERY
// op for (collection, shard) targeting this node is cancelled, the
// partial ".recovering/" directory is erased, and a fresh recovery is
// submitted. Other ops (different transfer type, different target,
// already terminal) are left alone.
func TestRestart_CancelsInflightAndErasesRecoveryDir(t *testing.T) {
	tmp := t.TempDir()

	// Pre-existing partial recovery on disk + an in-flight FSM op
	// targeting "self" (the node under test). The op for "other-node"
	// and the COPY op should NOT be cancelled.
	livePath := tmp + "/C/S"
	recoveryPath := livePath + ".recovering"
	require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
	require.NoError(t, os.WriteFile(recoveryPath+"/partial.bin", []byte("partial"), 0o644))

	inflightUUID := strfmt.UUID("11111111-1111-1111-1111-111111111111")
	otherTargetUUID := strfmt.UUID("22222222-2222-2222-2222-222222222222")
	copyOpUUID := strfmt.UUID("33333333-3333-3333-3333-333333333333")
	terminalUUID := strfmt.UUID("44444444-4444-4444-4444-444444444444")

	raft := &stubRaft{
		opsByCollShard: map[string][]api.ReplicationDetailsResponse{
			"C/S": {
				{
					Uuid:         inflightUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "self",
					TransferType: api.SELF_RECOVERY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
				},
				{
					Uuid:         otherTargetUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "other-node", // not us
					TransferType: api.SELF_RECOVERY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
				},
				{
					Uuid:         copyOpUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "self",
					TransferType: api.COPY.String(), // not SELF_RECOVERY
					Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
				},
				{
					Uuid:         terminalUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "self",
					TransferType: api.SELF_RECOVERY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.READY)}, // already terminal
				},
			},
		},
	}

	// Schema: at least one peer with data so the post-restart probe
	// would find a source. (We don't assert the new op registers
	// because the orchestrator's flag is off in this test setup; we
	// only assert the cancel + erase semantics here.)
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	require.NoError(t, o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"}))

	// Only the in-flight SELF_RECOVERY op targeting "self" was cancelled.
	require.Equal(t, []strfmt.UUID{inflightUUID}, raft.cancelled,
		"only the in-flight SELF_RECOVERY op for self should be cancelled, got %v", raft.cancelled)

	// Recovery dir is gone; the live dir is unaffected (it didn't exist).
	_, err := os.Stat(recoveryPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "recovery dir should be erased, got %v", err)
}

// TestRestart_NoInflightOpsAndNoRecoveryDir is the benign "nothing to
// do" case: no FSM ops, no leftover dir. Restart should still succeed
// (and just submit a fresh recovery, but Submit is a no-op when the
// feature flag is off, which it is in tests).
func TestRestart_NoInflightOpsAndNoRecoveryDir(t *testing.T) {
	raft := &stubRaft{} // GetReplicationDetailsByCollectionAndShard returns "not found"
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	require.NoError(t, o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"}))
	require.Empty(t, raft.cancelled)
}

// TestAcceptEmpty_RemovesRecoveryDir creates a stale ".recovering"
// folder and verifies AcceptEmpty wipes it and creates the live path.
func TestAcceptEmpty_RemovesRecoveryDir(t *testing.T) {
	tmp := t.TempDir()
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	livePath := tmp + "/C/S"
	recoveryPath := livePath + ".recovering"
	require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
	require.NoError(t, os.WriteFile(recoveryPath+"/leftover.bin", []byte("garbage"), 0o644))

	got, err := o.AcceptEmpty(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, livePath, got)

	_, err = os.Stat(recoveryPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "recovery path should be gone, got %v", err)

	info, err := os.Stat(livePath)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestCleanupOrphanRecoveryDirs verifies orphans (recovery dir + live
// sibling) are removed; in-flight recoveries (recovery dir alone) and
// unrelated dirs are left untouched.
func TestCleanupOrphanRecoveryDirs(t *testing.T) {
	root := t.TempDir()
	mk := func(p string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p), 0o755))
	}
	mk("Coll/shard1")              // live (no orphan needed)
	mk("Coll/shard1.recovering")   // orphan: live sibling exists
	mk("Coll/shard2.recovering")   // in-flight: no sibling
	mk("Coll/shard3")              // live, no recovery dir
	mk("Coll2/tenantA")            // live (different collection)
	mk("Coll2/tenantA.recovering") // orphan in another collection

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	o := New(Config{Logger: logger, NodeName: "self"})

	removed, err := o.CleanupOrphanRecoveryDirs(root)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join(root, "Coll/shard1.recovering"),
		filepath.Join(root, "Coll2/tenantA.recovering"),
	}, removed)

	// Live dirs untouched.
	for _, p := range []string{"Coll/shard1", "Coll/shard3", "Coll2/tenantA"} {
		_, err := os.Stat(filepath.Join(root, p))
		require.NoError(t, err, "live dir %s should be present", p)
	}
	// In-flight recovery untouched.
	_, err = os.Stat(filepath.Join(root, "Coll/shard2.recovering"))
	require.NoError(t, err, "in-flight recovery dir must be preserved")
}

// TestRegisterAndPoll_ReturnsNotFoundWhenOpVanishes covers the
// class-deletion / tenant-deletion / ForceDelete* path: the orchestrator
// is polling the FSM for a UUID that gets force-deleted upstream. After
// a few consecutive not-found responses, registerAndPoll must return
// the typed sentinel so runOne can exit instead of looping forever.
func TestRegisterAndPoll_ReturnsNotFoundWhenOpVanishes(t *testing.T) {
	raft := &stubRaft{
		// detailsByUUID is nil → every GetReplicationDetailsByReplicationId
		// call returns ErrReplicationOperationNotFound.
		detailsByUUID: nil,
	}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	err := o.registerAndPoll(context.Background(), ShardRef{Collection: "C", Shard: "S"}, "peer1")
	require.Error(t, err)
	require.True(t, errors.Is(err, replicationtypes.ErrReplicationOperationNotFound),
		"expected ErrReplicationOperationNotFound after sustained not-found polls, got: %v", err)
}

// TestSubmit_NoOpInMaintenanceMode verifies the maintenance-mode gate:
// when the operator has put the node into maintenance mode, Submit
// must not spawn any work, and the recovery semaphore must remain
// uncontended.
func TestSubmit_NoOpInMaintenanceMode(t *testing.T) {
	raft := &stubRaft{}
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	o := New(Config{
		Raft:                   raft,
		Schema:                 stubSchema{replicas: []string{"self"}},
		PathResolver:           stubPathResolver{root: t.TempDir()},
		ClientFactory:          nil,
		NodeSelector:           &stubNodeSelector{},
		NodeName:               "self",
		Enabled:                runtime.NewDynamicValue(true),
		MaintenanceModeEnabled: func() bool { return true },
		Logger:                 logger,
		PollInterval:           10_000_000,
		ProbeTimeout:           100_000_000,
	})

	// Submit must return immediately without spawning work; if it did
	// spawn, the stubSchema (single replica = self) would fall through
	// to actionEmptyFallback and create a directory.
	o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

	// Empty-fallback would have called PathResolver.ShardPath; we
	// detect "did anything happen?" by checking the temp dir is empty.
	entries, err := os.ReadDir(o.pathResolver.(stubPathResolver).root)
	require.NoError(t, err)
	require.Empty(t, entries, "Submit must be a no-op in maintenance mode (no shard dirs created)")
}

// TestRunOne_CancelledTerminal verifies that a SELF_RECOVERY op reported
// as CANCELLED by the FSM is treated as terminal: runOne returns without
// re-registering a fresh op. Pre-fix, registerAndPoll returned a generic
// error and runOne would retry — defeating operator-driven cancel via
// /replication/replicate/{id}/cancel.
func TestRunOne_CancelledTerminal(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			listFiles: func(ctx context.Context, in *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error) {
				return &protocol.ListFilesResponse{FileNames: []string{"a"}}, nil
			},
		}, nil
	}
	raft := &stubRaft{detailsByUUID: map[strfmt.UUID]api.ReplicationDetailsResponse{}}
	// Once an op is registered, the FSM immediately returns CANCELLED
	// for that UUID.
	raft.registerHook = func(uuid strfmt.UUID, _, _, _, _ string) {
		raft.mu.Lock()
		defer raft.mu.Unlock()
		raft.detailsByUUID[uuid] = api.ReplicationDetailsResponse{
			Status: api.ReplicationDetailsState{State: string(api.CANCELLED)},
		}
	}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	done := make(chan struct{})
	enterrorsGo := func() {
		defer close(done)
		o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)
	}
	go enterrorsGo()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runOne did not return after CANCELLED — orchestrator likely retrying")
	}

	raft.mu.Lock()
	calls := len(raft.registeredCalls)
	raft.mu.Unlock()
	require.Equal(t, 1, calls, "operator-cancelled op must not be re-registered; got %d RegisterSelfRecovery calls", calls)
}

// TestPerOrchestratorRNG_NotDeterministic verifies the per-orchestrator
// RNG produces variable shuffle output across calls. Pre-fix, rand.Shuffle
// went through the global math/rand source, which (without explicit seed)
// is deterministic — every call yielded the same permutation.
func TestPerOrchestratorRNG_NotDeterministic(t *testing.T) {
	o := New(Config{
		// minimal config; the test exercises the rng field directly.
		Logger: logrus.New(),
	})
	require.NotNil(t, o.rng, "Orchestrator.rng must be initialised by New()")

	// Shuffle a 10-element slice many times; if any two orderings
	// differ we have variability. Pre-fix this would produce the same
	// order every iteration.
	const N = 20
	first := make([]int, 10)
	for i := range first {
		first[i] = i
	}
	o.rngMu.Lock()
	o.rng.Shuffle(len(first), func(i, j int) { first[i], first[j] = first[j], first[i] })
	o.rngMu.Unlock()

	differs := false
	for k := 0; k < N && !differs; k++ {
		cur := make([]int, 10)
		for i := range cur {
			cur[i] = i
		}
		o.rngMu.Lock()
		o.rng.Shuffle(len(cur), func(i, j int) { cur[i], cur[j] = cur[j], cur[i] })
		o.rngMu.Unlock()
		for i := range cur {
			if cur[i] != first[i] {
				differs = true
				break
			}
		}
	}
	require.True(t, differs, "per-orchestrator RNG produced identical shuffles across %d trials — global rand regression?", N+1)
}

// TestAcceptEmpty_PromotesInMemoryWrapper verifies the operator
// escape-hatch invokes onRecoveryComplete after creating the empty
// shard dir. Without this, a RecoveryShard wrapper installed by the
// startup hook stays load-blocked forever despite disk being ready.
// Mirrors the empty-fallback path inside runOne.
func TestAcceptEmpty_PromotesInMemoryWrapper(t *testing.T) {
	tmp := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	var (
		promoteCalls atomic.Int32
		gotColl      atomic.Value
		gotShard     atomic.Value
	)
	o := New(Config{
		Raft:         &stubRaft{},
		Schema:       stubSchema{}, // ShardReplicas returns nil err with default replicas
		PathResolver: stubPathResolver{root: tmp},
		NodeSelector: &stubNodeSelector{},
		NodeName:     "self",
		OnRecoveryComplete: func(_ context.Context, collection, shard string) error {
			promoteCalls.Add(1)
			gotColl.Store(collection)
			gotShard.Store(shard)
			return nil
		},
		Logger: logger,
	})

	_, err := o.AcceptEmpty(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.EqualValues(t, 1, promoteCalls.Load(), "OnRecoveryComplete must be called exactly once")
	require.Equal(t, "C", gotColl.Load())
	require.Equal(t, "S", gotShard.Load())
}

// TestAcceptEmpty_SchemaErrorIsTypedSentinel verifies that the schema
// gate's error chain contains ErrSelfRecoveryShardNotInSchema. The REST
// handler relies on this sentinel to map the failure to HTTP 404
// instead of the generic 500.
func TestAcceptEmpty_SchemaErrorIsTypedSentinel(t *testing.T) {
	tmp := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	o := New(Config{
		Raft:         &stubRaft{},
		Schema:       stubSchema{err: errors.New("class \"NoSuch\" not found in schema")},
		PathResolver: stubPathResolver{root: tmp},
		NodeSelector: &stubNodeSelector{},
		NodeName:     "self",
		Logger:       logger,
	})

	_, err := o.AcceptEmpty(context.Background(), ShardRef{Collection: "NoSuch", Shard: "S"})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSelfRecoveryShardNotInSchema),
		"error chain must contain ErrSelfRecoveryShardNotInSchema for handler 4xx mapping; got %v", err)
}

// TestCancelInflightSelfRecoveryOps_NoMetricDoubleCount verifies the
// cancel path does NOT pre-increment CompletedTotal{result="cancelled"}
// — runOne is the single source of truth, fired when the FSM state is
// observed terminal. Pre-incrementing here used to double-count and
// would also tick even if the cancel RPC ultimately failed.
func TestCancelInflightSelfRecoveryOps_NoMetricDoubleCount(t *testing.T) {
	raft := &stubRaft{
		opsByCollShard: map[string][]api.ReplicationDetailsResponse{
			"C/S": {{
				Uuid:         strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				TransferType: api.SELF_RECOVERY.String(),
				TargetNodeId: "self",
				Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
			}},
		},
	}
	o := newOrchestratorForTest(t, raft, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	before := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("cancelled"))

	cancelled, err := o.cancelInflightSelfRecoveryOps(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Len(t, cancelled, 1, "the SELF_RECOVERY HYDRATING op should have been cancelled")

	after := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("cancelled"))
	require.Equal(t, before, after,
		"cancelInflightSelfRecoveryOps must not increment CompletedTotal{cancelled} — runOne owns that metric")
}

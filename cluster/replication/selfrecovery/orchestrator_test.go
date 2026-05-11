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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/usecases/cluster"
)

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

var _ cluster.NodeSelector = (*stubNodeSelector)(nil)

// stubFileReplicationClient embeds the generated client so only the probe path
// needs stubbing; any other method call panics on the nil embedded interface.
type stubFileReplicationClient struct {
	protocol.FileReplicationServiceClient
	probeShardData func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error)
}

func (c *stubFileReplicationClient) ProbeShardData(ctx context.Context, in *protocol.ProbeShardDataRequest, _ ...grpc.CallOption) (*protocol.ProbeShardDataResponse, error) {
	return c.probeShardData(ctx, in)
}

type stubRaft struct {
	mu                  sync.Mutex
	registeredCalls     []registeredCall
	registerErr         error
	detailsByUUID       map[strfmt.UUID]api.ReplicationDetailsResponse
	registerHook        func(uuid strfmt.UUID, sourceNode, collection, shard, targetNode string)
	getDetailsCallCount atomic.Int32

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
		PollInterval:  10_000_000,
		ProbeTimeout:  100_000_000,
	})
	o.probeBackoffMin = 5_000_000
	o.probeBackoffMax = 20_000_000
	o.restartTimeout = 200 * time.Millisecond
	o.vanishedGracePeriod = 10 * time.Millisecond
	return o
}

func TestProbeAndDecide_PeerHasData(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return &protocol.ProbeShardDataResponse{HasData: true}, nil
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

func TestProbeAndDecide_AllEmpty(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return &protocol.ProbeShardDataResponse{HasData: false}, nil
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionEmptyFallback, dec.action)
	require.Len(t, dec.probedPeers, 2)
}

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

func TestProbeAndDecide_MixedUnreachableAndEmpty(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		if addr == "10.0.0.1:50051" {
			return &stubFileReplicationClient{
				probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
					return &protocol.ProbeShardDataResponse{HasData: false}, nil
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

func TestProbeAndDecide_AllRecovering(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return nil, status.Errorf(codes.Unavailable,
					"shard %q on index %q is recovering: shard recovering from peer", in.ShardName, in.IndexName)
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionEmptyFallback, dec.action,
		"every replica recovering the same shard must empty-fallback, not deadlock retrying each other")
}

func TestProbeAndDecide_AllIndexNotLoaded(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1", "peer2": "10.0.0.2"},
		ports: map[string]int{"peer1": 50051, "peer2": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return nil, status.Errorf(codes.Unavailable, "local index %q not loaded yet", in.IndexName)
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self", "peer1", "peer2"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionRetry, dec.action,
		"index-not-loaded is transient (peer may still hold data): retry, never empty-fallback")
}

func TestProbeAndDecide_NoOtherReplicas(t *testing.T) {
	ns := &stubNodeSelector{addrs: map[string]string{}, ports: map[string]int{}}
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{replicas: []string{"self"}}, ns, nil, stubPathResolver{root: t.TempDir()})

	dec, err := o.probeAndDecide(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.NoError(t, err)
	require.Equal(t, actionEmptyFallback, dec.action)
}

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

func TestRestart_CancelsInflightAndErasesRecoveryDir(t *testing.T) {
	tmp := t.TempDir()

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
					TargetNodeId: "other-node",
					TransferType: api.SELF_RECOVERY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
				},
				{
					Uuid:         copyOpUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "self",
					TransferType: api.COPY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
				},
				{
					Uuid:         terminalUUID,
					Collection:   "C",
					ShardId:      "S",
					TargetNodeId: "self",
					TransferType: api.SELF_RECOVERY.String(),
					Status:       api.ReplicationDetailsState{State: string(api.READY)},
				},
			},
		},
	}

	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	require.NoError(t, o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"}))

	require.Equal(t, []strfmt.UUID{inflightUUID}, raft.cancelled,
		"only the in-flight SELF_RECOVERY op for self should be cancelled, got %v", raft.cancelled)

	_, err := os.Stat(recoveryPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "recovery dir should be erased, got %v", err)
}

func TestRestart_NoInflightOpsAndNoRecoveryDir(t *testing.T) {
	raft := &stubRaft{}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	require.NoError(t, o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"}))
	require.Empty(t, raft.cancelled)
}

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

func TestCleanupOrphanRecoveryDirs(t *testing.T) {
	root := t.TempDir()
	mk := func(p string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p), 0o755))
	}
	mk("Coll/shard1")
	mk("Coll/shard1.recovering")
	mk("Coll/shard2.recovering")
	mk("Coll/shard3")
	mk("Coll2/tenantA")
	mk("Coll2/tenantA.recovering")

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	o := New(Config{Logger: logger, NodeName: "self"})

	removed, err := o.CleanupOrphanRecoveryDirs(root)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join(root, "Coll/shard1.recovering"),
		filepath.Join(root, "Coll2/tenantA.recovering"),
	}, removed)

	for _, p := range []string{"Coll/shard1", "Coll/shard3", "Coll2/tenantA"} {
		_, err := os.Stat(filepath.Join(root, p))
		require.NoError(t, err, "live dir %s should be present", p)
	}
	_, err = os.Stat(filepath.Join(root, "Coll/shard2.recovering"))
	require.NoError(t, err, "in-flight recovery dir must be preserved")
}

func TestRegisterAndPoll_ReturnsNotFoundWhenOpVanishes(t *testing.T) {
	raft := &stubRaft{
		detailsByUUID: nil,
	}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	err := o.registerAndPoll(context.Background(), ShardRef{Collection: "C", Shard: "S"}, "peer1")
	require.Error(t, err)
	require.True(t, errors.Is(err, replicationtypes.ErrReplicationOperationNotFound),
		"expected ErrReplicationOperationNotFound after sustained not-found polls, got: %v", err)
}

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
		Enabled:                true,
		MaintenanceModeEnabled: func() bool { return true },
		Logger:                 logger,
		PollInterval:           10_000_000,
		ProbeTimeout:           100_000_000,
	})

	o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

	entries, err := os.ReadDir(o.pathResolver.(stubPathResolver).root)
	require.NoError(t, err)
	require.Empty(t, entries, "Submit must be a no-op in maintenance mode (no shard dirs created)")
}

func TestSubmit_RaceWithClose(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	for i := 0; i < 200; i++ {
		o := New(Config{
			Raft:         &stubRaft{},
			Schema:       stubSchema{replicas: []string{"self"}},
			PathResolver: stubPathResolver{root: t.TempDir()},
			NodeSelector: &stubNodeSelector{},
			NodeName:     "self",
			Enabled:      true,
			Concurrency:  2,
			Logger:       logger,
			PollInterval: 10 * time.Millisecond,
			ProbeTimeout: 100 * time.Millisecond,
		})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)
		}()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = o.Close(ctx)
		}()
		wg.Wait()
	}
}

func TestRunOne_CancelledTerminal(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(ctx context.Context, in *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return &protocol.ProbeShardDataResponse{HasData: true}, nil
			},
		}, nil
	}
	raft := &stubRaft{detailsByUUID: map[strfmt.UUID]api.ReplicationDetailsResponse{}}
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

func TestPerOrchestratorRNG_NotDeterministic(t *testing.T) {
	o := New(Config{
		Logger: logrus.New(),
	})
	require.NotNil(t, o.rng, "Orchestrator.rng must be initialised by New()")

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
		Schema:       stubSchema{},
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

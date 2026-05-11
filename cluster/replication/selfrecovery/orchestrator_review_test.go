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
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// TestSubmit_DeclinesWhenDisabledOrMaintenance verifies Submit reports
// false (so the caller falls back to normal init) when the feature is
// off or maintenance mode is on, and true when the work is queued.
func TestSubmit_DeclinesWhenDisabledOrMaintenance(t *testing.T) {
	mk := func(enabled *runtime.DynamicValue[bool], maint func() bool) *Orchestrator {
		o := newOrchestratorForTest(t, &stubRaft{},
			stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil,
			stubPathResolver{root: t.TempDir()})
		o.enabled = enabled
		o.maintenanceModeEnabled = maint
		return o
	}

	t.Run("feature off", func(t *testing.T) {
		o := mk(nil, nil)
		require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
	t.Run("maintenance mode", func(t *testing.T) {
		o := mk(runtime.NewDynamicValue(true), func() bool { return true })
		require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
	t.Run("queued", func(t *testing.T) {
		o := mk(runtime.NewDynamicValue(true), nil)
		require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
}

// TestSubmit_QueueFullDropsAndReturnsFalse verifies that once the bounded
// worker queue overflows, Submit returns false and bumps SubmitDroppedTotal
// (so the startup hook can fall back to normal init rather than strand the
// shard in RECOVERING).
func TestSubmit_QueueFullDropsAndReturnsFalse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	block := make(chan struct{})
	t.Cleanup(func() { cancel(); close(block) })

	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	o := New(Config{
		Raft:         &stubRaft{},
		Schema:       stubSchema{replicas: []string{"self", "peer1"}},
		PathResolver: stubPathResolver{root: t.TempDir()},
		NodeSelector: ns,
		NodeName:     "self",
		Enabled:      runtime.NewDynamicValue(true),
		ClientFactory: func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
			<-block
			return nil, errors.New("released")
		},
		Logger:       quietLogger(),
		PollInterval: 10 * time.Millisecond,
		ProbeTimeout: time.Second,
	})
	o.submitQueueCapacity = 1 // make overflow trivial to provoke

	before := testutil.ToFloat64(o.metrics.SubmitDroppedTotal)
	dropped := 0
	for i := 0; i < 4; i++ {
		if !o.Submit(ctx, ShardRef{Collection: "C", Shard: fmt.Sprintf("S%d", i)}, false) {
			dropped++
		}
	}
	require.GreaterOrEqual(t, dropped, 1, "with capacity 1 and a blocked worker, some submits must be dropped")
	require.InDelta(t, float64(dropped), testutil.ToFloat64(o.metrics.SubmitDroppedTotal)-before, 0.001,
		"SubmitDroppedTotal must count exactly the dropped submits")
}

// TestHasInflightReplicationOp covers the M2 guard: only a non-terminal
// op targeting this node counts.
func TestHasInflightReplicationOp(t *testing.T) {
	op := func(target, transfer string, state api.ShardReplicationState) api.ReplicationDetailsResponse {
		return api.ReplicationDetailsResponse{
			Uuid:         strfmt.UUID("00000000-0000-0000-0000-000000000001"),
			Collection:   "C",
			ShardId:      "S",
			TargetNodeId: target,
			TransferType: transfer,
			Status:       api.ReplicationDetailsState{State: string(state)},
		}
	}
	cases := []struct {
		name string
		ops  []api.ReplicationDetailsResponse
		want bool
	}{
		{"no ops", nil, false},
		{"COPY on this node, hydrating", []api.ReplicationDetailsResponse{op("self", api.COPY.String(), api.HYDRATING)}, true},
		{"COPY on other node", []api.ReplicationDetailsResponse{op("other", api.COPY.String(), api.HYDRATING)}, false},
		{"terminal SELF_RECOVERY on this node", []api.ReplicationDetailsResponse{op("self", api.SELF_RECOVERY.String(), api.READY)}, false},
		{"non-terminal SELF_RECOVERY on this node", []api.ReplicationDetailsResponse{op("self", api.SELF_RECOVERY.String(), api.FINALIZING)}, true},
		{"mixed: other-node + this-node terminal + this-node non-terminal", []api.ReplicationDetailsResponse{
			op("other", api.MOVE.String(), api.HYDRATING),
			op("self", api.SELF_RECOVERY.String(), api.CANCELLED),
			op("self", api.COPY.String(), api.REGISTERED),
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raft := &stubRaft{}
			if tc.ops != nil {
				raft.opsByCollShard = map[string][]api.ReplicationDetailsResponse{"C/S": tc.ops}
			}
			o := newOrchestratorForTest(t, raft, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})
			got, err := o.HasInflightReplicationOp(context.Background(), "C", "S")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRunOne_GiveUpAfterMaxAttempts: all peers unreachable forever →
// runOne exhausts retries, ticks GiveupTotal and CompletedTotal{failure},
// and returns instead of looping forever.
func TestRunOne_GiveUpAfterMaxAttempts(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
		return nil, errors.New("connection refused")
	}
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: t.TempDir()})

	beforeGiveup := testutil.ToFloat64(o.metrics.GiveupTotal)
	beforeFailure := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runOne did not give up after maxAttempts of unreachable peers")
	}

	require.InDelta(t, beforeGiveup+1, testutil.ToFloat64(o.metrics.GiveupTotal), 0.001)
	require.InDelta(t, beforeFailure+1, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure")), 0.001)
}

// TestRunOne_EmptyFallbackBootstrapSeverity verifies the per-op
// fromBootstrap flag routes the all-peers-empty outcome to the
// gentler counter during the RAFT bootstrap window and to the
// catastrophic-wipe counter otherwise.
func TestRunOne_EmptyFallbackBootstrapSeverity(t *testing.T) {
	mkOrch := func(t *testing.T) (*Orchestrator, string) {
		root := t.TempDir()
		ns := &stubNodeSelector{
			addrs: map[string]string{"peer1": "10.0.0.1"},
			ports: map[string]int{"peer1": 50051},
		}
		clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
			return &stubFileReplicationClient{
				listFiles: func(_ context.Context, _ *protocol.ListFilesRequest) (*protocol.ListFilesResponse, error) {
					return &protocol.ListFilesResponse{}, nil // definitively empty
				},
			}, nil
		}
		return newOrchestratorForTest(t, &stubRaft{},
			stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory, stubPathResolver{root: root}), root
	}

	t.Run("during bootstrap", func(t *testing.T) {
		o, _ := mkOrch(t)
		before := testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal)
		beforeOther := testutil.ToFloat64(o.metrics.NoDataEmptyTotal)
		o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, true /* fromBootstrap */)
		require.InDelta(t, before+1, testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal), 0.001)
		require.InDelta(t, beforeOther, testutil.ToFloat64(o.metrics.NoDataEmptyTotal), 0.001)
	})
	t.Run("post bootstrap", func(t *testing.T) {
		o, _ := mkOrch(t)
		before := testutil.ToFloat64(o.metrics.NoDataEmptyTotal)
		beforeOther := testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal)
		o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)
		require.InDelta(t, before+1, testutil.ToFloat64(o.metrics.NoDataEmptyTotal), 0.001)
		require.InDelta(t, beforeOther, testutil.ToFloat64(o.metrics.NoDataDuringBootstrapTotal), 0.001)
	})
}

// TestProbeErrorClassifiers pins the rolling-upgrade substring fallbacks
// used when an older peer doesn't carry typed gRPC codes: a shard-absent
// phrasing must classify as "definitively empty", "not paused" must
// classify as "shard present", and an unrelated error must be neither.
func TestProbeErrorClassifiers(t *testing.T) {
	require.True(t, isShardAbsentErr(errors.New("rpc error: incoming list files get shard is nil")))
	require.True(t, isShardAbsentErr(errors.New("shard not found")))
	require.False(t, isShardAbsentErr(errors.New("file segment-1.db not found")))
	require.False(t, isShardAbsentErr(nil))

	require.True(t, isShardPresentButNotPausedErr(errors.New("shard \"S\" is not paused for transfer")))
	require.False(t, isShardPresentButNotPausedErr(errors.New("connection refused")))
	require.False(t, isShardPresentButNotPausedErr(nil))
}

// TestRestart_RejectsWhenLiveDirExists: /restart on a shard that already
// has a live local dir is a 409-class error and must not cancel anything.
func TestRestart_RejectsWhenLiveDirExists(t *testing.T) {
	tmp := t.TempDir()
	require.NoError(t, os.MkdirAll(tmp+"/C/S", 0o755))
	raft := &stubRaft{}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	err := o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSelfRecoveryShardAlreadyLive)
	require.Empty(t, raft.cancelled, "Restart must bail before cancelling when the live dir exists")
}

// TestWaitForOpTerminal_VanishedGrace: a polled op that the FSM reports
// as not-found is treated as terminal, after the grace sleep.
func TestWaitForOpTerminal_VanishedGrace(t *testing.T) {
	raft := &stubRaft{detailsByUUID: nil} // every lookup returns not-found
	o := newOrchestratorForTest(t, raft, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	start := time.Now()
	require.NoError(t, o.waitForOpTerminal(context.Background(), strfmt.UUID("11111111-1111-1111-1111-111111111111")))
	require.GreaterOrEqual(t, time.Since(start), o.vanishedGracePeriod, "must wait the grace period before returning")
}

// TestRestart_TimeoutLeavesRecoveryDir: when an in-flight op never
// settles, Restart honours its timeout, returns an error, and leaves the
// partial ".recovering/" dir intact (so the next attempt can resume).
func TestRestart_TimeoutLeavesRecoveryDir(t *testing.T) {
	tmp := t.TempDir()
	recoveryPath := tmp + "/C/S.recovering"
	require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
	require.NoError(t, os.WriteFile(recoveryPath+"/partial.bin", []byte("x"), 0o644))

	inflightUUID := strfmt.UUID("11111111-1111-1111-1111-111111111111")
	raft := &stubRaft{
		opsByCollShard: map[string][]api.ReplicationDetailsResponse{
			"C/S": {{
				Uuid:         inflightUUID,
				Collection:   "C",
				ShardId:      "S",
				TargetNodeId: "self",
				TransferType: api.SELF_RECOVERY.String(),
				Status:       api.ReplicationDetailsState{State: string(api.HYDRATING)},
			}},
		},
		detailsByUUID: map[strfmt.UUID]api.ReplicationDetailsResponse{
			inflightUUID: {Status: api.ReplicationDetailsState{State: string(api.HYDRATING)}}, // never terminal
		},
	}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	err := o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.Error(t, err, "Restart must surface the timeout while the op is still settling")
	require.Equal(t, []strfmt.UUID{inflightUUID}, raft.cancelled)
	_, statErr := os.Stat(recoveryPath)
	require.NoError(t, statErr, "recovery dir must be left intact on timeout so the next attempt resumes")
}

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
)

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

func TestSubmit_DeclinesWhenDisabledOrMaintenance(t *testing.T) {
	mk := func(enabled bool, maint func() bool) *Orchestrator {
		o := newOrchestratorForTest(t, &stubRaft{},
			stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil,
			stubPathResolver{root: t.TempDir()})
		o.enabled = enabled
		o.maintenanceModeEnabled = maint
		return o
	}

	t.Run("feature off", func(t *testing.T) {
		o := mk(false, nil)
		require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
	t.Run("maintenance mode", func(t *testing.T) {
		o := mk(true, func() bool { return true })
		require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
	t.Run("queued", func(t *testing.T) {
		o := mk(true, nil)
		require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
	})
}

func TestSubmit_NeverDropsAllSubmissionsProcessed(t *testing.T) {
	const n = 5000 // far above the removed 1024 queue cap: proves nothing is dropped at scale

	processed := make(chan ShardRef, n)
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil,
		stubPathResolver{root: t.TempDir()})
	o.enabled = true
	o.concurrency = 4
	o.emptyFallbackHook = func(ref ShardRef) { processed <- ref }

	for i := 0; i < n; i++ {
		require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: fmt.Sprintf("S%d", i)}, false),
			"Submit must never drop; every submission must be queued")
	}

	seen := make(map[string]struct{}, n)
	deadline := time.After(30 * time.Second)
	for len(seen) < n {
		select {
		case ref := <-processed:
			seen[ref.Shard] = struct{}{}
		case <-deadline:
			t.Fatalf("only %d/%d submissions processed; the rest were lost", len(seen), n)
		}
	}
}

func TestClose_DrainsWorkersAndRefusesNewSubmits(t *testing.T) {
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self"}}, &stubNodeSelector{}, nil,
		stubPathResolver{root: t.TempDir()})
	o.enabled = true

	require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, o.Close(closeCtx))

	require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S2"}, false))
	require.NoError(t, o.Close(context.Background()))
}

func TestClose_BeforeAnySubmit(t *testing.T) {
	o := newOrchestratorForTest(t, &stubRaft{}, stubSchema{}, &stubNodeSelector{}, nil,
		stubPathResolver{root: t.TempDir()})
	require.NoError(t, o.Close(context.Background()))
	require.False(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))
}

func TestClose_SurfacesDeadlineWhenWorkerStuck(t *testing.T) {
	block := make(chan struct{})
	probeCalled := make(chan struct{}, 1)
	t.Cleanup(func() { close(block) })

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
		Enabled:      true,
		ClientFactory: func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
			select {
			case probeCalled <- struct{}{}:
			default:
			}
			<-block
			return nil, errors.New("released")
		},
		Logger:       quietLogger(),
		PollInterval: 10 * time.Millisecond,
		ProbeTimeout: 10 * time.Second,
	})
	require.True(t, o.Submit(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false))

	select {
	case <-probeCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never reached the blocking ClientFactory")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := o.Close(closeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"Close must surface the caller's deadline when workers don't drain in time")
}

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

func TestHasInflightSelfRecoveryOp(t *testing.T) {
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
		{"self-recovery on this node, hydrating", []api.ReplicationDetailsResponse{op("self", api.SELF_RECOVERY.String(), api.HYDRATING)}, true},
		{"self-recovery on this node, terminal", []api.ReplicationDetailsResponse{op("self", api.SELF_RECOVERY.String(), api.READY)}, false},
		{"self-recovery on other node", []api.ReplicationDetailsResponse{op("other", api.SELF_RECOVERY.String(), api.HYDRATING)}, false},
		{"copy on this node is not self-recovery", []api.ReplicationDetailsResponse{op("self", api.COPY.String(), api.HYDRATING)}, false},
		{"mixed: terminal + non-terminal self-recovery on this node", []api.ReplicationDetailsResponse{
			op("self", api.SELF_RECOVERY.String(), api.READY),
			op("self", api.SELF_RECOVERY.String(), api.FINALIZING),
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raft := &stubRaft{}
			if tc.ops != nil {
				raft.opsByCollShard = map[string][]api.ReplicationDetailsResponse{"C/S": tc.ops}
			}
			o := newOrchestratorForTest(t, raft, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})
			got, err := o.HasInflightSelfRecoveryOp(context.Background(), "C", "S")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

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

func TestRunOne_EmptyFallbackBootstrapSeverity(t *testing.T) {
	mkOrch := func(t *testing.T) (*Orchestrator, string) {
		root := t.TempDir()
		ns := &stubNodeSelector{
			addrs: map[string]string{"peer1": "10.0.0.1"},
			ports: map[string]int{"peer1": 50051},
		}
		clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
			return &stubFileReplicationClient{
				probeShardData: func(_ context.Context, _ *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
					return &protocol.ProbeShardDataResponse{HasData: false}, nil
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
		o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, true)
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

func TestRunOne_EmptyFallbackPromoteFailureRecordsFailure(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(_ context.Context, _ *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return &protocol.ProbeShardDataResponse{HasData: false}, nil
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory,
		stubPathResolver{root: t.TempDir()})
	o.onRecoveryComplete = func(_ context.Context, _, _ string) error {
		return errors.New("simulated promote failure")
	}

	beforeFailure := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure"))
	beforeSuccess := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("success"))
	beforeEmptyLabel := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("empty_fallback"))
	beforeEmpty := testutil.ToFloat64(o.metrics.NoDataEmptyTotal)

	o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

	require.InDelta(t, beforeFailure+1, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure")), 0.001,
		"promote-after-empty failure must record a failure outcome")
	require.InDelta(t, beforeSuccess, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("success")), 0.001,
		"promote-after-empty failure must NOT record a success outcome")
	require.InDelta(t, beforeEmptyLabel, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("empty_fallback")), 0.001,
		"promote-after-empty failure must NOT record an empty_fallback outcome (it never finished promoting)")
	require.InDelta(t, beforeEmpty, testutil.ToFloat64(o.metrics.NoDataEmptyTotal), 0.001,
		"NoDataEmptyTotal increments only after a successful promote")
}

func TestRunOne_EmptyFallbackSuccessRecordsEmptyFallbackLabel(t *testing.T) {
	ns := &stubNodeSelector{
		addrs: map[string]string{"peer1": "10.0.0.1"},
		ports: map[string]int{"peer1": 50051},
	}
	clientFactory := func(_ context.Context, _ string) (copier.FileReplicationServiceClient, error) {
		return &stubFileReplicationClient{
			probeShardData: func(_ context.Context, _ *protocol.ProbeShardDataRequest) (*protocol.ProbeShardDataResponse, error) {
				return &protocol.ProbeShardDataResponse{HasData: false}, nil
			},
		}, nil
	}
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self", "peer1"}}, ns, clientFactory,
		stubPathResolver{root: t.TempDir()})

	beforeEmptyLabel := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("empty_fallback"))
	beforeSuccess := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("success"))
	beforeFailure := testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure"))

	o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

	require.InDelta(t, beforeEmptyLabel+1, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("empty_fallback")), 0.001,
		"empty-fallback success must record under the empty_fallback label")
	require.InDelta(t, beforeSuccess, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("success")), 0.001,
		"empty-fallback must NOT inflate the success label (CompletedTotal must distinguish a data-pulled recovery from an empty shard)")
	require.InDelta(t, beforeFailure, testutil.ToFloat64(o.metrics.CompletedTotal.WithLabelValues("failure")), 0.001,
		"empty-fallback must NOT inflate the failure label (the benign bootstrap-window case would generate spurious failure alerts)")
}

func TestProbeErrorClassifiers(t *testing.T) {
	require.True(t, isShardAbsentErr(errors.New("rpc error: incoming list files get shard is nil")))
	require.True(t, isShardAbsentErr(errors.New("shard not found")))
	require.False(t, isShardAbsentErr(errors.New("file segment-1.db not found")))
	require.False(t, isShardAbsentErr(nil))

	require.True(t, isPeerRecoveringErr(errors.New("shard \"S\" on index \"C\" is recovering: shard recovering from peer")))
	require.False(t, isPeerRecoveringErr(errors.New("local index \"C\" not loaded yet")))
	require.False(t, isPeerRecoveringErr(errors.New("connection refused")))
	require.False(t, isPeerRecoveringErr(nil))
}

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

func TestRestart_SchemaErrorIsTypedSentinel(t *testing.T) {
	tmp := t.TempDir()
	recoveryPath := tmp + "/NoSuch/S.recovering"
	require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
	require.NoError(t, os.WriteFile(recoveryPath+"/marker", []byte("dont-touch"), 0o644))

	raft := &stubRaft{}
	o := newOrchestratorForTest(t, raft,
		stubSchema{err: errors.New("class \"NoSuch\" not found in schema")},
		&stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	err := o.Restart(context.Background(), ShardRef{Collection: "NoSuch", Shard: "S"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSelfRecoveryShardNotInSchema,
		"error chain must contain ErrSelfRecoveryShardNotInSchema for handler 4xx mapping; got %v", err)
	require.Empty(t, raft.cancelled, "Restart must bail before cancelling when the shard is not in schema")
	_, statErr := os.Stat(recoveryPath + "/marker")
	require.NoError(t, statErr, "recovery dir must not be touched when the schema gate refuses the request")
}

func TestWaitForOpTerminal_VanishedGrace(t *testing.T) {
	raft := &stubRaft{detailsByUUID: nil}
	o := newOrchestratorForTest(t, raft, stubSchema{}, &stubNodeSelector{}, nil, stubPathResolver{root: t.TempDir()})

	start := time.Now()
	require.NoError(t, o.waitForOpTerminal(context.Background(), strfmt.UUID("11111111-1111-1111-1111-111111111111")))
	require.GreaterOrEqual(t, time.Since(start), o.vanishedGracePeriod, "must wait the grace period before returning")
}

func TestRestart_SerialisedAgainstInFlightWorker(t *testing.T) {
	tmp := t.TempDir()
	o := newOrchestratorForTest(t, &stubRaft{},
		stubSchema{replicas: []string{"self", "peer1"}},
		&stubNodeSelector{}, nil, stubPathResolver{root: tmp})
	ref := ShardRef{Collection: "C", Shard: "S"}

	workerHolds := make(chan struct{})
	workerReleases := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		unlock := o.lockShard(ref)
		close(workerHolds)
		<-workerReleases
		unlock()
	}()
	<-workerHolds

	restartDone := make(chan error, 1)
	go func() {
		restartDone <- o.Restart(context.Background(), ref)
	}()

	select {
	case <-restartDone:
		t.Fatal("Restart returned while a worker still held the shard lock")
	case <-time.After(50 * time.Millisecond):
	}

	close(workerReleases)
	<-workerDone

	select {
	case err := <-restartDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Restart did not return after the worker released the shard lock")
	}
}

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
			inflightUUID: {Status: api.ReplicationDetailsState{State: string(api.HYDRATING)}},
		},
	}
	o := newOrchestratorForTest(t, raft, stubSchema{replicas: []string{"self", "peer1"}}, &stubNodeSelector{}, nil, stubPathResolver{root: tmp})

	err := o.Restart(context.Background(), ShardRef{Collection: "C", Shard: "S"})
	require.Error(t, err, "Restart must surface the timeout while the op is still settling")
	require.Equal(t, []strfmt.UUID{inflightUUID}, raft.cancelled)
	_, statErr := os.Stat(recoveryPath)
	require.NoError(t, statErr, "recovery dir must be left intact on timeout so the next attempt resumes")
}

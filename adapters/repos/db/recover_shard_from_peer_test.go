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

package db

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// fakeSelfRecoveryOrch is a stub SelfRecoveryOrchestrator for unit tests.
type fakeSelfRecoveryOrch struct {
	enabled     bool
	inflightOp  bool
	inflightErr error
	submitOK    bool

	submitCalls      int
	gotFromBootstrap bool
	// submitHook runs synchronously inside SubmitRecovery — used to
	// inspect caller state at the moment a worker would have dequeued.
	submitHook func()
}

func (f *fakeSelfRecoveryOrch) Enabled() bool { return f.enabled }

func (f *fakeSelfRecoveryOrch) HasInflightReplicationOp(_ context.Context, _, _ string) (bool, error) {
	return f.inflightOp, f.inflightErr
}

func (f *fakeSelfRecoveryOrch) SubmitRecovery(_ context.Context, _, _ string, fromBootstrap bool) bool {
	f.submitCalls++
	f.gotFromBootstrap = fromBootstrap
	if f.submitHook != nil {
		f.submitHook()
	}
	return f.submitOK
}

func (f *fakeSelfRecoveryOrch) Close(_ context.Context) error { return nil }

var _ SelfRecoveryOrchestrator = (*fakeSelfRecoveryOrch)(nil)

func newTestIndexForRecovery(t *testing.T, orch SelfRecoveryOrchestrator, raftBootstrapComplete func() bool) *Index {
	t.Helper()
	return &Index{
		Config: IndexConfig{
			RootPath:                 t.TempDir(),
			ClassName:                "C",
			SelfRecoveryOrchestrator: orch,
			RaftBootstrapComplete:    raftBootstrapComplete,
		},
		logger: logrus.New(),
	}
}

// schemaReloadCtx tags the ctx the way executor.ReloadLocalDB does.
func schemaReloadCtx() context.Context {
	return enterrors.WithStartupDBLoad(context.Background())
}

// TestShouldRecoverShardFromPeer covers the pure eligibility predicate.
func TestShouldRecoverShardFromPeer(t *testing.T) {
	t.Run("orchestrator nil", func(t *testing.T) {
		idx := &Index{Config: IndexConfig{RootPath: t.TempDir(), ClassName: "C"}, logger: logrus.New()}
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("feature disabled", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: false}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("ctx not from schema reload", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(context.Background(), "S"))
	})

	t.Run("shard dir already exists", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.NoError(t, os.MkdirAll(shardPath(idx.path(), "S"), 0o755))
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("in-flight replication op already targets the shard", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, inflightOp: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("in-flight check errors → conservative skip", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, inflightErr: errors.New("fsm unavailable")}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("eligible", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.True(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls, "predicate must not call SubmitRecovery")
	})
}

// TestRecoverShardFromPeerIfNeeded covers the install + submit
// orchestration on top of the eligibility predicate. The install must
// happen BEFORE SubmitRecovery or a fast-firing worker can clobber
// state — see install_before_submit_ordering subtest.
func TestRecoverShardFromPeerIfNeeded(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()

	t.Run("not eligible → false, no submit, no install", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: false}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls)
		require.Nil(t, idx.shards.Load("S"), "no wrapper must be installed when the predicate rejects")
	})

	t.Run("happy path → true, wrapper installed, submitted once", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
		shard := idx.shards.Load("S")
		require.NotNil(t, shard, "wrapper must remain in i.shards on the happy path")
		_, isRecovering := shard.(*RecoveringShard)
		require.True(t, isRecovering, "installed entry must be a *RecoveringShard")
	})

	t.Run("install_before_submit_ordering", func(t *testing.T) {
		// Regression: the wrapper must be in i.shards by the time
		// SubmitRecovery is called, so a worker that dequeues
		// immediately cannot overtake the install.
		var sawWrapperAtSubmit bool
		var idx *Index
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx = newTestIndexForRecovery(t, orch, nil)
		orch.submitHook = func() {
			if s := idx.shards.Load("S"); s != nil {
				_, sawWrapperAtSubmit = s.(*RecoveringShard)
			}
		}
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.True(t, sawWrapperAtSubmit, "wrapper must be installed BEFORE SubmitRecovery")
	})

	t.Run("queue_full_reverts_wrapper", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: false}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls, "submission is attempted exactly once")
		require.Nil(t, idx.shards.Load("S"),
			"wrapper must be reverted from i.shards when SubmitRecovery declines, so the caller's normal-init path can create the shard cleanly")
	})

	t.Run("fromBootstrap propagation", func(t *testing.T) {
		cases := []struct {
			name                  string
			raftBootstrapComplete func() bool
			wantFromBootstrap     bool
		}{
			{"during bootstrap (not complete)", func() bool { return false }, true},
			{"post bootstrap (complete)", func() bool { return true }, false},
			{"nil hook → treated as post-bootstrap", nil, false},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
				idx := newTestIndexForRecovery(t, orch, tc.raftBootstrapComplete)
				require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
				require.Equal(t, tc.wantFromBootstrap, orch.gotFromBootstrap)
			})
		}
	})
}

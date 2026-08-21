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

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeSelfRecoveryOrch struct {
	enabled  bool
	submitOK bool

	submitCalls      int
	gotFromBootstrap bool
	submitHook       func()
}

func (f *fakeSelfRecoveryOrch) Enabled() bool { return f.enabled }

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

func schemaReloadCtx() context.Context {
	return enterrors.WithStartupDBLoad(context.Background())
}

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

	t.Run("eligible", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.True(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls, "predicate must not call SubmitRecovery")
	})
}

// The wrapper install must happen before SubmitRecovery, else a fast worker can clobber state.
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

	t.Run("resuming self-recovery → wrapper installed, NOT submitted, no empty dir", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(true)
		idx.SetReplicationFSMReader(fsm)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls, "a resuming op must not be re-submitted")
		_, isRecovering := idx.shards.Load("S").(*RecoveringShard)
		require.True(t, isRecovering, "resuming op must install a load-blocking wrapper")
		require.NoDirExists(t, shardPath(idx.path(), "S"), "no empty live dir may be planted for the resuming op")
	})

	t.Run("non-self-recovery in-flight op → false, no install, no submit", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(false)
		fsm.EXPECT().HasActiveTargetReplicationForShard("C", "S", "node1").Return(true)
		idx.SetReplicationFSMReader(fsm)
		require.False(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Zero(t, orch.submitCalls)
		require.Nil(t, idx.shards.Load("S"))
	})

	t.Run("in-flight op elsewhere → recovery proceeds", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		idx.getSchema = &fakeSchemaGetter{}
		fsm := replicationTypes.NewMockReplicationFSMReader(t)
		fsm.EXPECT().HasActiveSelfRecoveryTargetingShard("C", "S", "node1").Return(false)
		fsm.EXPECT().HasActiveTargetReplicationForShard("C", "S", "node1").Return(false)
		idx.SetReplicationFSMReader(fsm)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
	})

	t.Run("nil FSM reader counts as no in-flight op", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
		require.Equal(t, 1, orch.submitCalls)
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

// Pins X1: a forced load mid-copy must not clear the block nor plant an empty live dir.
func TestLoadLocalShardLeavesRecoveringShardUntouched(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch, nil)
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.getSchema = &fakeSchemaGetter{}

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
	rec, ok := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, ok)

	require.NoError(t, idx.LoadLocalShard(context.Background(), "S", false))
	require.NoDirExists(t, shardPath(idx.path(), "S"))
	require.True(t, rec.isLoadBlocked())
	_, stillRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, stillRecovering)
}

// Pins R2: a recovering shard yields nil (remote fallback) on the direct-local
// path and a 422-mappable, sentinel-preserving error on the GetShard path.
func TestRecoveringShardReadPaths(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch, nil)
	idx.closingCtx = context.Background()
	idx.shardCreateLocks = esync.NewKeyRWLocker()

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))

	shard, release, err := idx.getShardForDirectLocalOperation(context.Background(), "", "S", localShardOperationRead, 0)
	defer release()
	require.NoError(t, err)
	require.Nil(t, shard, "direct local operation must fall back to a remote replica")

	_, release2, err := idx.GetShard(context.Background(), "S")
	defer release2()
	var unprocessable enterrors.ErrUnprocessable
	require.True(t, errors.As(err, &unprocessable), "got %T: %v", err, err)
	require.True(t, enterrors.IsShardRecovering(err))
}

func TestForEachShardSkipRecovering(t *testing.T) {
	class := &models.Class{Class: "C"}
	promMetrics := monitoring.GetMetrics()
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	idx := newTestIndexForRecovery(t, orch, nil)
	idx.closingCtx = context.Background()

	require.True(t, idx.recoverShardFromPeerIfNeeded(schemaReloadCtx(), class, "S", promMetrics))
	_, isRecovering := idx.shards.Load("S").(*RecoveringShard)
	require.True(t, isRecovering)

	visited := 0
	require.NoError(t, idx.forEachShardSkipRecovering(func(name string, shard ShardLike) error {
		visited++
		return nil
	}))
	require.Zero(t, visited, "recovering shard must be skipped")

	require.NotPanics(t, func() {
		require.NoError(t, idx.addProperty(context.Background(), &models.Property{
			Name: "category", DataType: []string{"text"},
		}))
	})
}

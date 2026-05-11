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
)

// fakeSelfRecoveryOrch is a stub SelfRecoveryOrchestrator for unit tests.
type fakeSelfRecoveryOrch struct {
	enabled     bool
	inflightOp  bool
	inflightErr error
	submitOK    bool

	submitCalls      int
	gotFromBootstrap bool
}

func (f *fakeSelfRecoveryOrch) Enabled() bool { return f.enabled }

func (f *fakeSelfRecoveryOrch) HasInflightReplicationOp(_ context.Context, _, _ string) (bool, error) {
	return f.inflightOp, f.inflightErr
}

func (f *fakeSelfRecoveryOrch) SubmitRecovery(_ context.Context, _, _ string, fromBootstrap bool) bool {
	f.submitCalls++
	f.gotFromBootstrap = fromBootstrap
	return f.submitOK
}

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

func TestShouldRecoverShardFromPeer(t *testing.T) {
	t.Run("orchestrator nil", func(t *testing.T) {
		idx := &Index{Config: IndexConfig{RootPath: t.TempDir(), ClassName: "C"}, logger: logrus.New()}
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
	})

	t.Run("feature disabled", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: false}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls)
	})

	t.Run("ctx not from schema reload", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(context.Background(), "S"))
		require.Zero(t, orch.submitCalls, "must not submit when not a schema-reload AddClass")
	})

	t.Run("shard dir already exists", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.NoError(t, os.MkdirAll(shardPath(idx.path(), "S"), 0o755))
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls)
	})

	t.Run("in-flight replication op already targets the shard", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, inflightOp: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls, "must not submit a duplicate op while one is in flight")
	})

	t.Run("in-flight check errors → conservative skip", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, inflightErr: errors.New("fsm unavailable"), submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Zero(t, orch.submitCalls)
	})

	t.Run("submission declined (queue full) → false", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: false}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.False(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Equal(t, 1, orch.submitCalls, "submission is attempted exactly once")
	})

	t.Run("happy path → true, submitted once", func(t *testing.T) {
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		idx := newTestIndexForRecovery(t, orch, nil)
		require.True(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
		require.Equal(t, 1, orch.submitCalls)
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
				require.True(t, idx.shouldRecoverShardFromPeer(schemaReloadCtx(), "S"))
				require.Equal(t, tc.wantFromBootstrap, orch.gotFromBootstrap)
			})
		}
	})
}

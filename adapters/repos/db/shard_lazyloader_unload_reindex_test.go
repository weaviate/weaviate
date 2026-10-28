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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLazyLoadShard_TryUnloadNeverWaitsOnTheReindexLookup(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var enterOnce, unblockOnce sync.Once
	release := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	f.index.db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		enterOnce.Do(func() { close(entered) })
		<-unblock
		return func(string, string) bool { return false }
	})

	done := make(chan UnloadOutcome, 1)
	go func() { done <- f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest}) }()

	select {
	case <-entered:
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- f.index.Shutdown(context.Background()) }()
		select {
		case <-shutdownDone:
		case <-time.After(2 * time.Second):
			release()
			require.FailNow(t, "an index shutdown waited on the unloader's reindex lookup")
		}
		release()
		require.FailNow(t, "TryUnload consulted the reindex lookup")
	case <-done:
	case <-time.After(10 * time.Second):
		release()
		require.FailNow(t, "TryUnload blocked")
	}

	shard, rel, err := f.index.pinResidentShard(f.name, false)
	require.NoError(t, err)
	require.NotNil(t, shard)
	rel()
	outcome, err := f.index.UnloadLocalShard(testCtx(), f.name)
	require.NoError(t, err)
	require.Equal(t, ShardUnloadOutcomeUnloaded, outcome)
}

func TestLazyLoadShard_TryUnloadReindexSnapshot(t *testing.T) {
	failing := func(context.Context) (ShardReindexActivityLookup, error) {
		return nil, errors.New("leader unreachable")
	}
	liveHere := func(context.Context) (ShardReindexActivityLookup, error) {
		return func(string, string) bool { return true }, nil
	}
	tests := []struct {
		name     string
		disabled bool
		builder  ShardReindexActivitySnapshotBuilder
		cleanup  bool
		zero     bool
		want     UnloadOutcome
	}{
		{name: "no snapshot taken", zero: true, want: UnloadOutcomeReindexUnknown},
		{name: "unwired lookup", want: UnloadOutcomeOK},
		{name: "failed lookup", builder: failing, want: UnloadOutcomeReindexUnknown},
		{name: "live reindex", builder: liveHere, want: UnloadOutcomeReindex},
		{name: "terminal cleanup in progress", cleanup: true, want: UnloadOutcomeReindex},
		{name: "runtime reindex disabled", disabled: true, zero: true, want: UnloadOutcomeOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t, func(c *Config) { c.RuntimeReindexDisabled = tt.disabled })
			f.put(t)
			if tt.builder != nil {
				f.index.db.SetShardReindexActivitySnapshot(tt.builder)
			}
			if tt.cleanup {
				f.index.db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
					return func(string, string) bool { return true }
				})
			}
			req := UnloadRequest{Mode: UnloadModeRequest}
			if !tt.zero {
				snap, err := f.index.db.ReindexActivitySnapshot(testCtx())
				require.Equal(t, tt.builder != nil && tt.want == UnloadOutcomeReindexUnknown, err != nil)
				req.Reindex = snap
			}
			require.Equal(t, tt.want, tryUnloadPromptly(t, f.lazy, req))
		})
	}
}

func TestReindexActivitySnapshotHonoursItsContext(t *testing.T) {
	f := newUnloadFixture(t)
	f.index.db.SetShardReindexActivitySnapshot(func(ctx context.Context) (ShardReindexActivityLookup, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	snap, err := f.index.db.ReindexActivitySnapshot(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, snap.Usable())
}

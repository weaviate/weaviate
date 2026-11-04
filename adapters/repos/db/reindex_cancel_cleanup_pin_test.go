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
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	pinSweepProp    = "category"
	pinSweepSidecar = "property_category__enable_filterable_ingest_1"
	pinSweepCold    = "cold-tenant"
)

// onLogMessage runs fn synchronously when an entry with msg is logged.
type onLogMessage struct {
	msg string
	fn  func()
}

func (h onLogMessage) Levels() []logrus.Level { return logrus.AllLevels }

func (h onLogMessage) Fire(e *logrus.Entry) error {
	if e.Message == h.msg {
		h.fn()
	}
	return nil
}

func loggerWithHooks(hooks ...logrus.Hook) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.DebugLevel)
	for _, h := range hooks {
		logger.AddHook(h)
	}
	return logger
}

func newPinSweepIndex(t *testing.T, ctx context.Context, logger logrus.FieldLogger) (*Shard, *Index) {
	t.Helper()
	class := newTestClassWithProps("PinSweep_"+uuid.NewString()[:8], []string{pinSweepProp})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false,
		func(idx *Index) {
			idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
			if logger != nil {
				idx.logger = logger
			}
		})
	hot := shd.(*Shard)
	t.Cleanup(func() { hot.Shutdown(context.Background()) })
	return hot, idx
}

func addColdPinSweepShard(t *testing.T, ctx context.Context, idx *Index, allocChecker memwatch.AllocChecker) *LazyLoadShard {
	t.Helper()
	mkSidecarDir(t, shardPathLSM(idx.path(), pinSweepCold), pinSweepSidecar)
	cold := NewLazyLoadShard(ctx, nil, pinSweepCold, idx, idx.getClass(), idx.centralJobQueue,
		allocChecker, idx.shardLoadLimiter, idx.shardReindexer, false, idx.bitmapBufPool)
	idx.shards.Store(pinSweepCold, cold)
	t.Cleanup(func() {
		if cold.isLoaded() {
			require.NoError(t, cold.Shutdown(context.Background()))
		}
	})
	return cold
}

func TestCleanStalePartialReindexStateHoldsColdShardAgainstUnload(t *testing.T) {
	ctx := testCtx()
	var idx *Index
	unloaded := make(chan error, 1)
	probe := func() {
		enterrors.GoWrapper(func() {
			_, err := idx.UnloadLocalShard(ctx, pinSweepCold)
			unloaded <- err
		}, idx.logger)
		assert.Never(t, func() bool { return idx.shards.Load(pinSweepCold) == nil },
			200*time.Millisecond, time.Millisecond, "an unload took the shard out of the map mid-load")
	}
	_, idx = newPinSweepIndex(t, ctx, nil)
	cold := addColdPinSweepShard(t, ctx, idx, loadProbeAllocChecker{memwatch.NewDummyMonitor(), probe})

	err := idx.cleanStalePartialReindexState(ctx, pinSweepProp, "filterable", nil, nil)
	require.ErrorIs(t, err, ErrCleanupShardFailed)

	require.NoError(t, <-unloaded)
	require.Nil(t, idx.shards.Load(pinSweepCold))
	require.False(t, cold.isLoaded())
}

func TestCleanStalePartialReindexStatePinsTheSweptShard(t *testing.T) {
	tests := []struct {
		name string
		cold bool
	}{
		{name: "loaded shard"},
		{name: "cold shard loaded by the sweep", cold: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			var target func() *Shard
			var pinsDuringSweep atomic.Int64
			pinsDuringSweep.Store(-1)
			hook := onLogMessage{msg: "partial-reindex cleanup: sidecar buckets shut down", fn: func() {
				if s := target(); s != nil {
					pinsDuringSweep.Store(s.inUseCounter.Load())
				}
			}}
			hot, idx := newPinSweepIndex(t, ctx, loggerWithHooks(hook))
			target = func() *Shard { return hot }
			if tc.cold {
				cold := addColdPinSweepShard(t, ctx, idx, idx.allocChecker)
				target = cold.currentShard
				idx.shards.LoadAndDelete(hot.Name())
			} else {
				mkSidecarDir(t, hot.pathLSM(), pinSweepSidecar)
			}

			require.NoError(t, idx.cleanStalePartialReindexState(ctx, pinSweepProp, "filterable", nil, nil))

			require.EqualValues(t, 1, pinsDuringSweep.Load(), "the swept shard was not pinned against shutdown")
			require.EqualValues(t, 0, target().inUseCounter.Load(), "the sweep did not release its pin")
		})
	}
}

func TestCleanStalePartialReindexStateInASuspendedNamespace(t *testing.T) {
	tests := []struct {
		name          string
		cold          bool
		wantTruncated bool
	}{
		{name: "loaded shard is swept"},
		{name: "cold shard is not loaded", cold: true, wantTruncated: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			hot, idx := newPinSweepIndex(t, ctx, nil)
			lsmPath := hot.pathLSM()
			var cold *LazyLoadShard
			if tc.cold {
				cold = addColdPinSweepShard(t, ctx, idx, idx.allocChecker)
				idx.shards.LoadAndDelete(hot.Name())
				lsmPath = shardPathLSM(idx.path(), pinSweepCold)
			} else {
				mkSidecarDir(t, lsmPath, pinSweepSidecar)
			}
			idx.namespace, idx.namespacesExister = "alpha", existerWithState(t, api.NamespaceStateSuspended)

			err := idx.cleanStalePartialReindexState(ctx, pinSweepProp, "filterable", nil, nil)

			_, statErr := os.Stat(filepath.Join(lsmPath, pinSweepSidecar))
			if tc.wantTruncated {
				require.ErrorIs(t, err, ErrCleanupSweepTruncated)
				require.False(t, cold.isLoaded(), "the sweep loaded a shard of a suspended namespace")
				require.NoError(t, statErr)
				return
			}
			require.NoError(t, err)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestCleanStalePartialReindexStateReportsAShardBeingTornDownAsTruncated(t *testing.T) {
	tests := []struct {
		name string
		flag func(s *Shard) *atomic.Bool
	}{
		{name: "shutdown requested", flag: func(s *Shard) *atomic.Bool { return &s.shutdownRequested }},
		{name: "drop requested", flag: func(s *Shard) *atomic.Bool { return &s.dropRequested }},
		{name: "already shut", flag: func(s *Shard) *atomic.Bool { return &s.shut }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			hot, idx := newPinSweepIndex(t, ctx, nil)
			mkSidecarDir(t, hot.pathLSM(), pinSweepSidecar)
			tc.flag(hot).Store(true)
			defer tc.flag(hot).Store(false)

			err := idx.cleanStalePartialReindexState(ctx, pinSweepProp, "filterable", nil, nil)

			require.ErrorIs(t, err, ErrCleanupSweepTruncated)
			require.NotErrorIs(t, err, ErrCleanupShardFailed)
			_, statErr := os.Stat(filepath.Join(hot.pathLSM(), pinSweepSidecar))
			require.NoError(t, statErr, "a shard being torn down must not be swept")
		})
	}
}

func TestPinShardForSweepOfAnUnmappedShard(t *testing.T) {
	ctx := testCtx()
	_, idx := newPinSweepIndex(t, ctx, nil)

	shard, release, err := idx.pinShardForSweep(ctx, "not-in-the-map")

	require.NoError(t, err)
	require.Nil(t, shard)
	require.Nil(t, release)
}

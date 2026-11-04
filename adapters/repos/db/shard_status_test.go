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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// storePaused probes below every shard check: a paused bucket refuses on its own.
func storePaused(t *testing.T, shard *Shard) bool {
	t.Helper()
	key := []byte("probe")
	err := shard.store.Bucket(helpers.ObjectsBucketLSM).Put(key, key,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, key))
	if errors.Is(err, storagestate.ErrStatusReadOnly) {
		return true
	}
	require.NoError(t, err)
	return false
}

func TestShardStatusFollowsTheOverlay(t *testing.T) {
	tests := []struct {
		name       string
		pressure   bool
		reindexing bool
		wantStatus storagestate.Status
		wantPaused bool
		wantErr    string
	}{
		{
			name:       "no condition",
			wantStatus: storagestate.StatusReady,
		},
		{
			name:       "resource pressure",
			pressure:   true,
			wantStatus: storagestate.StatusReadOnly,
			wantPaused: true,
			wantErr:    "store is read-only due to: resource pressure",
		},
		{
			name:       "reindex keeps its plain error",
			reindexing: true,
			wantStatus: storagestate.StatusReadOnly,
			wantPaused: true,
			wantErr:    storagestate.ErrStatusReadOnly.Error(),
		},
		{
			name:       "both report the pressure",
			pressure:   true,
			reindexing: true,
			wantStatus: storagestate.StatusReadOnly,
			wantPaused: true,
			wantErr:    "store is read-only due to: resource pressure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy, idx := testShard(t, t.Context(), "ShardStatusView")
			s := underlyingShard(t, lazy)

			require.NoError(t, s.setReindexing(tc.reindexing))
			setResourcePressure(t, idx, s, tc.pressure)

			require.Equal(t, tc.wantStatus, s.GetStatus())
			require.Equal(t, tc.wantPaused, storePaused(t, s))
			if tc.wantErr == "" {
				require.NoError(t, s.refuseWhilePaused())
			} else {
				require.EqualError(t, s.refuseWhilePaused(), tc.wantErr)
			}
		})
	}
}

func TestShardConditionsLiftIndependently(t *testing.T) {
	tests := []struct {
		name  string
		first func(t *testing.T, idx *Index, s *Shard)
		last  func(t *testing.T, idx *Index, s *Shard)
	}{
		{
			name: "pressure lifts during a reindex",
			first: func(t *testing.T, idx *Index, s *Shard) {
				setResourcePressure(t, idx, s, false)
			},
			last: func(t *testing.T, idx *Index, s *Shard) {
				require.NoError(t, s.setReindexing(false))
			},
		},
		{
			name: "reindex ends under pressure",
			first: func(t *testing.T, idx *Index, s *Shard) {
				require.NoError(t, s.setReindexing(false))
			},
			last: func(t *testing.T, idx *Index, s *Shard) {
				setResourcePressure(t, idx, s, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy, idx := testShard(t, t.Context(), "ShardConditionsLift")
			s := underlyingShard(t, lazy)

			require.NoError(t, s.setReindexing(true))
			setResourcePressure(t, idx, s, true)

			tc.first(t, idx, s)
			require.Equal(t, storagestate.StatusReadOnly, s.GetStatus())
			require.True(t, storePaused(t, s), "the other condition must keep holding the store")
			require.Error(t, s.refuseWhilePaused())

			tc.last(t, idx, s)
			require.Equal(t, storagestate.StatusReady, s.GetStatus())
			require.False(t, storePaused(t, s))
			require.NoError(t, s.refuseWhilePaused())
		})
	}
}

// Reading the overlay outside statusLock lets a stale pause land after a newer resume.
func TestShardSyncStorePause_ConcurrentFlipsSettleOnTheOverlay(t *testing.T) {
	lazy, idx := testShard(t, t.Context(), "ShardSyncConcurrent")
	s := underlyingShard(t, lazy)

	for i := range 500 {
		pressure := i%2 == 0
		reindexing := i%3 == 0

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			idx.db.nodeOverlay.SetResourcePressure(pressure)
			errs[0] = s.syncStorePause()
		}()
		go func() {
			defer wg.Done()
			<-start
			errs[1] = s.setReindexing(reindexing)
		}()
		close(start)
		wg.Wait()

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		require.Equal(t, pressure || reindexing, storePaused(t, s), "iteration %d", i)
	}
}

func TestShardMarkShutdown_LiftsPauseForGood(t *testing.T) {
	tests := []struct {
		name   string
		holdBy func(t *testing.T, idx *Index, s *Shard)
	}{
		{
			name: "held by pressure",
			holdBy: func(t *testing.T, idx *Index, s *Shard) {
				setResourcePressure(t, idx, s, true)
			},
		},
		{
			name: "held by a reindex",
			holdBy: func(t *testing.T, idx *Index, s *Shard) {
				require.NoError(t, s.setReindexing(true))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy, idx := testShard(t, t.Context(), "ShardMarkShutdown")
			s := underlyingShard(t, lazy)

			tc.holdBy(t, idx, s)
			require.True(t, storePaused(t, s))

			s.markShutdown()
			require.Equal(t, storagestate.StatusShutdown, s.GetStatus())
			require.False(t, storePaused(t, s))

			require.NoError(t, s.syncStorePause())
			require.False(t, storePaused(t, s), "a sweep must not pause a store going down")
		})
	}
}

func TestShardDropsItsPhase(t *testing.T) {
	tests := []struct {
		name    string
		discard func(t *testing.T, s *Shard)
	}{
		{
			name: "shutdown",
			discard: func(t *testing.T, s *Shard) {
				require.NoError(t, s.Shutdown(context.Background()))
			},
		},
		{
			name: "drop",
			discard: func(t *testing.T, s *Shard) {
				require.NoError(t, s.drop(false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy, idx := testShard(t, t.Context(), "ShardDropsPhase")
			s := underlyingShard(t, lazy)
			class := idx.Config.ClassName.String()

			phase := idx.nodeOverlay().PhaseOf(class, s.name)
			require.NotNil(t, phase)
			require.Equal(t, storagestate.StatusReady, phase.Status())

			tc.discard(t, s)

			require.Nil(t, idx.nodeOverlay().PhaseOf(class, s.name))
		})
	}
}

// Loading to answer would spend the memory a resource sweep is reacting to.
func TestLazyLoadShardConditionsDoNotLoad(t *testing.T) {
	shard, idx := testShard(t, t.Context(), "LazyConditionsDoNotLoad")
	loaded := underlyingShard(t, shard)

	tests := []struct {
		name     string
		shard    *LazyLoadShard
		pressure bool
		loaded   bool
		wantErr  string
	}{
		{
			name:  "unloaded shard",
			shard: newColdShard(idx, "cold_tenant"),
		},
		{
			name:     "unloaded shard under pressure",
			shard:    newColdShard(idx, "cold_tenant"),
			pressure: true,
			wantErr:  "store is read-only due to: resource pressure",
		},
		{
			name:   "loaded shard",
			shard:  newLoadedLazyShard(loaded),
			loaded: true,
		},
		{
			name:     "loaded shard under pressure",
			shard:    newLoadedLazyShard(loaded),
			pressure: true,
			loaded:   true,
			wantErr:  "store is read-only due to: resource pressure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx.db.nodeOverlay.SetResourcePressure(tc.pressure)
			t.Cleanup(func() { setResourcePressure(t, idx, loaded, false) })

			require.NoError(t, tc.shard.syncStorePause())
			err := tc.shard.refuseWhilePaused()

			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}
			require.Equal(t, tc.loaded, tc.shard.isLoaded())
			if tc.loaded {
				assert.Equal(t, tc.pressure, storePaused(t, loaded))
			}
		})
	}
}

func TestUnloadedLazyShardReportsWriteRefusals(t *testing.T) {
	_, idx := testShard(t, t.Context(), "LazyUserReadOnly")
	cold := newColdShard(idx, "cold_tenant")

	tests := []struct {
		name     string
		readOnly bool
		pressure bool
		want     storagestate.Status
	}{
		{name: "writable", want: storagestate.StatusLazyLoading},
		{name: "user read-only", readOnly: true, want: storagestate.StatusReadOnly},
		{name: "resource pressure", pressure: true, want: storagestate.StatusReadOnly},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx.db.nodeOverlay.SetReadOnly(idx.Config.ClassName.String(), "cold_tenant", tt.readOnly)
			idx.db.nodeOverlay.SetResourcePressure(tt.pressure)

			report := idx.reportedStatus("cold_tenant", cold)

			assert.Equal(t, tt.want.String(), report.Status)
			assert.False(t, cold.isLoaded())
		})
	}
}

func TestLazyLoadShardReportsReindex(t *testing.T) {
	shard, _ := testShard(t, t.Context(), "LazyReportsReindex")
	loaded := underlyingShard(t, shard)
	lazy := newLoadedLazyShard(loaded)

	require.NoError(t, lazy.setReindexing(true))
	require.ErrorIs(t, lazy.refuseWhilePaused(), storagestate.ErrStatusReadOnly)

	require.NoError(t, lazy.setReindexing(false))
	require.NoError(t, lazy.refuseWhilePaused())
}

// The paused store's refusal must not skip the other shards or the index's config.
func TestPausedShardAppliesTheVectorConfigOnResume(t *testing.T) {
	tests := []struct {
		name  string
		pause func(t *testing.T, idx *Index, shard *Shard, on bool)
	}{
		{name: "reindex", pause: func(t *testing.T, _ *Index, shard *Shard, on bool) {
			require.NoError(t, shard.setReindexing(on))
		}},
		{name: "freeze upload", pause: func(t *testing.T, _ *Index, shard *Shard, on bool) {
			shard.phase.SetTransferring(on)
			require.NoError(t, shard.syncStorePause())
		}},
		{name: "resource pressure", pause: func(t *testing.T, idx *Index, shard *Shard, on bool) {
			setResourcePressure(t, idx, shard, on)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			sl, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PausedVectorConfig"}, enthnsw.NewDefaultUserConfig(), false, false)
			shard := underlyingShard(t, sl)
			shardEF := func() int64 {
				var ef int64
				_, err := shard.WithVectorIndex("", func(index VectorIndex) error {
					ef = index.(interface{ EF() int64 }).EF()
					return nil
				})
				require.NoError(t, err)
				return ef
			}
			cfg := enthnsw.NewDefaultUserConfig()
			cfg.EF = 321

			tt.pause(t, idx, shard, true)
			require.Error(t, idx.updateVectorIndexConfig(ctx, cfg))
			require.Equal(t, 321, idx.GetVectorIndexConfig("").(enthnsw.UserConfig).EF, "later loads read the index's config")
			require.NotEqual(t, int64(321), shardEF())

			tt.pause(t, idx, shard, false)
			require.Eventually(t, func() bool { return shardEF() == 321 }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

// A leaked halt keeps compaction and async replication off until a restart.
func TestHaltForTransferPanicReleasesTheHalt(t *testing.T) {
	sl, _ := testShard(t, t.Context(), "HaltPanic")
	shard := underlyingShard(t, sl)
	var original VectorIndex
	_, err := shard.WithVectorIndex("", func(index VectorIndex) error {
		original = index
		return nil
	})
	require.NoError(t, err)
	panicking := NewMockVectorIndex(t)
	panicking.EXPECT().PrepareForBackup(mock.Anything).RunAndReturn(func(context.Context) error { panic("prepare") })
	panicking.EXPECT().ResumeAfterBackup(mock.Anything).Return(nil).Maybe()
	shard.setVectorIndex("", panicking)
	defer shard.setVectorIndex("", original)

	err = shard.HaltForTransfer(t.Context(), true, 0)

	require.ErrorContains(t, err, "halt for transfer panicked: prepare")
	require.Zero(t, shard.haltForTransferCount.Load())
}

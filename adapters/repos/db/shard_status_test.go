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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
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

// The shard's status, store pause and write refusal follow the overlay, one condition at a time.
func TestShardFollowsTheOverlay(t *testing.T) {
	type step struct {
		do         func(t *testing.T, idx *Index, s *Shard)
		wantStatus storagestate.Status
		wantPaused bool
		wantErr    string
	}
	pressure := func(on bool) func(*testing.T, *Index, *Shard) {
		return func(t *testing.T, idx *Index, s *Shard) { setResourcePressure(t, idx, s, on) }
	}
	reindex := func(on bool) func(*testing.T, *Index, *Shard) {
		return func(t *testing.T, _ *Index, s *Shard) { require.NoError(t, s.setReindexing(on)) }
	}
	shutdown := func(t *testing.T, _ *Index, s *Shard) {
		s.markShutdown()
		require.NoError(t, s.syncStorePause(), "a sweep must not pause a store going down")
	}
	const pressureErr = "store is read-only due to: resource pressure"
	reindexErr := storagestate.ErrStatusReadOnly.Error()
	ready := step{wantStatus: storagestate.StatusReady}
	withDo := func(st step, do func(*testing.T, *Index, *Shard)) step { st.do = do; return st }

	tests := []struct {
		name  string
		steps []step
	}{
		{name: "pressure", steps: []step{
			{do: pressure(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: pressureErr},
			withDo(ready, pressure(false)),
		}},
		{name: "a reindex keeps its plain error", steps: []step{
			{do: reindex(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: reindexErr},
			withDo(ready, reindex(false)),
		}},
		{name: "pressure lifts during a reindex", steps: []step{
			{do: reindex(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: reindexErr},
			{do: pressure(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: pressureErr},
			{do: pressure(false), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: reindexErr},
			withDo(ready, reindex(false)),
		}},
		{name: "a reindex ends under pressure", steps: []step{
			{do: reindex(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: reindexErr},
			{do: pressure(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: pressureErr},
			{do: reindex(false), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: pressureErr},
			withDo(ready, pressure(false)),
		}},
		{name: "shutdown lifts a pause for good", steps: []step{
			{do: reindex(true), wantStatus: storagestate.StatusReadOnly, wantPaused: true, wantErr: reindexErr},
			{do: shutdown, wantStatus: storagestate.StatusShutdown, wantErr: reindexErr},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lazy, idx := testShard(t, t.Context(), "ShardFollowsTheOverlay")
			s := underlyingShard(t, lazy)
			for i, st := range tc.steps {
				st.do(t, idx, s)
				require.Equal(t, st.wantStatus, s.GetStatus(), "step %d", i)
				require.Equal(t, st.wantPaused, storePaused(t, s), "step %d", i)
				if st.wantErr == "" {
					require.NoError(t, s.isReadOnly(), "step %d", i)
				} else {
					require.EqualError(t, s.isReadOnly(), st.wantErr, "step %d", i)
				}
			}
		})
	}
}

// A PUT runs inside the RAFT apply, so it must not wait on a tenant that is loading.
func TestUpdateShardStatusDoesNotWaitOnALoadingShard(t *testing.T) {
	_, idx := testShard(t, t.Context(), "LoadingShardStatus")
	loading := newColdShard(idx, "loading")
	idx.shards.Store("loading", loading)
	loading.mutex.Lock() // a load holds it throughout
	defer loading.mutex.Unlock()

	done := make(chan error, 1)
	enterrors.GoWrapper(func() {
		done <- idx.updateShardStatus(t.Context(), "loading", storagestate.StatusReady.String())
	}, idx.logger)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the PUT waited on the loading shard")
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
			err := tc.shard.isReadOnly()

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
	idx.shards.Store("cold_tenant", cold)

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

			report, err := idx.localShardStatus(t.Context(), "cold_tenant")

			require.NoError(t, err)
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
	require.ErrorIs(t, lazy.isReadOnly(), storagestate.ErrStatusReadOnly)

	require.NoError(t, lazy.setReindexing(false))
	require.NoError(t, lazy.isReadOnly())
}

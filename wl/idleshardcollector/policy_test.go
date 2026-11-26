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

package idleshardcollector

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

func cold(h *harness) *fakeShard {
	return &fakeShard{reg: h.reg}
}

func tickOnce(interval time.Duration) {
	time.Sleep(interval)
	synctest.Wait()
}

func idleKnobs(k *testKnobs) {
	set(nil, k.idleTTL, 2*time.Second)
}

func TestIdleSweepRetriesARefusedShard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.idleTTL, 40*time.Second) })
		busy := h.loadN(1)[0]
		busy.refuse.Store(true)
		time.Sleep(time.Minute)
		synctest.Wait()

		require.True(t, busy.loaded(), "a refused shutdown keeps the shard loaded")
		require.Equal(t, db.UnloadRequest{IdleTTL: 40 * time.Second}, busy.requests()[0])
		require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("failed")))

		busy.refuse.Store(false)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.False(t, busy.loaded())
		require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("ok")))
	})
}

func TestIdleSweepWalksTheRegistryInChunks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.idleTTL, 10*time.Second)
			set(t, k.rate, 10)
		})
		shards := make([]*fakeShard, idleSweepChunk+3)
		for i := range shards {
			shards[i] = h.reg.load()
		}
		busy, idle := shards[:idleSweepChunk], shards[idleSweepChunk:]
		for range 12 {
			for _, s := range busy {
				s.touch()
			}
			tickOnce(time.Second)
		}
		require.Equal(t, idleSweepChunk, h.loadedCount(busy))
		require.Zero(t, h.loadedCount(idle), "shards past the first chunk are swept")
		h.reg.mu.Lock()
		defer h.reg.mu.Unlock()
		require.GreaterOrEqual(t, h.reg.chunks, 2)
	})
}

func TestIdleShardIsNeverUnloadedBeforeIdleTTL(t *testing.T) {
	for _, idleTTL := range []time.Duration{time.Second, 1500 * time.Millisecond, 2100 * time.Millisecond} {
		t.Run(idleTTL.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.idleTTL, idleTTL)
					set(t, k.checkInterval, 50*time.Millisecond)
				})
				shard := h.reg.load()
				time.Sleep(950 * time.Millisecond)
				shard.touch()
				touched := time.Now()

				for time.Since(touched) < idleTTL+2*time.Second {
					time.Sleep(50 * time.Millisecond)
					synctest.Wait()
					if idle := time.Since(touched); idle < idleTTL {
						require.True(t, shard.loaded(), "unloaded after %v idle with idle_ttl %v", idle, idleTTL)
					}
				}
				require.False(t, shard.loaded())
				require.Len(t, shard.requests(), 1, "selection and TryUnload agree on when the shard is idle")
			})
		})
	}
}

func TestIdleSweepRunsEveryCheckIntervalAndResumesWhenCutShort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.checkInterval, time.Hour)
			set(t, k.rate, 2)
		})
		shards := h.loadN(5)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		set(t, h.knobs.idleTTL, 10*time.Second)
		h.p.refreshConfig()
		synctest.Wait()
		require.Equal(t, 3, h.loadedCount(shards))
		for _, want := range []int{1, 0} {
			tickOnce(time.Hour)
			require.Equal(t, want, h.loadedCount(shards), "a sweep cut short by unload_rate resumes on the next check")
		}

		late := h.loadN(1)[0]
		time.Sleep(30 * time.Minute)
		synctest.Wait()
		require.True(t, late.loaded(), "the next sweep waits for check_interval")
		time.Sleep(time.Hour)
		synctest.Wait()
		require.False(t, late.loaded())
	})
}

func TestKnobChangesTakeEffectWithinOneTick(t *testing.T) {
	tests := []struct {
		name   string
		before func(*testKnobs)
		change func(t *testing.T, k *testKnobs)
		want   int
		swept  bool
	}{
		{name: "setting idle_ttl", change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 30*time.Second) }, swept: true},
		{
			name:   "setting idle_ttl while paused",
			before: func(k *testKnobs) { set(nil, k.paused, true) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 30*time.Second) },
			want:   6,
		},
		{
			name:   "unpausing",
			before: func(k *testKnobs) { set(nil, k.paused, true); set(nil, k.idleTTL, 30*time.Second) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.paused, false) },
			swept:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.checkInterval, 10*time.Second)
					if tt.before != nil {
						tt.before(k)
					}
				})
				shards := h.loadN(6)
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, 6, h.loadedCount(shards))
				require.False(t, h.p.collecting())

				tt.change(t, h.knobs)
				tickOnce(10 * time.Second)
				require.Equal(t, tt.want, h.loadedCount(shards))
				require.Equal(t, tt.swept, h.p.collecting())
				h.reg.mu.Lock()
				defer h.reg.mu.Unlock()
				require.Equal(t, tt.swept, h.reg.chunks > 0, "a paused tick never walks the registry")
			})
		})
	}
}

func TestWorkerPoolResizesLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.workers, 4) })
		synctest.Wait()
		require.EqualValues(t, 4, h.p.workersRunning.Load())
		for _, n := range []int{1, 8, 2, MaxUnloadWorkers + 5, 0} {
			set(t, h.knobs.workers, n)
			tickOnce(time.Second)
			require.EqualValues(t, min(max(n, 1), MaxUnloadWorkers), h.p.workersRunning.Load(), "workers=%d", n)
		}
	})
}

func TestRetiredWorkerStartsNoQueuedUnload(t *testing.T) {
	const workers = 16
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.checkInterval, time.Hour)
			set(t, k.workers, workers)
			set(t, k.rate, 4*workers)
		})
		first, queued := make(chan struct{}), make(chan struct{})
		shards := make([]*fakeShard, 2*workers)
		for i := range shards {
			shards[i] = h.reg.load()
			shards[i].block.Store(&first)
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		set(t, h.knobs.idleTTL, 2*time.Second)
		h.p.refreshConfig()
		synctest.Wait()
		inFlight := func() int {
			n := 0
			for _, s := range shards {
				if len(s.requests()) > 0 && s.loaded() {
					n++
				}
			}
			return n
		}
		require.Equal(t, workers, inFlight())
		for _, s := range shards {
			if len(s.requests()) == 0 {
				require.True(t, s.pending.Load())
				s.block.Store(&queued)
			}
		}

		set(t, h.knobs.workers, 1)
		h.p.refreshConfig()
		synctest.Wait()
		close(first)
		synctest.Wait()
		require.LessOrEqual(t, inFlight(), 1, "a retired worker started a queued unload")
		require.EqualValues(t, 1, h.p.workersRunning.Load())

		close(queued)
		synctest.Wait()
		tickOnce(time.Hour)
		require.Zero(t, h.loadedCount(shards), "a handoff a retired worker dropped is requeued")
		require.Zero(t, h.p.pending.Load())
	})
}

func TestUnloadPanicKeepsTheWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, idleKnobs)
		shards := h.loadN(2)
		shards[0].panics.Store(true)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.True(t, shards[0].loaded())
		require.False(t, shards[0].pending.Load())
		require.EqualValues(t, 1, h.p.workersRunning.Load())
		require.False(t, shards[1].loaded(), "the same worker keeps unloading")
		require.Zero(t, h.p.pending.Load())
	})
}

func TestCloseStopsEveryGoroutineAndReleasesQueuedShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			idleKnobs(k)
			set(t, k.workers, 1)
		})
		gate := make(chan struct{})
		first := h.reg.load()
		first.block.Store(&gate)
		time.Sleep(time.Second)
		shards := append([]*fakeShard{first}, h.loadN(2)...)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.Len(t, shards[0].requests(), 1)
		require.True(t, shards[1].pending.Load())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, h.p.close(ctx), context.DeadlineExceeded, "close waits for an unload in flight")
		require.False(t, shards[1].pending.Load(), "a timed-out close must not strand a queued claim")
		require.True(t, shards[1].loaded())
		close(gate)
		require.NoError(t, h.p.close(context.Background()))
		require.NoError(t, h.p.close(context.Background()))
		require.Zero(t, h.p.workersRunning.Load())
		require.Zero(t, h.p.pending.Load())
		require.False(t, h.p.collecting())

		h.p.beforeLoad(cold(h))
		h.p.refreshConfig()
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Empty(t, shards[1].requests())
		require.Empty(t, shards[2].requests())
	})
}

func TestBeforeLoadAllocatesNothing(t *testing.T) {
	h := newHarness(t, func(k *testKnobs) { set(t, k.checkInterval, time.Hour) })
	observed := func() uint64 {
		var m dto.Metric
		require.NoError(t, h.p.metrics.reloadAfterUnload.Write(&m))
		return m.GetHistogram().GetSampleCount()
	}
	h.p.beforeLoad(cold(h))
	require.Zero(t, observed(), "a never-unloaded shard is not observed")
	loading := cold(h)
	loading.unloadedAt.Store(time.Now().UnixNano())
	require.Zero(t, testing.AllocsPerRun(200, func() { h.p.beforeLoad(loading) }))
	require.Positive(t, observed())
}

func TestSeriesStayBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, idleKnobs)
		for range 50 {
			h.loadN(1)[0].inUse.Store(true)
			h.loadN(1)
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		tickOnce(time.Second)
		require.Equal(t, 50.0, testutil.ToFloat64(h.p.metrics.loaded))
		require.Positive(t, testutil.CollectAndCount(h.p.metrics.unloads))

		families, err := h.prom.Gather()
		require.NoError(t, err)
		series := 0
		for _, f := range families {
			for _, m := range f.GetMetric() {
				series++
				for _, l := range m.GetLabel() {
					require.NotEqual(t, "namespace", l.GetName(), "%s carries a namespace label", f.GetName())
				}
			}
		}
		require.LessOrEqual(t, series, 6, "series must not grow with the shard count")
		again := newMetrics(h.prom, h.p.logger)
		require.Same(t, h.p.metrics.loaded, again.loaded, "a second registration reuses the registered metrics")
		require.Same(t, h.p.metrics.unloading, again.unloading)
	})
}

func TestQueuedUnloadsFollowTheCurrentSettings(t *testing.T) {
	tests := []struct {
		name   string
		change func(t *testing.T, k *testKnobs)
		want   []db.UnloadRequest
	}{
		{
			name:   "idle_ttl raised",
			change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, time.Hour) },
			want:   []db.UnloadRequest{{IdleTTL: time.Hour}},
		},
		{name: "idle_ttl turned off", change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 0) }},
		{name: "paused", change: func(t *testing.T, k *testKnobs) { set(t, k.paused, true) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.checkInterval, 24*time.Hour)
					set(t, k.idleTTL, 30*time.Second)
					set(t, k.paused, true)
				})
				shards := h.loadN(4)
				time.Sleep(time.Minute)
				synctest.Wait()
				gate := make(chan struct{})
				shards[0].block.Store(&gate)

				set(t, h.knobs.paused, false)
				h.p.refreshConfig()
				synctest.Wait()
				require.Len(t, shards[0].requests(), 1, "the only worker is busy on the first shard")
				queued := shards[1]
				require.True(t, queued.pending.Load(), "the second shard waits in the queue")

				tt.change(t, h.knobs)
				h.p.refreshConfig()
				synctest.Wait()
				close(gate)
				synctest.Wait()

				require.True(t, queued.loaded())
				require.Equal(t, tt.want, queued.requests(), "a handoff runs with the current settings or not at all")
				require.False(t, queued.pending.Load(), "the handoff released its claim")
				require.Zero(t, h.p.pending.Load())
			})
		})
	}
}

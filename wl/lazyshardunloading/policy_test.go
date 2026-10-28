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

package lazyshardunloading

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func cold(h *harness, namespace string) *fakeShard {
	return &fakeShard{name: "loading", namespace: namespace, reg: h.reg}
}

func admissions(h *harness) (under, madeRoom, overcommit float64) {
	return testutil.ToFloat64(h.p.metrics.admitUnder), testutil.ToFloat64(h.p.metrics.admitMadeRoom),
		testutil.ToFloat64(h.p.metrics.admitOvercommit)
}

func tickOnce(interval time.Duration) {
	time.Sleep(interval)
	synctest.Wait()
}

func TestMakeRoomUnloadsTheOldestEvenUnderMinResidency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 3)
			set(t, k.interval, time.Hour)
		})
		shards := h.loadN(3, "")

		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()

		require.False(t, shards[0].loaded())
		require.True(t, shards[1].loaded())
		require.True(t, shards[2].loaded())
		require.Equal(t, []db.UnloadRequest{{Mode: db.UnloadModeRequest, PickedAt: shards[0].LastAccess()}}, shards[0].requests())
		under, madeRoom, overcommit := admissions(h)
		require.Equal(t, [3]float64{0, 1, 0}, [3]float64{under, madeRoom, overcommit})
		require.Equal(t, 1.0, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("make_room", "ok")))
		require.Zero(t, h.p.pending.Load())
	})
}

func TestMakeRoomUnderTheLimitAdmitsWithoutUnloading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.maxLoaded, 3) })
		shards := h.loadN(2, "")
		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		require.Equal(t, 2, h.loadedCount(shards))
		under, madeRoom, overcommit := admissions(h)
		require.Equal(t, [3]float64{1, 0, 0}, [3]float64{under, madeRoom, overcommit})
	})
}

func TestMakeRoomOvercommits(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testKnobs)
		arm       func(h *harness, shards []*fakeShard) *fakeShard
	}{
		{
			name: "every candidate is in use",
			arm: func(h *harness, shards []*fakeShard) *fakeShard {
				for _, s := range shards {
					s.inUse.Store(true)
				}
				return cold(h, "")
			},
		},
		{
			name:      "no candidates are sampled",
			configure: func(k *testKnobs) { set(nil, k.candidates, 0) },
			arm:       func(h *harness, _ []*fakeShard) *fakeShard { return cold(h, "") },
		},
		{
			name: "every candidate is already claimed",
			arm: func(h *harness, shards []*fakeShard) *fakeShard {
				for _, s := range shards {
					s.pending.Store(true)
				}
				return cold(h, "")
			},
		},
		{
			name: "the only candidate is the loading shard",
			arm: func(h *harness, shards []*fakeShard) *fakeShard {
				for _, s := range shards[1:] {
					s.inUse.Store(true)
				}
				return shards[0]
			},
		},
		{
			name: "every candidate is in an inactive namespace",
			arm: func(h *harness, shards []*fakeShard) *fakeShard {
				h.active.Store("ns", false)
				return cold(h, "")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.maxLoaded, 3)
					set(t, k.interval, time.Hour)
					if tt.configure != nil {
						tt.configure(k)
					}
				})
				shards := h.loadN(3, "ns")
				pending := make([]bool, len(shards))
				for i, s := range shards {
					pending[i] = s.pending.Load()
				}
				loading := tt.arm(h, shards)
				for i, s := range shards {
					pending[i] = s.pending.Load()
				}

				h.p.beforeLoad(loading)
				synctest.Wait()

				require.Equal(t, 3, h.loadedCount(shards))
				_, madeRoom, overcommit := admissions(h)
				require.Zero(t, madeRoom)
				require.Equal(t, 1.0, overcommit)
				for i, s := range shards {
					require.Equal(t, pending[i], s.pending.Load(), "a failed handoff releases its claim")
				}
				require.Zero(t, h.p.pending.Load())
			})
		})
	}
}

func TestMakeRoomOvercommitsWhenTheQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 1)
			set(t, k.interval, time.Hour)
		}, withQueueCap(1))
		shards := h.loadN(4, "")
		gate := make(chan struct{})
		shards[0].block.Store(&gate)

		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		h.p.beforeLoad(cold(h, ""))
		h.p.beforeLoad(cold(h, ""))

		_, madeRoom, overcommit := admissions(h)
		require.Equal(t, 2.0, madeRoom)
		require.Equal(t, 1.0, overcommit)
		require.False(t, shards[2].pending.Load(), "the victim that did not fit is released")
		close(gate)
		synctest.Wait()
		require.False(t, shards[0].loaded())
		require.False(t, shards[1].loaded())
		require.True(t, shards[2].loaded())
		require.Zero(t, h.p.pending.Load())
	})
}

func TestPausedLoadsBehaveAsPlainLazyLoading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 1)
			set(t, k.minResidency, 0)
			set(t, k.idleTTL, time.Second)
			set(t, k.paused, true)
		})
		shards := h.loadN(3, "")
		h.p.beforeLoad(cold(h, ""))
		time.Sleep(time.Minute)
		synctest.Wait()

		require.Equal(t, 3, h.loadedCount(shards))
		under, madeRoom, overcommit := admissions(h)
		require.Equal(t, [3]float64{0, 0, 0}, [3]float64{under, madeRoom, overcommit})
		require.True(t, h.p.reserveWarmupLoad())
		h.p.releaseWarmupLoad()
		for _, s := range shards {
			require.Empty(t, s.requests())
		}
	})
}

func TestRequestModeIgnoresSoftClausesTheLoopHonoursThem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 2)
			set(t, k.minResidency, 30*time.Second)
		})
		old := h.loadN(2, "")
		for _, s := range old {
			s.queueBusy.Store(true)
		}
		time.Sleep(time.Minute)
		fresh := h.loadN(1, "")[0]
		fresh.queueBusy.Store(true)
		synctest.Wait()

		tickOnce(time.Second)
		require.True(t, fresh.loaded())
		require.Empty(t, fresh.requests(), "the loop never hands over a shard under min_residency")
		require.True(t, old[0].loaded(), "a queued vector index keeps a background unload off")
		require.Equal(t, db.UnloadRequest{Mode: db.UnloadModeBackground, MinResidency: 30 * time.Second, PickedAt: old[0].LastAccess()}, old[0].requests()[0])
		require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("watermark", "queue_not_empty")))

		for _, s := range old {
			s.inUse.Store(true)
		}
		synctest.Wait()
		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		require.False(t, fresh.loaded(), "make-room ignores residency and queues")
		require.Equal(t, []db.UnloadRequest{{Mode: db.UnloadModeRequest, PickedAt: fresh.LastAccess()}}, fresh.requests())
	})
}

func TestWatermarkHysteresisAndUnloadRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.high, 0.8)
			set(t, k.low, 0.6)
			set(t, k.rate, 2)
		})
		shards := h.loadN(10, "")
		time.Sleep(time.Minute)
		synctest.Wait()
		steps := []struct {
			ratio float64
			want  int
		}{
			{ratio: 0.85, want: 8},
			{ratio: 0.7, want: 6},
			{ratio: 0.5, want: 6},
			{ratio: 0.7, want: 6},
			{ratio: 0.9, want: 4},
			{ratio: 0.8, want: 2},
			{ratio: 0.59, want: 2},
		}
		for i, step := range steps {
			h.setRatio(step.ratio)
			tickOnce(time.Second)
			require.Equal(t, step.want, h.loadedCount(shards), "step %d ratio %v", i, step.ratio)
		}
		require.Equal(t, []int{8, 9}, loadedIndexes(shards), "the least recently used go first")
	})
}

func loadedIndexes(shards []*fakeShard) []int {
	var out []int
	for i, s := range shards {
		if s.loaded() {
			out = append(out, i)
		}
	}
	return out
}

func TestShardLimitDrainsAtUnloadRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.rate, 2) })
		shards := h.loadN(10, "")
		time.Sleep(time.Minute)
		synctest.Wait()
		set(t, h.knobs.maxLoaded, 5)
		for _, want := range []int{8, 6, 5, 5} {
			tickOnce(time.Second)
			require.Equal(t, want, h.loadedCount(shards))
		}
		require.Equal(t, []int{5, 6, 7, 8, 9}, loadedIndexes(shards))
		require.Equal(t, 5.0, testutil.ToFloat64(h.p.metrics.limit))
		require.Zero(t, testutil.ToFloat64(h.p.metrics.overLimit))
	})
}

func TestIdleTTLUnloadsBelowEveryLimitAndStopsAtZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.idleTTL, 10*time.Second)
			set(t, k.minResidency, 5*time.Second)
		})
		busy := h.reg.load("busy", "")
		idle := h.reg.load("idle", "")
		for range 12 {
			busy.touch()
			tickOnce(time.Second)
		}
		require.True(t, busy.loaded())
		require.False(t, idle.loaded())
		require.Equal(t, db.UnloadRequest{Mode: db.UnloadModeBackground, MinResidency: 5 * time.Second, IdleTTL: 10 * time.Second, PickedAt: idle.LastAccess()},
			idle.requests()[len(idle.requests())-1])
		require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("idle", "ok")))

		set(t, h.knobs.idleTTL, 0)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.True(t, busy.loaded(), "idle_ttl 0 stops idle unloading")
	})
}

func TestIdleSweepRunsEveryIdleCheckIntervalAndResumesWhenCutShort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.minResidency, 0)
			set(t, k.idleCheckInterval, time.Hour)
			set(t, k.rate, 2)
		})
		shards := h.loadN(5, "")
		time.Sleep(20 * time.Second)
		synctest.Wait()
		set(t, h.knobs.idleTTL, 10*time.Second)
		for _, want := range []int{3, 1, 0} {
			tickOnce(time.Second)
			require.Equal(t, want, h.loadedCount(shards), "a sweep cut short by unload_rate resumes on the next tick")
		}

		late := h.loadN(1, "")[0]
		time.Sleep(30 * time.Minute)
		synctest.Wait()
		require.True(t, late.loaded(), "the next full sweep waits for idle_check_interval")
		time.Sleep(time.Hour)
		synctest.Wait()
		require.False(t, late.loaded())
	})
}

func TestKnobChangesTakeEffectWithinOneTick(t *testing.T) {
	tests := []struct {
		name   string
		before func(*testKnobs)
		ratio  float64
		change func(t *testing.T, k *testKnobs)
		want   int
	}{
		{name: "lowering the limit", change: func(t *testing.T, k *testKnobs) { set(t, k.maxLoaded, 3) }, want: 3},
		{name: "setting idle_ttl", change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 30*time.Second) }, want: 0},
		{
			name:  "setting watermarks",
			ratio: 0.9,
			change: func(t *testing.T, k *testKnobs) {
				set(t, k.high, 0.8)
				set(t, k.low, 0.5)
			},
			want: 0,
		},
		{
			name:   "pausing",
			before: func(k *testKnobs) { set(nil, k.paused, true) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.maxLoaded, 1) },
			want:   6,
		},
		{
			name:   "unpausing",
			before: func(k *testKnobs) { set(nil, k.paused, true); set(nil, k.maxLoaded, 1) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.paused, false) },
			want:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.interval, 10*time.Second)
					if tt.before != nil {
						tt.before(k)
					}
				})
				h.setRatio(tt.ratio)
				shards := h.loadN(6, "")
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, 6, h.loadedCount(shards))

				tt.change(t, h.knobs)
				tickOnce(10 * time.Second)
				require.Equal(t, tt.want, h.loadedCount(shards))
			})
		})
	}
}

func TestRaisingTheLimitStopsDraining(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.rate, 1) })
		shards := h.loadN(6, "")
		time.Sleep(time.Minute)
		synctest.Wait()
		set(t, h.knobs.maxLoaded, 2)
		tickOnce(time.Second)
		require.Equal(t, 5, h.loadedCount(shards))
		set(t, h.knobs.maxLoaded, 10)
		tickOnce(time.Second)
		tickOnce(time.Second)
		require.Equal(t, 5, h.loadedCount(shards))
	})
}

func TestRefreshConfigAppliesKnobsWithoutWaitingForTheTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.interval, time.Hour)
			set(t, k.minResidency, 0)
		})
		shards := h.loadN(4, "")
		synctest.Wait()
		set(t, h.knobs.maxLoaded, 1)
		h.p.refreshConfig()
		synctest.Wait()
		require.Equal(t, 1, h.loadedCount(shards))
		require.EqualValues(t, 1, h.p.snap.Load().maxLoaded)
	})
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

func TestUnloadPanicKeepsTheWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 1)
			set(t, k.interval, time.Hour)
		})
		shards := h.loadN(2, "")
		shards[0].panics.Store(true)
		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		require.True(t, shards[0].loaded())
		require.False(t, shards[0].pending.Load())
		require.EqualValues(t, 1, h.p.workersRunning.Load())
		shards[0].inUse.Store(true)

		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		require.False(t, shards[1].loaded(), "the same worker keeps unloading")
		require.Zero(t, h.p.pending.Load())
	})
}

func TestCloseStopsEveryGoroutineAndReleasesQueuedShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 1)
			set(t, k.interval, time.Hour)
			set(t, k.workers, 1)
		})
		shards := h.loadN(3, "")
		gate := make(chan struct{})
		shards[0].block.Store(&gate)
		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		h.p.beforeLoad(cold(h, ""))
		require.True(t, shards[1].pending.Load())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, h.p.close(ctx), context.DeadlineExceeded, "close waits for an unload in flight")
		close(gate)
		require.NoError(t, h.p.close(context.Background()))
		require.NoError(t, h.p.close(context.Background()))
		require.Zero(t, h.p.workersRunning.Load())
		require.False(t, shards[1].pending.Load(), "a queued handoff is released")
		require.True(t, shards[1].loaded())
		require.Zero(t, h.p.pending.Load())

		_, madeRoom, overcommit := admissions(h)
		h.p.beforeLoad(cold(h, ""))
		h.p.refreshConfig()
		_, madeRoomAfter, overcommitAfter := admissions(h)
		require.Equal(t, [2]float64{madeRoom, overcommit}, [2]float64{madeRoomAfter, overcommitAfter})
	})
}

func TestNamespaceShareAndActiveNamespaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 10)
			set(t, k.share, 0.5)
		}, withNamespaces())
		small := h.loadN(3, "small")
		big := h.loadN(7, "big")
		time.Sleep(time.Minute)
		synctest.Wait()

		h.p.beforeLoad(cold(h, "small"))
		synctest.Wait()
		require.Equal(t, 3, h.loadedCount(small), "an over-share namespace gives up shards first, though younger")
		require.False(t, big[0].loaded())

		set(t, h.knobs.maxLoaded, 7)
		tickOnce(time.Second)
		require.Equal(t, 3, h.loadedCount(small))
		require.Equal(t, 4, h.loadedCount(big))

		h.active.Store("big", false)
		set(t, h.knobs.maxLoaded, 3)
		tickOnce(time.Second)
		require.Equal(t, 4, h.loadedCount(big), "an inactive namespace never gives up shards")
		require.Equal(t, 0, h.loadedCount(small))
		require.Equal(t, 4.0, testutil.ToFloat64(h.p.metrics.loaded.WithLabelValues("big")))
		require.Equal(t, 1.0, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("make_room", "ok", "big")))
		tickOnce(time.Second)
		require.Equal(t, 1, testutil.CollectAndCount(h.p.metrics.loaded), "an emptied namespace drops its gauge")
	})
}

func TestReserveWarmupLoad(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testKnobs)
		loaded    int
		ratio     float64
		want      bool
	}{
		{name: "no limits", loaded: 100, want: true},
		{name: "under the shard limit", configure: func(k *testKnobs) { set(nil, k.maxLoaded, 3) }, loaded: 2, want: true},
		{name: "at the shard limit", configure: func(k *testKnobs) { set(nil, k.maxLoaded, 3) }, loaded: 3},
		{name: "under the high watermark", configure: func(k *testKnobs) { set(nil, k.high, 0.8); set(nil, k.low, 0.5) }, ratio: 0.7, want: true},
		{name: "at the high watermark", configure: func(k *testKnobs) { set(nil, k.high, 0.8); set(nil, k.low, 0.5) }, ratio: 0.8},
		{name: "paused at the limit", configure: func(k *testKnobs) { set(nil, k.maxLoaded, 1); set(nil, k.paused, true) }, loaded: 3, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.interval, time.Hour)
					if tt.configure != nil {
						tt.configure(k)
					}
				})
				h.setRatio(tt.ratio)
				for range tt.loaded {
					h.reg.load("s", "")
				}
				require.Equal(t, tt.want, h.p.reserveWarmupLoad())
				if tt.want {
					h.p.releaseWarmupLoad()
				}
				require.Zero(t, h.p.warmupReserved.Load())
			})
		})
	}
}

func TestReloadAfterUnloadIsObserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, nil)
		s := cold(h, "")
		h.p.beforeLoad(s)
		s.unloadedAt.Store(time.Now().UnixNano())
		time.Sleep(5 * time.Second)
		h.p.beforeLoad(s)
		require.Equal(t, 1, histogramCount(t, h, "weaviate_lazy_shard_unloading_reload_after_unload_seconds"))
	})
}

func histogramCount(t *testing.T, h *harness, name string) int {
	t.Helper()
	families, err := h.prom.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return int(f.GetMetric()[0].GetHistogram().GetSampleCount())
		}
	}
	return 0
}

func TestGaugesWithoutNamespaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 2)
			set(t, k.minResidency, time.Hour)
		})
		h.loadN(3, "")
		tickOnce(time.Second)
		require.Equal(t, 3.0, testutil.ToFloat64(h.p.metrics.loaded.WithLabelValues()))
		require.Equal(t, 2.0, testutil.ToFloat64(h.p.metrics.limit))
		require.Equal(t, 1.0, testutil.ToFloat64(h.p.metrics.overLimit))
		require.Zero(t, testutil.ToFloat64(h.p.metrics.unloading))
	})
}

func TestMetricsRegisterTwiceOnOneRegistry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, nil)
		again := newMetrics(h.prom, false, h.p.logger)
		require.Same(t, h.p.metrics.limit, again.limit)
		mismatched := newMetrics(h.prom, true, h.p.logger)
		require.NotNil(t, mismatched.unloads)
	})
}

func newAllocHarness(tb testing.TB, inUse bool, maxLoaded int) (*harness, *fakeShard) {
	h := newHarness(tb, func(k *testKnobs) {
		set(tb, k.maxLoaded, maxLoaded)
		set(tb, k.share, 0.5)
		set(tb, k.interval, time.Hour)
	}, withNamespaces())
	for i := range 64 {
		s := h.reg.load("s", "ns")
		s.lastAccess.Store(uint32(i))
		s.inUse.Store(inUse)
	}
	h.p.adjustWorkers(0)
	loading := cold(h, "ns")
	loading.unloadedAt.Store(time.Now().UnixNano())
	return h, loading
}

func (h *harness) beforeLoadAndDrain(loading *fakeShard) {
	h.p.beforeLoad(loading)
	select {
	case it := <-h.p.makeRoomCh:
		h.p.release(it)
	default:
	}
}

func TestBeforeLoadAllocatesNothing(t *testing.T) {
	tests := []struct {
		name      string
		inUse     bool
		maxLoaded int
		admission func(h *harness) float64
	}{
		{name: "under the limit", maxLoaded: 1000, admission: func(h *harness) float64 { u, _, _ := admissions(h); return u }},
		{name: "overcommit", inUse: true, maxLoaded: 1, admission: func(h *harness) float64 { _, _, o := admissions(h); return o }},
		{name: "made room", maxLoaded: 1, admission: func(h *harness) float64 { _, m, _ := admissions(h); return m }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, loading := newAllocHarness(t, tt.inUse, tt.maxLoaded)
			require.Zero(t, testing.AllocsPerRun(200, func() { h.beforeLoadAndDrain(loading) }))
			require.Positive(t, tt.admission(h))
		})
	}
}

func BenchmarkBeforeLoad(b *testing.B) {
	cases := []struct {
		name      string
		inUse     bool
		maxLoaded int
	}{
		{name: "under_limit", maxLoaded: 1000},
		{name: "overcommit", inUse: true, maxLoaded: 1},
		{name: "made_room", maxLoaded: 1},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			h, loading := newAllocHarness(b, c.inUse, c.maxLoaded)
			b.ReportAllocs()
			for b.Loop() {
				h.beforeLoadAndDrain(loading)
			}
		})
	}
}

func TestReindexSnapshotIsTakenPerTickAndRidesOnEveryUnload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 2)
			set(t, k.minResidency, 0)
		})
		var calls atomic.Int64
		h.p.setReindexSource(func(ctx context.Context) (db.ReindexActivity, error) {
			calls.Add(1)
			return db.NewReindexActivity(nil), nil
		})
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load(), "installing the source takes the first snapshot at once")
		shards := h.loadN(3, "")
		synctest.Wait()

		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		var reqs []db.UnloadRequest
		for _, s := range shards {
			reqs = append(reqs, s.requests()...)
		}
		require.NotEmpty(t, reqs)
		for _, r := range reqs {
			require.True(t, r.Reindex.Usable(), "every unload carries the last tick's snapshot")
		}
		require.GreaterOrEqual(t, calls.Load(), int64(3), "one snapshot per tick")
	})
}

func TestReindexSnapshotIsNotTakenWithoutAnyTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, nil)
		var calls atomic.Int64
		h.p.setReindexSource(func(ctx context.Context) (db.ReindexActivity, error) {
			calls.Add(1)
			return db.NewReindexActivity(nil), nil
		})
		tickOnce(10 * time.Second)
		require.Zero(t, calls.Load())
	})
}

func TestSlowReindexSnapshotIsBoundedAndNeverBlocksALoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.maxLoaded, 1) })
		h.loadN(2, "")
		var calls atomic.Int64
		h.p.setReindexSource(func(ctx context.Context) (db.ReindexActivity, error) {
			calls.Add(1)
			<-ctx.Done()
			return db.ReindexActivity{}, ctx.Err()
		})
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load())

		start := time.Now()
		h.p.beforeLoad(cold(h, ""))
		require.Zero(t, time.Since(start), "make-room reads the last snapshot, never the source")

		time.Sleep(reindexSnapshotTimeout)
		synctest.Wait()
		require.False(t, h.p.reindexSnapshot().Usable())
		require.EqualValues(t, 1, warnings(h, "unloads are refused"), "a failing source logs once")
	})
}

func TestFailingReindexSnapshotLogsOncePerStreak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) { set(t, k.maxLoaded, 10) })
		var fail atomic.Bool
		fail.Store(true)
		h.p.setReindexSource(func(ctx context.Context) (db.ReindexActivity, error) {
			if fail.Load() {
				return db.ReindexActivity{}, errors.New("leader unreachable")
			}
			return db.NewReindexActivity(nil), nil
		})
		for range 5 {
			tickOnce(time.Second)
		}
		require.EqualValues(t, 1, warnings(h, "unloads are refused"))
		require.False(t, h.p.reindexSnapshot().Usable())

		fail.Store(false)
		tickOnce(time.Second)
		require.True(t, h.p.reindexSnapshot().Usable())
		fail.Store(true)
		tickOnce(time.Second)
		require.EqualValues(t, 2, warnings(h, "unloads are refused"))
	})
}

func warnings(h *harness, substr string) int {
	n := 0
	for _, e := range h.logs.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, substr) {
			n++
		}
	}
	return n
}

func TestConcurrentWarmupReservationsNeverPassTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit, loaders = 3, 32
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, limit)
			set(t, k.interval, time.Hour)
		})
		h.reg.load("loaded", "")
		var granted atomic.Int64
		var wg sync.WaitGroup
		for range loaders {
			wg.Go(func() {
				if h.p.reserveWarmupLoad() {
					granted.Add(1)
				}
			})
		}
		wg.Wait()
		require.EqualValues(t, limit-1, granted.Load(), "one shard is loaded, so only limit-1 warmups fit")
		require.False(t, h.p.reserveWarmupLoad())
		for range granted.Load() {
			h.p.releaseWarmupLoad()
		}
		require.True(t, h.p.reserveWarmupLoad(), "released reservations free their room")
		h.p.releaseWarmupLoad()
	})
}

func TestRemovedNamespaceLosesItsSeries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 1)
			set(t, k.minResidency, 0)
		}, withNamespaces())
		h.loadN(1, "gone")
		h.loadN(1, "kept")
		tickOnce(time.Second)
		require.Positive(t, testutil.CollectAndCount(h.p.metrics.unloads))

		monitoring.GetMetrics().DeleteNamespace("gone")

		for _, ns := range []string{"gone", "kept"} {
			found := false
			for _, n := range seriesNamespaces(t, h) {
				found = found || n == ns
			}
			require.Equal(t, ns == "kept", found, "namespace %q", ns)
		}
	})
}

func seriesNamespaces(t *testing.T, h *harness) []string {
	t.Helper()
	families, err := h.prom.Gather()
	require.NoError(t, err)
	var out []string
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "namespace" {
					out = append(out, l.GetValue())
				}
			}
		}
	}
	return out
}

func TestMakeRoomSkipsAVictimAccessedSincePicked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.maxLoaded, 2)
			set(t, k.interval, time.Hour)
		})
		shards := h.loadN(2, "")
		block := make(chan struct{})
		shards[0].block.Store(&block)

		h.p.beforeLoad(cold(h, ""))
		synctest.Wait()
		reqs := shards[0].requests()
		require.Len(t, reqs, 1)
		require.Equal(t, shards[0].LastAccess(), reqs[0].PickedAt, "make-room tells TryUnload what the victim was picked at")
		close(block)
		synctest.Wait()
	})
}

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
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func cold(h *harness, namespace string) *fakeShard {
	return &fakeShard{name: "loading", namespace: namespace, reg: h.reg}
}

func tickOnce(interval time.Duration) {
	time.Sleep(interval)
	synctest.Wait()
}

func idleKnobs(k *testKnobs) {
	set(nil, k.idleTTL, 2*time.Second)
}

func TestPausedNeverUnloads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			idleKnobs(k)
			set(t, k.paused, true)
		})
		shards := h.loadN(3, "")
		h.p.beforeLoad(cold(h, ""))
		time.Sleep(time.Minute)
		synctest.Wait()

		require.Equal(t, 3, h.loadedCount(shards))
		for _, s := range shards {
			require.Empty(t, s.requests())
		}
	})
}

func TestIdleSweepRetriesARefusedShard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.idleTTL, 40*time.Second)
		})
		busy := h.loadN(1, "")[0]
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
			shards[i] = h.reg.load(strconv.Itoa(i), "")
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

func TestIdleTTLUnloadsIdleShardsAndStopsAtZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.idleTTL, 10*time.Second)
		})
		busy := h.reg.load("busy", "")
		idle := h.reg.load("idle", "")
		for range 12 {
			busy.touch()
			tickOnce(time.Second)
		}
		require.True(t, busy.loaded())
		require.False(t, idle.loaded())
		require.Equal(t, db.UnloadRequest{IdleTTL: 10 * time.Second}, idle.requests()[len(idle.requests())-1])
		require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("ok")))

		set(t, h.knobs.idleTTL, 0)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.True(t, busy.loaded(), "idle_ttl 0 stops idle unloading")
	})
}

func TestFractionalIdleTTLWaitsUntilTheShardIsIdle(t *testing.T) {
	tests := []struct {
		name    string
		idleTTL time.Duration
		wait    time.Duration
	}{
		{name: "below two seconds", idleTTL: 1500 * time.Millisecond, wait: 3 * time.Second},
		{name: "just past a whole second", idleTTL: 2100 * time.Millisecond, wait: 4 * time.Second},
		{name: "just past half a minute", idleTTL: 30500 * time.Millisecond, wait: 32 * time.Second},
		{name: "a whole second", idleTTL: 2 * time.Second, wait: 3 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.idleTTL, tt.idleTTL)
				})
				shard := h.reg.load("idle", "")

				time.Sleep(tt.wait)
				synctest.Wait()

				require.False(t, shard.loaded())
				require.Len(t, shard.requests(), 1, "selection and TryUnload agree on when the shard is idle")
			})
		})
	}
}

func TestIdleShardIsNeverUnloadedBeforeIdleTTL(t *testing.T) {
	for _, idleTTL := range []time.Duration{time.Second, 1500 * time.Millisecond, 2 * time.Second, 10 * time.Second} {
		t.Run(idleTTL.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.idleTTL, idleTTL)
					set(t, k.checkInterval, 50*time.Millisecond)
				})
				shard := h.reg.load("idle", "")
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
		shards := h.loadN(5, "")
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

		late := h.loadN(1, "")[0]
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
	}{
		{name: "setting idle_ttl", change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 30*time.Second) }, want: 0},
		{
			name:   "pausing",
			before: func(k *testKnobs) { set(nil, k.paused, true) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.idleTTL, 30*time.Second) },
			want:   6,
		},
		{
			name:   "unpausing",
			before: func(k *testKnobs) { set(nil, k.paused, true); set(nil, k.idleTTL, 30*time.Second) },
			change: func(t *testing.T, k *testKnobs) { set(t, k.paused, false) },
			want:   0,
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

func TestRefreshConfigAppliesKnobsWithoutWaitingForTheTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.checkInterval, time.Hour)
		})
		shards := h.loadN(4, "")
		time.Sleep(10 * time.Second)
		synctest.Wait()
		set(t, h.knobs.idleTTL, 2*time.Second)
		h.p.refreshConfig()
		synctest.Wait()
		require.Zero(t, h.loadedCount(shards))
		require.Equal(t, 2*time.Second, h.p.snap.Load().idleTTL)
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
		h := newHarness(t, idleKnobs)
		shards := h.loadN(2, "")
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

func blockedSweep(t *testing.T) (*harness, []*fakeShard, chan struct{}) {
	h := newHarness(t, func(k *testKnobs) {
		idleKnobs(k)
		set(t, k.workers, 1)
	})
	gate := make(chan struct{})
	first := h.reg.load("blocked", "")
	first.block.Store(&gate)
	time.Sleep(time.Second)
	shards := append([]*fakeShard{first}, h.loadN(2, "")...)
	time.Sleep(5 * time.Second)
	synctest.Wait()
	require.Len(t, shards[0].requests(), 1)
	require.True(t, shards[1].pending.Load())
	return h, shards, gate
}

func TestCloseStopsEveryGoroutineAndReleasesQueuedShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, shards, gate := blockedSweep(t)

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

		h.p.beforeLoad(cold(h, ""))
		h.p.refreshConfig()
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Empty(t, shards[1].requests())
		require.Empty(t, shards[2].requests())
	})
}

func TestIdleSweepSkipsInactiveNamespaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, idleKnobs, withNamespaces())
		h.active.Store("parked", false)
		live := h.loadN(3, "live")
		parked := h.loadN(3, "parked")
		time.Sleep(10 * time.Second)
		synctest.Wait()

		require.Zero(t, h.loadedCount(live))
		require.Equal(t, 3, h.loadedCount(parked), "an inactive namespace never gives up shards")
		require.Equal(t, 3.0, testutil.ToFloat64(h.p.metrics.loaded.WithLabelValues("parked")))
		tickOnce(time.Second)
		require.Equal(t, 1, testutil.CollectAndCount(h.p.metrics.loaded), "an emptied namespace drops its gauge")
	})
}

func TestReloadAfterUnloadIsObserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, nil)
		s := cold(h, "")
		h.p.beforeLoad(s)
		s.unloadedAt.Store(time.Now().UnixNano())
		time.Sleep(5 * time.Second)
		h.p.beforeLoad(s)
		require.Equal(t, 1, histogramCount(t, h, "weaviate_idle_shard_collector_reload_after_unload_seconds"))
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
		h := newHarness(t, nil)
		h.loadN(3, "")
		tickOnce(time.Second)
		require.Equal(t, 3.0, testutil.ToFloat64(h.p.metrics.loaded.WithLabelValues()))
		require.Zero(t, testutil.ToFloat64(h.p.metrics.unloading))
	})
}

func TestMetricsRegisterTwiceOnOneRegistry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, nil)
		again := newMetrics(h.prom, false, h.p.logger)
		require.Same(t, h.p.metrics.unloading, again.unloading)
		mismatched := newMetrics(h.prom, true, h.p.logger)
		require.NotNil(t, mismatched.unloads)
	})
}

func newAllocHarness(tb testing.TB) (*harness, *fakeShard) {
	h := newHarness(tb, func(k *testKnobs) { set(tb, k.checkInterval, time.Hour) })
	loading := cold(h, "")
	loading.unloadedAt.Store(time.Now().UnixNano())
	return h, loading
}

func TestBeforeLoadAllocatesNothing(t *testing.T) {
	h, loading := newAllocHarness(t)
	require.Zero(t, testing.AllocsPerRun(200, func() { h.p.beforeLoad(loading) }))
	require.Positive(t, histogramCount(t, h, "weaviate_idle_shard_collector_reload_after_unload_seconds"))
}

func BenchmarkBeforeLoad(b *testing.B) {
	h, loading := newAllocHarness(b)
	b.ReportAllocs()
	for b.Loop() {
		h.p.beforeLoad(loading)
	}
}

func TestRemovedNamespaceLosesItsSeries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, idleKnobs, withNamespaces())
		for _, ns := range []string{"gone", "kept"} {
			h.loadN(1, ns)[0].inUse.Store(true)
		}
		h.loadN(1, "idle")
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Positive(t, testutil.CollectAndCount(h.p.metrics.unloads))
		require.Empty(t, familyLabelValues(t, h, "weaviate_idle_shard_collector_unloads_total", "namespace"))

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

func familyLabelValues(t *testing.T, h *harness, family, label string) []string {
	t.Helper()
	families, err := h.prom.Gather()
	require.NoError(t, err)
	var out []string
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label {
					out = append(out, l.GetValue())
				}
			}
		}
	}
	return out
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

func TestCloseTimingOutStillReleasesQueuedShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, shards, gate := blockedSweep(t)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, h.p.close(ctx), context.DeadlineExceeded)
		require.False(t, shards[1].pending.Load(), "a timed-out close must not strand a queued claim")
		require.True(t, shards[1].loaded())
		close(gate)
	})
}

// synctest fails a test whose bubble still holds goroutines once it returns,
// so this pins that Close leaves neither the loop nor a worker behind.
func TestCloseLeavesNoGoroutineBehind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		k := newTestKnobs()
		set(t, k.workers, 8)
		idleKnobs(k)
		reg := newFakeRegistry()
		p := newPolicy[*fakeShard](reg, k.knobs(), policyDeps{
			logger: logger, clock: reg.clock, namespaces: true,
			metrics: newMetrics(nil, true, logger),
		})
		for range 4 {
			reg.load("s", "ns")
		}
		tickOnce(5 * time.Second)
		p.beforeLoad(&fakeShard{name: "loading", reg: reg})
		synctest.Wait()
		set(t, k.workers, 2)
		p.refreshConfig()
		synctest.Wait()

		require.NoError(t, p.close(context.Background()))
		require.Zero(t, p.workersRunning.Load())
	})
}

func TestCollectingFollowsTheKnobs(t *testing.T) {
	tests := []struct {
		name      string
		configure func(k *testKnobs)
		close     bool
		want      bool
	}{
		{name: "idle_ttl off"},
		{name: "idle_ttl set", configure: idleKnobs, want: true},
		{name: "paused", configure: func(k *testKnobs) { idleKnobs(k); set(nil, k.paused, true) }},
		{name: "closed", configure: idleKnobs, close: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, tt.configure)
				if tt.close {
					require.NoError(t, h.p.close(context.Background()))
				}
				require.Equal(t, tt.want, h.p.collecting())
			})
		})
	}
}

func TestCollectingStopsOnALiveChange(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	k := newTestKnobs()
	set(t, k.checkInterval, time.Hour)
	c := New(Config{Logger: logger, Knobs: k.knobs()})
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.False(t, c.Collecting(), "idle_ttl 0 collects nothing")

	set(t, k.idleTTL, time.Second)
	c.RefreshConfig()
	require.True(t, c.Collecting())
	set(t, k.paused, true)
	c.RefreshConfig()
	require.False(t, c.Collecting(), "a runtime change applies without waiting for the check")
}

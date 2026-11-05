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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

const queuedNamespace = "ns"

func TestQueuedUnloadsFollowTheCurrentSettings(t *testing.T) {
	unpause := func(h *harness) {
		set(h.t, h.knobs.paused, false)
		h.p.refreshConfig()
	}
	makeRoomTwice := func(h *harness) {
		h.p.beforeLoad(cold(h, queuedNamespace))
		h.p.beforeLoad(cold(h, queuedNamespace))
	}
	idleSweep := func(k *testKnobs) {
		set(nil, k.idleTTL, 30*time.Second)
		set(nil, k.paused, true)
	}
	limitDrain := func(k *testKnobs) {
		set(nil, k.maxLoaded, 2)
		set(nil, k.paused, true)
	}
	memoryDrain := func(k *testKnobs) {
		set(nil, k.high, 0.8)
		set(nil, k.low, 0.5)
		set(nil, k.paused, true)
	}
	makeRoomAtLimit := func(k *testKnobs) { set(nil, k.maxLoaded, 3) }
	makeRoomAtMemory := func(k *testKnobs) {
		set(nil, k.high, 0.8)
		set(nil, k.low, 0.5)
	}
	stays := func(t *testing.T, queued *fakeShard) {
		require.True(t, queued.loaded())
		require.Empty(t, queued.requests(), "a handoff no setting wants anymore never reaches TryUnload")
	}
	refusedBy := func(check func(*testing.T, db.UnloadRequest)) func(*testing.T, *fakeShard) {
		return func(t *testing.T, queued *fakeShard) {
			require.True(t, queued.loaded())
			reqs := queued.requests()
			require.Len(t, reqs, 1)
			check(t, reqs[0])
		}
	}
	unloadedWith := func(check func(*testing.T, db.UnloadRequest)) func(*testing.T, *fakeShard) {
		return func(t *testing.T, queued *fakeShard) {
			require.False(t, queued.loaded())
			reqs := queued.requests()
			require.Len(t, reqs, 1)
			check(t, reqs[0])
		}
	}
	idleTTLIs := func(want time.Duration) func(*testing.T, db.UnloadRequest) {
		return func(t *testing.T, r db.UnloadRequest) { require.Equal(t, want, r.IdleTTL) }
	}
	minResidencyIs := func(want time.Duration) func(*testing.T, db.UnloadRequest) {
		return func(t *testing.T, r db.UnloadRequest) { require.Equal(t, want, r.MinResidency) }
	}
	anyRequest := func(*testing.T, db.UnloadRequest) {}

	tests := []struct {
		name      string
		configure func(k *testKnobs)
		ratio     float64
		queue     func(h *harness)
		change    func(h *harness)
		want      func(t *testing.T, queued *fakeShard)
	}{
		{
			name:      "idle sweep, idle_ttl raised",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.idleTTL, time.Hour) },
			want:      refusedBy(idleTTLIs(time.Hour)),
		},
		{
			name:      "idle sweep, idle_ttl lowered",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.idleTTL, 10*time.Second) },
			want:      unloadedWith(idleTTLIs(10 * time.Second)),
		},
		{
			name:      "idle sweep, idle_ttl turned off",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.idleTTL, 0) },
			want:      stays,
		},
		{
			name:      "idle sweep, min_residency raised",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.minResidency, 20*time.Second) },
			want:      unloadedWith(minResidencyIs(20 * time.Second)),
		},
		{
			name:      "idle sweep, min_residency raised past idle_ttl turns idle unloading off",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.minResidency, time.Hour) },
			want:      stays,
		},
		{
			name:      "idle sweep, paused",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.paused, true) },
			want:      stays,
		},
		{
			name:      "idle sweep, namespace no longer active",
			configure: idleSweep,
			queue:     unpause,
			change:    func(h *harness) { h.active.Store(queuedNamespace, false) },
			want:      stays,
		},
		{
			name:      "idle sweep, reindex snapshot replaced",
			configure: idleSweep,
			queue:     unpause,
			change:    storeUsableReindexSnapshot,
			want:      unloadedWith(func(t *testing.T, r db.UnloadRequest) { require.True(t, r.Reindex.Usable()) }),
		},
		{
			name:      "limit drain, min_residency raised",
			configure: limitDrain,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.minResidency, time.Hour) },
			want:      refusedBy(minResidencyIs(time.Hour)),
		},
		{
			name:      "limit drain, max_loaded_shards raised",
			configure: limitDrain,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.maxLoaded, 100) },
			want:      stays,
		},
		{
			name:      "limit drain, max_loaded_shards cleared",
			configure: limitDrain,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.maxLoaded, 0) },
			want:      stays,
		},
		{
			name:      "limit drain, max_loaded_shards lowered",
			configure: limitDrain,
			queue:     unpause,
			change:    func(h *harness) { set(h.t, h.knobs.maxLoaded, 1) },
			want:      unloadedWith(anyRequest),
		},
		{
			name:      "memory drain, watermarks cleared",
			configure: memoryDrain,
			ratio:     0.9,
			queue:     unpause,
			change: func(h *harness) {
				set(h.t, h.knobs.high, 0)
				set(h.t, h.knobs.low, 0)
			},
			want: stays,
		},
		{
			name:      "memory drain, heap fell below the low watermark",
			configure: memoryDrain,
			ratio:     0.9,
			queue:     unpause,
			change:    func(h *harness) { h.setRatio(0.1) },
			want:      stays,
		},
		{
			name:      "memory drain, heap between the watermarks keeps draining",
			configure: memoryDrain,
			ratio:     0.9,
			queue:     unpause,
			change:    func(h *harness) { h.setRatio(0.6) },
			want:      unloadedWith(anyRequest),
		},
		{
			name:      "make-room, max_loaded_shards raised",
			configure: makeRoomAtLimit,
			queue:     makeRoomTwice,
			change:    func(h *harness) { set(h.t, h.knobs.maxLoaded, 100) },
			want:      stays,
		},
		{
			name:      "make-room, min_residency raised still does not apply",
			configure: makeRoomAtLimit,
			queue:     makeRoomTwice,
			change:    func(h *harness) { set(h.t, h.knobs.minResidency, time.Hour) },
			want:      unloadedWith(minResidencyIs(0)),
		},
		{
			name:      "make-room, heap fell below the low watermark",
			configure: makeRoomAtMemory,
			ratio:     0.9,
			queue:     makeRoomTwice,
			change:    func(h *harness) { h.setRatio(0.1) },
			want:      stays,
		},
		{
			name:      "make-room, reindex snapshot replaced",
			configure: makeRoomAtLimit,
			queue:     makeRoomTwice,
			change:    storeUsableReindexSnapshot,
			want:      unloadedWith(func(t *testing.T, r db.UnloadRequest) { require.True(t, r.Reindex.Usable()) }),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.minResidency, 0)
					set(t, k.interval, 24*time.Hour)
					tt.configure(k)
				})
				h.setRatio(tt.ratio)
				h.p.storeRatio(tt.ratio)
				shards := h.loadN(4, queuedNamespace)
				time.Sleep(time.Minute)
				synctest.Wait()
				gate := make(chan struct{})
				shards[0].block.Store(&gate)

				tt.queue(h)
				synctest.Wait()
				require.Len(t, shards[0].requests(), 1, "the only worker is busy on the first shard")
				queued := shards[1]
				require.True(t, queued.pending.Load(), "the second shard waits in the queue")

				tt.change(h)
				h.p.refreshConfig()
				synctest.Wait()
				close(gate)
				synctest.Wait()

				tt.want(t, queued)
				require.False(t, queued.pending.Load(), "the handoff released its claim")
				require.Zero(t, h.p.pending.Load())
			})
		})
	}
}

func storeUsableReindexSnapshot(h *harness) {
	fresh := db.NewReindexActivity(nil)
	h.p.reindex.Store(&fresh)
}

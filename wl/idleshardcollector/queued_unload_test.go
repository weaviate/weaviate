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
	idleSweep := func(k *testKnobs) {
		set(nil, k.idleTTL, 30*time.Second)
		set(nil, k.paused, true)
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

	tests := []struct {
		name      string
		configure func(k *testKnobs)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.checkInterval, 24*time.Hour)
					tt.configure(k)
				})
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

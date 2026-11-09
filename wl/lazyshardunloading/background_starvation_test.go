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
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// maxMakeRoomRun bounds the make-room items a queued background item may wait behind.
const maxMakeRoomRun = 8

func queueUnload(t *testing.T, h *harness, r reason) *fakeShard {
	t.Helper()
	s := h.reg.load(string(r)+"-"+strconv.Itoa(int(h.reg.Loaded())), "")
	require.True(t, s.TryMarkUnloadPending())
	ch, mode := h.p.backgroundCh, db.UnloadModeBackground
	if r == reasonMakeRoom {
		ch, mode = h.p.makeRoomCh, db.UnloadModeRequest
	}
	require.True(t, h.p.enqueue(ch, item[*fakeShard]{shard: s, reason: r, req: db.UnloadRequest{Mode: mode}}))
	return s
}

// processedOrder returns the reasons in the order the worker took the items.
func processedOrder(t *testing.T, queued map[*fakeShard]reason) []reason {
	t.Helper()
	shards := make([]*fakeShard, 0, len(queued))
	for s := range queued {
		require.Positive(t, s.seq.Load(), "every queued item is processed")
		shards = append(shards, s)
	}
	slices.SortFunc(shards, func(a, b *fakeShard) int { return int(a.seq.Load() - b.seq.Load()) })
	order := make([]reason, len(shards))
	for i, s := range shards {
		order[i] = queued[s]
	}
	return order
}

// longestRun is the longest run of r that ends before the last item of the other kind.
func longestRun(order []reason, r reason) int {
	last := -1
	for i, o := range order {
		if o != r {
			last = i
		}
	}
	longest, run := 0, 0
	for _, o := range order[:last+1] {
		if o == r {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
	}
	return longest
}

func TestWorkerBoundsMakeRoomPreference(t *testing.T) {
	tests := []struct {
		name       string
		makeRoom   int
		background int
	}{
		{name: "make-room backlog never empties", makeRoom: 300, background: 10},
		{name: "make-room idle", background: 10},
		{name: "background backlog, a few make-room", makeRoom: 10, background: 300},
		{name: "make-room only", makeRoom: 10},
		{name: "both backlogs", makeRoom: 300, background: 300},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.maxLoaded, 1)
					set(t, k.minResidency, 0)
					set(t, k.interval, 24*time.Hour)
				})
				gate := make(chan struct{})
				first := queueUnload(t, h, reasonMakeRoom)
				first.block.Store(&gate)
				synctest.Wait()
				require.Len(t, first.requests(), 1)

				queued := map[*fakeShard]reason{}
				for range tt.makeRoom {
					queued[queueUnload(t, h, reasonMakeRoom)] = reasonMakeRoom
				}
				for range tt.background {
					queued[queueUnload(t, h, reasonWatermark)] = reasonWatermark
				}
				close(gate)
				synctest.Wait()

				order := processedOrder(t, queued)
				require.LessOrEqual(t, longestRun(order, reasonMakeRoom), maxMakeRoomRun,
					"a background item waits behind a bounded number of make-room items")
				require.LessOrEqual(t, longestRun(order, reasonWatermark), 1,
					"make-room keeps priority: never two background items in a row while make-room waits")
				if tt.makeRoom > 0 && tt.background > 0 {
					require.Equal(t, reasonMakeRoom, order[0], "make-room is still taken first")
				}
				require.Equal(t, float64(tt.background), testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("watermark", "ok")))
				require.Equal(t, float64(tt.makeRoom+1), testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("make_room", "ok")))
				require.Zero(t, h.p.pending.Load())
			})
		})
	}
}

func TestLimitDrainConvergesUnderSustainedMakeRoom(t *testing.T) {
	const (
		maxLoaded  = 20
		background = 40
		unloadTime = 10 * time.Millisecond
		duration   = 5 * time.Second
	)
	for _, workers := range []int{1, 4} {
		t.Run("workers="+strconv.Itoa(workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.maxLoaded, maxLoaded)
					set(t, k.minResidency, 0)
					set(t, k.workers, workers)
					set(t, k.candidates, MaxMakeRoomCandidates)
				})
				h.reg.unloadDelay.Store(int64(unloadTime))
				makeRoomBacklog := 2*workers + 2
				for i := range maxLoaded + background {
					h.reg.load("s"+strconv.Itoa(i), "")
				}
				loadOne := func() {
					h.p.beforeLoad(cold(h, ""))
					h.reg.load("new"+strconv.Itoa(int(h.reg.Loaded())), "")
				}
				for range makeRoomBacklog {
					loadOne()
				}
				makeRoomOK := func() int {
					return int(testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("make_room", "ok")))
				}
				done := 0
				for elapsed := time.Duration(0); elapsed < duration; elapsed += unloadTime {
					time.Sleep(unloadTime)
					synctest.Wait()
					n := makeRoomOK()
					for ; done < n; done++ {
						loadOne()
					}
					require.NotZero(t, len(h.p.makeRoomCh), "make-room work never runs out")
				}

				require.Positive(t, done)
				require.Equal(t, float64(background), testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("watermark", "ok")),
					"the limit drain is not starved by make-room work")
				require.LessOrEqual(t, h.reg.Loaded(), int64(maxLoaded+makeRoomBacklog+workers),
					"loaded converges to the limit plus the make-room backlog instead of keeping the drain queued")
			})
		})
	}
}

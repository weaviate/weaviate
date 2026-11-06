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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

const (
	retryOff  = time.Duration(0)
	retryLong = time.Hour
)

func makeRoomOnce(h *harness) {
	h.p.beforeLoad(cold(h, ""))
	synctest.Wait()
}

type starvationTrigger struct {
	name      string
	configure func(k *testKnobs)
	// start releases the trigger once the shards are loaded and aged.
	start func(h *harness)
	ratio float64
	// step gives the trigger one chance to pick a victim.
	step func(h *harness)
}

func starvationTriggers() []starvationTrigger {
	tick := func(*harness) { tickOnce(time.Second) }
	unpause := func(h *harness) { set(h.t, h.knobs.paused, false) }
	return []starvationTrigger{
		{
			name: "idle sweep",
			configure: func(k *testKnobs) {
				set(nil, k.idleTTL, 30*time.Second)
				set(nil, k.rate, 1)
				set(nil, k.paused, true)
			},
			start: unpause,
			step:  tick,
		},
		{
			name: "limit drain",
			configure: func(k *testKnobs) {
				set(nil, k.maxLoaded, 2)
				set(nil, k.rate, 1)
				set(nil, k.paused, true)
			},
			start: unpause,
			step:  tick,
		},
		{
			name: "memory watermark drain",
			configure: func(k *testKnobs) {
				set(nil, k.high, 0.8)
				set(nil, k.low, 0.5)
				set(nil, k.rate, 1)
				set(nil, k.paused, true)
			},
			start: unpause,
			ratio: 0.9,
			step:  tick,
		},
		{
			name: "make-room on a registry smaller than make_room_candidates",
			configure: func(k *testKnobs) {
				set(nil, k.maxLoaded, 2)
				set(nil, k.interval, 24*time.Hour)
			},
			start: func(*harness) {},
			step:  makeRoomOnce,
		},
	}
}

func newStarvationHarness(t *testing.T, trigger starvationTrigger, retry time.Duration) (*harness, []*fakeShard) {
	h := newHarness(t, func(k *testKnobs) {
		set(t, k.retryRefusedAfter, retry)
		set(t, k.minResidency, retryOff)
		trigger.configure(k)
	})
	h.setRatio(trigger.ratio)
	shards := h.loadN(4, "")
	return h, shards
}

func TestRefusedShardsDoNotStarveTheRest(t *testing.T) {
	for _, trigger := range starvationTriggers() {
		t.Run(trigger.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, shards := newStarvationHarness(t, trigger, retryLong)
				shards[0].refuseWith(db.UnloadOutcomeManualReadOnly)
				shards[1].refuseWith(db.UnloadOutcomePending2PC)
				time.Sleep(time.Minute)
				synctest.Wait()
				trigger.start(h)

				for range 5 {
					trigger.step(h)
				}

				require.False(t, shards[2].loaded(), "the blocked oldest shards starved the newer ones")
				require.True(t, shards[0].loaded())
				require.True(t, shards[1].loaded())
				require.Len(t, shards[0].requests(), 1, "a refused shard waits out its cooldown")
				require.Len(t, shards[1].requests(), 1)
			})
		})
	}
}

func TestCooldownLevels(t *testing.T) {
	soft := func(s *fakeShard) { s.queueBusy.Store(true) }
	correctness := func(s *fakeShard) { s.refuseWith(db.UnloadOutcomeVectorOp) }
	tests := []struct {
		name           string
		arm            func(s *fakeShard)
		wantMakeRoom   bool
		wantUnloaded   bool
		wantRequests   int
		wantOvercommit float64
	}{
		{
			name:         "a background soft refusal leaves the shard to make-room",
			arm:          soft,
			wantUnloaded: true,
			wantRequests: 2,
		},
		{
			name:           "a background correctness refusal keeps make-room off too",
			arm:            correctness,
			wantRequests:   1,
			wantOvercommit: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.retryRefusedAfter, retryLong)
					set(t, k.minResidency, retryOff)
					set(t, k.idleTTL, 10*time.Second)
					set(t, k.maxLoaded, 1)
				})
				target := h.reg.load("target", "")
				tt.arm(target)
				time.Sleep(11 * time.Second)
				synctest.Wait()
				require.Len(t, target.requests(), 1, "the idle sweep tried it")
				tickOnce(time.Second)
				tickOnce(time.Second)
				require.Len(t, target.requests(), 1, "a refusal keeps the loop off for the cooldown")

				h.p.beforeLoad(cold(h, ""))
				synctest.Wait()

				require.Equal(t, tt.wantUnloaded, !target.loaded())
				require.Len(t, target.requests(), tt.wantRequests)
				_, _, overcommit := admissions(h)
				require.Equal(t, tt.wantOvercommit, overcommit)
			})
		})
	}

	t.Run("a make-room correctness refusal keeps the loop off too", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, func(k *testKnobs) {
				set(t, k.retryRefusedAfter, retryLong)
				set(t, k.minResidency, retryOff)
				set(t, k.maxLoaded, 1)
			})
			target := h.reg.load("target", "")
			target.refuseWith(db.UnloadOutcomeHalted)

			h.p.beforeLoad(cold(h, ""))
			synctest.Wait()
			require.Len(t, target.requests(), 1)

			set(t, h.knobs.idleTTL, time.Second)
			for range 5 {
				tickOnce(time.Second)
			}
			require.Len(t, target.requests(), 1)
		})
	})
}

func TestRefusedShardIsRetriedAfterItsCooldown(t *testing.T) {
	const cooldown = time.Minute
	for _, trigger := range starvationTriggers() {
		t.Run(trigger.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, shards := newStarvationHarness(t, trigger, cooldown)
				blocked := shards[0]
				blocked.refuseWith(db.UnloadOutcomeManualReadOnly)
				for _, s := range shards[1:] {
					s.inUse.Store(true)
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				trigger.start(h)

				trigger.step(h)
				require.Len(t, blocked.requests(), 1)

				time.Sleep(cooldown / 2)
				synctest.Wait()
				trigger.step(h)
				require.Len(t, blocked.requests(), 1, "still cooling down")

				time.Sleep(cooldown)
				synctest.Wait()
				trigger.step(h)
				require.Len(t, blocked.requests(), 2, "retried after the cooldown")
				require.True(t, blocked.loaded(), "the blocker still holds")

				blocked.refuse.Store(nil)
				time.Sleep(2 * cooldown)
				synctest.Wait()
				trigger.step(h)
				require.False(t, blocked.loaded(), "unloads once the blocker lifts")
			})
		})
	}
}

func TestRetryRefusedAfterZeroRetriesOnTheNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.retryRefusedAfter, retryOff)
			set(t, k.minResidency, retryOff)
			set(t, k.rate, 1)
			set(t, k.paused, true)
		})
		shards := h.loadN(2, "")
		shards[0].refuseWith(db.UnloadOutcomeManualReadOnly)
		time.Sleep(time.Minute)
		synctest.Wait()
		set(t, h.knobs.paused, false)
		set(t, h.knobs.maxLoaded, 1)
		h.p.refreshConfig()
		synctest.Wait()
		start := len(shards[0].requests())

		for i := 1; i <= 3; i++ {
			tickOnce(time.Second)
			require.Len(t, shards[0].requests(), start+i)
		}
		require.True(t, shards[1].loaded(), "without a cooldown the oldest is retried first, as before")
	})
}

func TestRetryRefusedAfterChangesApplyWithinOneTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.retryRefusedAfter, retryLong)
			set(t, k.minResidency, retryOff)
			set(t, k.maxLoaded, 1)
			set(t, k.rate, 1)
			set(t, k.interval, time.Second)
			set(t, k.paused, true)
		})
		shards := h.loadN(3, "")
		for _, s := range shards {
			s.refuseWith(db.UnloadOutcomeManualReadOnly)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		set(t, h.knobs.paused, false)
		h.p.refreshConfig()
		synctest.Wait()
		require.Len(t, shards[0].requests(), 1)

		set(t, h.knobs.retryRefusedAfter, 5*time.Second)
		tickOnce(time.Second)
		require.Equal(t, 5*time.Second, h.p.snap.Load().retryRefusedAfter)
		require.Len(t, shards[1].requests(), 1, "the next refusal takes the new value")

		for range 10 {
			tickOnce(time.Second)
		}
		require.Len(t, shards[0].requests(), 1, "a lowered value leaves a running cooldown alone")
		require.Greater(t, len(shards[1].requests()), 1, "the new value ran out")

		set(t, h.knobs.retryRefusedAfter, retryOff)
		tickOnce(time.Second)
		tickOnce(time.Second)
		require.Greater(t, len(shards[0].requests()), 1, "0 ends the running cooldowns")
	})
}

func TestIdleBacklogDoesNotCountCooledShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.retryRefusedAfter, retryLong)
			set(t, k.minResidency, retryOff)
			set(t, k.idleCheckInterval, time.Hour)
			set(t, k.rate, 1)
			set(t, k.paused, true)
		})
		shards := h.loadN(3, "")
		shards[0].refuseWith(db.UnloadOutcomeManualReadOnly)
		shards[1].refuseWith(db.UnloadOutcomeManualReadOnly)
		time.Sleep(time.Minute)
		synctest.Wait()
		set(t, h.knobs.idleTTL, 10*time.Second)
		set(t, h.knobs.paused, false)
		h.p.refreshConfig()
		synctest.Wait()
		for range 2 {
			tickOnce(time.Second)
		}
		require.False(t, shards[2].loaded(), "the backlog reached the unblocked shard")
		h.reg.mu.Lock()
		chunks := h.reg.chunks
		h.reg.mu.Unlock()

		for range 5 {
			tickOnce(time.Second)
		}

		h.reg.mu.Lock()
		defer h.reg.mu.Unlock()
		require.Equal(t, chunks, h.reg.chunks, "only cooled shards are left, so the sweep waits for idle_check_interval")
	})
}

func TestPanickingUnloadCoolsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, func(k *testKnobs) {
			set(t, k.retryRefusedAfter, retryLong)
			set(t, k.maxLoaded, 1)
			set(t, k.interval, time.Hour)
		})
		shards := h.loadN(2, "")
		shards[0].panics.Store(true)

		makeRoomOnce(h)
		makeRoomOnce(h)

		require.Len(t, shards[0].requests(), 1)
		require.False(t, shards[1].loaded(), "the next victim is tried instead")
	})
}

func TestRefusalLevelOfEveryOutcome(t *testing.T) {
	tests := []struct {
		outcome db.UnloadOutcome
		want    refusalLevel
	}{
		{db.UnloadOutcomeOK, refusalNone},
		{db.UnloadOutcomeAlreadyCold, refusalNone},
		{db.UnloadOutcomeNotInMap, refusalNone},
		{db.UnloadOutcomeIndexClosing, refusalNone},
		{db.UnloadOutcomeTorn, refusalNone},
		{db.UnloadOutcomeReaccessed, refusalNone},
		{db.UnloadOutcomeReindexUnknown, refusalNone},
		{db.UnloadOutcomeResourceReadOnly, refusalNone},
		{db.UnloadOutcomeMinResidency, refusalSoft},
		{db.UnloadOutcomeNotIdle, refusalSoft},
		{db.UnloadOutcomeQueueNotEmpty, refusalSoft},
		{db.UnloadOutcomeHashtreeInit, refusalSoft},
		{db.UnloadOutcomeLockBusy, refusalCorrectness},
		{db.UnloadOutcomeRecovering, refusalCorrectness},
		{db.UnloadOutcomeInUse, refusalCorrectness},
		{db.UnloadOutcomeHalted, refusalCorrectness},
		{db.UnloadOutcomePending2PC, refusalCorrectness},
		{db.UnloadOutcomeChangeLog, refusalCorrectness},
		{db.UnloadOutcomeAsyncOverride, refusalCorrectness},
		{db.UnloadOutcomeAsyncCheckpoint, refusalCorrectness},
		{db.UnloadOutcomeVectorOp, refusalCorrectness},
		{db.UnloadOutcomeReindex, refusalCorrectness},
		{db.UnloadOutcomeManualReadOnly, refusalCorrectness},
	}
	for _, tt := range tests {
		t.Run(string(tt.outcome), func(t *testing.T) {
			require.Equal(t, tt.want, refusalLevelOf(tt.outcome))
		})
	}
}

func TestNodeWideRefusalsSetNoCooldown(t *testing.T) {
	for _, outcome := range []db.UnloadOutcome{db.UnloadOutcomeResourceReadOnly, db.UnloadOutcomeReindexUnknown} {
		t.Run(string(outcome), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, func(k *testKnobs) {
					set(t, k.retryRefusedAfter, retryLong)
					set(t, k.minResidency, retryOff)
					set(t, k.rate, 1)
					set(t, k.paused, true)
				})
				shards := h.loadN(2, "")
				shards[0].refuseWith(outcome)
				shards[1].refuseWith(outcome)
				time.Sleep(time.Minute)
				synctest.Wait()
				set(t, h.knobs.maxLoaded, 1)
				set(t, h.knobs.paused, false)
				h.p.refreshConfig()
				synctest.Wait()

				shards[0].refuse.Store(nil)
				shards[1].refuse.Store(nil)
				tickOnce(time.Second)

				require.False(t, shards[0].loaded(), "the unload resumes on the next tick once the node recovers")
				require.Positive(t, testutil.ToFloat64(h.p.metrics.unloads.WithLabelValues("watermark", string(outcome))))
			})
		})
	}
}

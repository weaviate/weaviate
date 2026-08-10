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
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Wave 2 S1: cleanup-vs-status-visibility race in
// [ReindexProvider.autoCleanupAfterTerminal].
//
// The DTM-backed [DB.AnyLiveReindexForShard] flips to "not live" the
// instant a task lands in FAILED / CANCELLED, but [autoCleanup
// AfterTerminal] is still tearing __reindex / __ingest sidecars for
// tens of seconds after that. A backup landing in that gap sees the
// gate as open and would snapshot half-removed dirs.
//
// The fix is a per-(collection, shard) cleanup-in-progress registry
// inside ReindexProvider. The tests below pin:
//
//   1. Register flips IsCleanupInProgress to true; unregister flips
//      it back to false.
//   2. Refcount survives interleaved register/unregister on the same
//      tuple from multiple goroutines / property loops.
//   3. The registry never observes a negative refcount (defensive
//      pin against an unregister-without-register typo).
//   4. The collection / shard scoping is precise — a registration on
//      one tuple never bleeds into a sibling tuple.
//   5. The builder closure exposes a fresh probe each time so the
//      backup gate sees the live state, not a stale snapshot.
//   6. MarkCleanupInProgress gates the whole collection, because the
//      sweep it guards walks every local shard.

// newCleanupRegistryProvider builds a minimal *ReindexProvider with
// only the cleanup registry initialized. Mirrors the literal-
// construction pattern used by reindex_provider_structural_invariants
// _test.go's structuralInvariantNewBareProvider — the full
// NewReindexProvider constructor requires a fully wired *DB, which
// the cleanup-registry contract doesn't depend on.
func newCleanupRegistryProvider() *ReindexProvider {
	return &ReindexProvider{
		cleanupInProgress: make(map[reindexCleanupKey]int),
	}
}

// TestCleanupInProgress_RegisterThenUnregister pins the canonical
// flow [autoCleanupAfterTerminal] follows for a single shard:
// register before the teardown loop, run cleanup, unregister from
// the defer. While registered the shard is reported as busy; once
// unregistered the slot drops out of the map and reports false.
func TestCleanupInProgress_RegisterThenUnregister(t *testing.T) {
	p := newCleanupRegistryProvider()

	require.False(t, p.IsCleanupInProgress("C", "shard1"),
		"fresh provider must not report cleanup-in-progress")

	p.registerCleanup("C", "shard1")
	require.True(t, p.IsCleanupInProgress("C", "shard1"),
		"after registerCleanup, IsCleanupInProgress must return true")

	p.unregisterCleanup("C", "shard1")
	require.False(t, p.IsCleanupInProgress("C", "shard1"),
		"after unregisterCleanup, IsCleanupInProgress must return false")
}

// TestCleanupInProgress_RefCountIsReentrant pins the refcount
// invariant: two terminal-state transitions on different (property,
// indexType) tuples sharing the same shard must not deregister each
// other prematurely. The first unregister leaves the count at 1,
// the second drops it to 0.
func TestCleanupInProgress_RefCountIsReentrant(t *testing.T) {
	p := newCleanupRegistryProvider()

	p.registerCleanup("C", "shard1")
	p.registerCleanup("C", "shard1")
	require.True(t, p.IsCleanupInProgress("C", "shard1"),
		"after two registerCleanup calls, slot must report busy")

	p.unregisterCleanup("C", "shard1")
	require.True(t, p.IsCleanupInProgress("C", "shard1"),
		"first unregister with outstanding refcount must keep slot busy")

	p.unregisterCleanup("C", "shard1")
	require.False(t, p.IsCleanupInProgress("C", "shard1"),
		"final unregister must release the slot")
}

// TestCleanupInProgress_ScopingByCollection pins that a registration
// on (CollectionA, shard1) does not block (CollectionB, shard1).
// Without this scoping a migration on collection A would gate every
// backup of collection B that touches a shard with the same name —
// surprisingly common when shards are auto-named "shard1" via UUID
// truncation.
func TestCleanupInProgress_ScopingByCollection(t *testing.T) {
	p := newCleanupRegistryProvider()

	p.registerCleanup("CollectionA", "shard1")
	require.True(t, p.IsCleanupInProgress("CollectionA", "shard1"))
	require.False(t, p.IsCleanupInProgress("CollectionB", "shard1"),
		"cleanup registration must be scoped by collection")

	p.unregisterCleanup("CollectionA", "shard1")
	require.False(t, p.IsCleanupInProgress("CollectionA", "shard1"))
}

// TestCleanupInProgress_ScopingByShard pins that a registration on
// (C, shard1) does not block (C, shard2). The DTM activity lookup
// has the same shard-scope contract; the cleanup registry must
// match it so the gate's combined answer stays per-shard.
func TestCleanupInProgress_ScopingByShard(t *testing.T) {
	p := newCleanupRegistryProvider()

	p.registerCleanup("C", "shard1")
	require.True(t, p.IsCleanupInProgress("C", "shard1"))
	require.False(t, p.IsCleanupInProgress("C", "shard2"),
		"cleanup registration must be scoped by shard")

	p.unregisterCleanup("C", "shard1")
}

// TestCleanupInProgress_NilRegistryIsConservativeFalse pins the
// defensive nil-check: a provider built without the cleanup map
// (test fixtures using zero-value literal construction) must NOT
// panic on IsCleanupInProgress and must report false (no entries =
// nothing in flight). Register / unregister still require a
// constructed map; that contract belongs to [NewReindexProvider].
func TestCleanupInProgress_NilRegistryIsConservativeFalse(t *testing.T) {
	p := &ReindexProvider{}
	require.False(t, p.IsCleanupInProgress("C", "shard1"),
		"nil registry must not panic and must report false")
}

// TestCleanupInProgress_ZeroRefcountDeletesEntry pins the map-key
// hygiene: once the refcount hits zero the key must drop out of the
// map. Without this the registry grows unbounded across the lifetime
// of a long-running provider (one entry per (collection, shard) that
// has ever had a cleanup).
func TestCleanupInProgress_ZeroRefcountDeletesEntry(t *testing.T) {
	p := newCleanupRegistryProvider()

	p.registerCleanup("C", "shard1")
	p.unregisterCleanup("C", "shard1")

	p.cleanupInProgressMu.RLock()
	defer p.cleanupInProgressMu.RUnlock()
	_, present := p.cleanupInProgress[newReindexCleanupKey("C", "shard1")]
	require.False(t, present, "zero refcount must remove the map entry")
	require.Equal(t, 0, len(p.cleanupInProgress),
		"registry must be empty once every register has been paired")
}

// TestCleanupInProgress_LookupBuilder pins the wiring contract:
// the builder returns a closure that probes the LIVE registry on
// every invocation — not a snapshotted bool from builder-call time.
// This mirrors [ShardReindexActivityLookupBuilder]'s contract so the
// backup gate can install both via the same pattern.
func TestCleanupInProgress_LookupBuilder(t *testing.T) {
	p := newCleanupRegistryProvider()

	builder := p.CleanupInProgressLookupBuilder()
	require.NotNil(t, builder, "builder must not return nil")

	lookup := builder()
	require.NotNil(t, lookup, "builder() must return a non-nil lookup")
	require.Equal(t, ReindexHoldNone, lookup("C", "shard1"),
		"lookup on a fresh registry must report no hold")

	p.registerCleanup("C", "shard1")
	require.Equal(t, ReindexHoldCleanup, lookup("C", "shard1"),
		"lookup must observe a registration that happened AFTER the closure was built")

	p.unregisterCleanup("C", "shard1")
	require.Equal(t, ReindexHoldNone, lookup("C", "shard1"),
		"lookup must observe an unregistration that happened AFTER the closure was built")
}

// TestCleanupInProgress_ConcurrentRegisterUnregister pins that the
// refcount survives the race a real workload exposes: many
// goroutines registering and unregistering against the same
// (collection, shard) tuple. The post-condition is the only
// observable contract — the count returns to zero and the entry is
// gone — but the test would fail under -race if the map access
// path were ever unsynchronized.
func TestCleanupInProgress_ConcurrentRegisterUnregister(t *testing.T) {
	p := newCleanupRegistryProvider()

	const goroutines = 32
	const opsPerGoroutine = 64

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				p.registerCleanup("C", "shard1")
				p.unregisterCleanup("C", "shard1")
			}
		}()
	}
	wg.Wait()

	require.False(t, p.IsCleanupInProgress("C", "shard1"),
		"after paired register/unregister waves, refcount must be zero")

	p.cleanupInProgressMu.RLock()
	defer p.cleanupInProgressMu.RUnlock()
	require.Equal(t, 0, len(p.cleanupInProgress),
		"final state must be an empty map")
}

// TestMarkCleanupInProgress pins that the gate covers the collection, and that
// one release reopens it.
func TestMarkCleanupInProgress(t *testing.T) {
	p := newCleanupRegistryProvider()
	payload := &ReindexTaskPayload{
		Collection:  "C",
		UnitToShard: map[string]string{"u1": "shard1", "u2": "shard2", "u3": "shard1"},
	}

	require.False(t, p.AnyCleanupInProgress())

	release := p.MarkCleanupInProgress(payload)
	assert.True(t, p.IsCleanupInProgress("C", "shard1"))
	assert.True(t, p.IsCleanupInProgress("C", "shard2"))
	assert.True(t, p.AnyCleanupInProgress(),
		"the restore gate asks the collection-blind question")

	release()
	assert.False(t, p.IsCleanupInProgress("C", "shard1"),
		"a shard named by two units must not need two releases")
	assert.False(t, p.IsCleanupInProgress("C", "shard2"))
	assert.False(t, p.AnyCleanupInProgress())
}

// A multi-tenant submission scoped to one tenant produces a payload naming only
// that tenant's shard, but the sweep it guards
// ([Index.CleanStalePartialReindexState]) takes no shard list and removes every
// local shard's __reindex / __ingest dirs. Gating only the named shard let a
// backup hardlink a sibling tenant mid-delete.
func TestMarkCleanupInProgressGatesShardsThePayloadDoesNotName(t *testing.T) {
	p := newCleanupRegistryProvider()

	release := p.MarkCleanupInProgress(&ReindexTaskPayload{
		Collection:  "Movies",
		UnitToShard: map[string]string{"u1": "t1"},
	})

	assert.True(t, p.IsCleanupInProgress("Movies", "t2"),
		"the sweep deletes t2's sidecars too, so a backup of t2 must be refused")
	assert.Equal(t, ReindexHoldCleanup, p.HoldForShard("Movies", "t2"),
		"the refusal must name the teardown, not fall through to no hold")

	release()
	assert.False(t, p.IsCleanupInProgress("Movies", "t2"))
}

// TestCleanupGateMatchesCollectionRegardlessOfCase pins that a registration and
// a probe spelling the collection name differently still match.
func TestCleanupGateMatchesCollectionRegardlessOfCase(t *testing.T) {
	tests := []struct {
		name           string
		registerAs     string
		probeAs        string
		shards         map[string]string
		probeShard     string
		wantInProgress bool
	}{
		{
			name:           "mixed case on both sides",
			registerAs:     "MoViEs",
			probeAs:        "mOvIeS",
			shards:         map[string]string{"u1": "shard1"},
			probeShard:     "shard1",
			wantInProgress: true,
		},
		{
			name:           "case folding also covers the whole-collection guard",
			registerAs:     "movies",
			probeAs:        "Movies",
			shards:         nil,
			probeShard:     "any-shard",
			wantInProgress: true,
		},
		{
			name:           "a genuinely different collection still does not match",
			registerAs:     "movies",
			probeAs:        "Actors",
			shards:         map[string]string{"u1": "shard1"},
			probeShard:     "shard1",
			wantInProgress: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newCleanupRegistryProvider()

			release := p.MarkCleanupInProgress(&ReindexTaskPayload{
				Collection:  tc.registerAs,
				UnitToShard: tc.shards,
			})

			assert.Equal(t, tc.wantInProgress, p.IsCleanupInProgress(tc.probeAs, tc.probeShard))

			release()
			assert.False(t, p.IsCleanupInProgress(tc.probeAs, tc.probeShard),
				"the release must find the same key the registration wrote")
			assert.False(t, p.AnyCleanupInProgress(),
				"a release under a different spelling would leak the entry forever")
		})
	}
}

// drainGateProvider carries the cleanup registry and running-handle map the
// drain gate consults.
func drainGateProvider(handles map[distributedtask.TaskDescriptor]*reindexTaskHandle) *ReindexProvider {
	return &ReindexProvider{
		cleanupInProgress: make(map[reindexCleanupKey]int),
		runningHandles:    handles,
		timings:           defaultReindexTimings(),
	}
}

// TestDrainWithCleanupGateHoldsTheGateAcrossTheWait pins that the gate is shut
// before the drain wait, not after, so a timed-out drain stays guarded.
func TestDrainWithCleanupGateHoldsTheGateAcrossTheWait(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-1", Version: 1}
	payload := &ReindexTaskPayload{
		Collection:  "Movies",
		UnitToShard: map[string]string{"u1": "shard1"},
	}

	// A handle whose Done() never fires models the stuck worker.
	p := drainGateProvider(map[distributedtask.TaskDescriptor]*reindexTaskHandle{
		desc: {doneCh: make(chan struct{})},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	release, err := p.DrainWithCleanupGate(ctx, payload, desc)
	require.Error(t, err, "the drain must report that it gave up")
	require.NotNil(t, release, "the release must be usable even on timeout")

	assert.True(t, p.IsCleanupInProgress("Movies", "shard1"),
		"the worker is still writing; a backup must not capture this shard")
	assert.True(t, p.AnyCleanupInProgress(),
		"the restore gate must be shut for the same reason")

	release()
	assert.False(t, p.AnyCleanupInProgress())
}

// The node handling a cancel may own none of the collection's shards, so it
// asks the owners this before answering. It knows the collection, not which
// shards the owner holds.
func TestAnyCleanupInProgressForCollection(t *testing.T) {
	tests := []struct {
		name    string
		payload *ReindexTaskPayload
		probe   string
		want    bool
	}{
		{
			name:    "per-shard registration",
			payload: &ReindexTaskPayload{Collection: "Movies", UnitToShard: map[string]string{"u1": "shard1"}},
			probe:   "Movies",
			want:    true,
		},
		{
			name:    "collection-wide registration",
			payload: &ReindexTaskPayload{Collection: "Movies"},
			probe:   "Movies",
			want:    true,
		},
		{
			name:    "a different collection",
			payload: &ReindexTaskPayload{Collection: "Movies", UnitToShard: map[string]string{"u1": "shard1"}},
			probe:   "Actors",
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newCleanupRegistryProvider()
			require.False(t, p.AnyCleanupInProgressForCollection(tc.probe))

			release := p.MarkCleanupInProgress(tc.payload)
			assert.Equal(t, tc.want, p.AnyCleanupInProgressForCollection(tc.probe))

			release()
			assert.False(t, p.AnyCleanupInProgressForCollection(tc.probe),
				"the answer must go back down or the owner looks busy forever")
		})
	}
}

// Pins both halves of the ordering autoCleanupAfterTerminal documents: the gate
// is up for the drain rather than raised once it finishes, and a drain that
// times out hands the gate to the worker's exit instead of dropping it.
func TestAutoCleanupAfterTerminalRaisesTheGateBeforeDraining(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-1", Version: 1}
	payload := &ReindexTaskPayload{
		Collection:    "Movies",
		Properties:    []string{"body"},
		MigrationType: ReindexTypeChangeTokenization,
		UnitToShard:   map[string]string{"u1": "shard1"},
	}

	// A handle that never closes keeps the drain blocked until the drain
	// timeout, which is shortened rather than reached through serverCtx: a
	// cancelled serverCtx would also release the gate, and it is precisely
	// whether the gate outlives the hook that this test is about.
	doneCh := make(chan struct{})
	p := drainGateProvider(map[distributedtask.TaskDescriptor]*reindexTaskHandle{
		desc: {doneCh: doneCh},
	})
	p.serverCtx = context.Background()
	p.timings.terminalCleanupDrainTimeout = time.Second

	logger, _ := logrustest.NewNullLogger()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.autoCleanupAfterTerminal(&distributedtask.Task{TaskDescriptor: desc}, payload, logger)
	}()

	require.Eventually(t, func() bool { return p.AnyCleanupInProgressForCollection("Movies") },
		500*time.Millisecond, 5*time.Millisecond,
		"the gate must be up while the drain is still blocked, not after it returns")
	select {
	case <-done:
		require.Fail(t, "the drain must still be running when the gate is already up")
	default:
	}

	<-done
	// The drain timed out, and the worker it lost is still writing. Releasing
	// on return would open the gate over that writer, so the hook hands it to
	// the worker's exit instead.
	require.Never(t, func() bool {
		return !p.AnyCleanupInProgressForCollection("Movies")
	}, 300*time.Millisecond, 10*time.Millisecond,
		"the gate must outlive the hook while the worker is still writing")

	close(doneCh)
	require.Eventually(t, func() bool {
		return !p.AnyCleanupInProgressForCollection("Movies")
	}, time.Second, 5*time.Millisecond,
		"the gate must reopen once the worker is gone")
}

// A drain that times out is the case the gate exists for, so the gate must
// survive the hook's return and follow the worker instead.
func TestReleaseCleanupGateOnWorkerExitHoldsUntilTheWorkerIsGone(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-3", Version: 1}
	doneCh := make(chan struct{})
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &ReindexProvider{
		cleanupInProgress: make(map[reindexCleanupKey]int),
		runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{
			desc: {doneCh: doneCh},
		},
		serverCtx: serverCtx,
		timings:   defaultReindexTimings(),
	}

	logger, _ := logrustest.NewNullLogger()
	release := p.MarkCleanupInProgress(&ReindexTaskPayload{
		Collection:  "Movies",
		UnitToShard: map[string]string{"u1": "shard1"},
	})
	p.ReleaseCleanupGateOnWorkerExit(desc, release, logger)

	require.Never(t, func() bool {
		return !p.AnyCleanupInProgressForCollection("Movies")
	}, 200*time.Millisecond, 10*time.Millisecond,
		"the gate must stay closed while the worker is still writing")

	close(doneCh)
	require.Eventually(t, func() bool {
		return !p.AnyCleanupInProgressForCollection("Movies")
	}, time.Second, 5*time.Millisecond,
		"the gate must reopen once the worker exits")
}

// A worker that never exits must not hold the gate forever: the restore gate
// this feeds is node-wide, so one wedged worker would refuse every restore on
// the node until the process is restarted.
func TestReleaseCleanupGateOnWorkerExitGivesUpAtTheCap(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-wedged", Version: 1}
	p := &ReindexProvider{
		cleanupInProgress: make(map[reindexCleanupKey]int),
		runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{
			// Never closed: this is the worker that does not come back.
			desc: {doneCh: make(chan struct{})},
		},
		// Never cancelled, so the cap is the only way out.
		serverCtx: context.Background(),
		timings:   defaultReindexTimings(),
	}

	logger, _ := logrustest.NewNullLogger()
	release := p.MarkCleanupInProgress(&ReindexTaskPayload{
		Collection:  "Movies",
		UnitToShard: map[string]string{"u1": "shard1"},
	})
	p.timings.workerExitGateCap = 50 * time.Millisecond
	p.ReleaseCleanupGateOnWorkerExit(desc, release, logger)

	require.Eventually(t, func() bool {
		return !p.AnyCleanupInProgressForCollection("Movies")
	}, 5*time.Second, 5*time.Millisecond,
		"the gate must reopen at the cap rather than wait for a process restart")
}

// Pins the two things that decide whether the gate is held: what the payload
// says there is to tear down, and whether the task's units show a worker ever
// ran. FAILED with nothing claimed still holds the gate, since a failed task
// means a worker reported something, unlike CANCELLED with nothing claimed.
func TestAutoCleanupAfterTerminalHoldsTheGateOnlyWhenThereIsSomethingToClean(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "task-2", Version: 1}
	const shard = "shard1"
	teardownPayload := &ReindexTaskPayload{
		Collection: "Movies", Properties: []string{"body"},
		MigrationType: ReindexTypeChangeTokenization,
		UnitToShard:   map[string]string{"u1": shard},
	}

	tests := []struct {
		name     string
		payload  *ReindexTaskPayload
		status   distributedtask.TaskStatus
		unit     distributedtask.UnitStatus
		wantHold bool
	}{
		{
			name: "migration type tears nothing down",
			payload: &ReindexTaskPayload{
				Collection: "Movies", Properties: []string{"body"},
				MigrationType: ReindexMigrationType("something-else"),
				UnitToShard:   map[string]string{"u1": shard},
			},
			status: distributedtask.TaskStatusFailed,
			unit:   distributedtask.UnitStatusInProgress,
		},
		{
			name:    "cancelled, no unit ever claimed — the submit rollback",
			payload: teardownPayload,
			status:  distributedtask.TaskStatusCancelled,
			unit:    distributedtask.UnitStatusPending,
		},
		{
			name:     "cancelled after a worker claimed a unit",
			payload:  teardownPayload,
			status:   distributedtask.TaskStatusCancelled,
			unit:     distributedtask.UnitStatusInProgress,
			wantHold: true,
		},
		{
			name:     "failed with nothing claimed",
			payload:  teardownPayload,
			status:   distributedtask.TaskStatusFailed,
			unit:     distributedtask.UnitStatusPending,
			wantHold: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A handle that never drains is the window in which a raised gate
			// is visible from outside. The drain budget is what separates the
			// two expectations: a skip has to return without touching the
			// drain, so it is set far above the deadline the skip rows assert;
			// a hold has to be observable and then end, so it is set low enough
			// that the drain times out and the call returns on its own.
			p := drainGateProvider(map[distributedtask.TaskDescriptor]*reindexTaskHandle{
				desc: {doneCh: make(chan struct{})},
			})
			p.serverCtx = context.Background()
			if tc.wantHold {
				p.timings.terminalCleanupDrainTimeout = time.Second
			} else {
				p.timings.terminalCleanupDrainTimeout = 30 * time.Second
			}

			task := &distributedtask.Task{
				TaskDescriptor: desc,
				Status:         tc.status,
				Units:          map[string]*distributedtask.Unit{"u1": {ID: "u1", Status: tc.unit}},
			}

			logger, _ := logrustest.NewNullLogger()
			done := make(chan struct{})
			enterrors.GoWrapper(func() {
				defer close(done)
				p.autoCleanupAfterTerminal(task, tc.payload, logger)
			}, logger)

			if !tc.wantHold {
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					require.Fail(t, "the teardown must return at once rather than drain a worker that never started")
				}
				require.Equal(t, ReindexHoldNone, p.HoldForShard("Movies", shard),
					"nothing was written, so nothing may be held")
				require.False(t, p.AnyCleanupInProgressForCollection("Movies"),
					"and the collection-wide probe must agree")
				return
			}

			require.Eventually(t,
				func() bool { return p.HoldForShard("Movies", shard) == ReindexHoldCleanup },
				time.Second, 5*time.Millisecond,
				"the sidecars are still coming down, so the backup must be refused")
			<-done
		})
	}
}

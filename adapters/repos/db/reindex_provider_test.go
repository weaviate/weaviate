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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
//   6. uniqueShardsFromPayload dedupes shards across UnitToShard
//      entries (multi-property migrations route multiple units to
//      the same shard).

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
	_, present := p.cleanupInProgress[reindexCleanupKey{collection: "C", shard: "shard1"}]
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
	require.False(t, lookup("C", "shard1"),
		"lookup on a fresh registry must report false")

	p.registerCleanup("C", "shard1")
	require.True(t, lookup("C", "shard1"),
		"lookup must observe a registration that happened AFTER the closure was built")

	p.unregisterCleanup("C", "shard1")
	require.False(t, lookup("C", "shard1"),
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

// TestUniqueShardsFromPayload_Dedupes pins that the helper used by
// [autoCleanupAfterTerminal] to enumerate shards collapses duplicates
// — multi-property semantic migrations route N units to the same
// shard, and we must register each shard exactly once so the
// matching unregister loop releases the slot symmetrically.
func TestUniqueShardsFromPayload_Dedupes(t *testing.T) {
	payload := &ReindexTaskPayload{
		Collection: "C",
		UnitToShard: map[string]string{
			"u1": "shardA",
			"u2": "shardB",
			"u3": "shardA", // dup
			"u4": "shardB", // dup
			"u5": "shardC",
		},
	}
	out := uniqueShardsFromPayload(payload)
	assert.ElementsMatch(t, []string{"shardA", "shardB", "shardC"}, out,
		"unique shards must include each distinct value exactly once")
}

// TestUniqueShardsFromPayload_EmptyPayload pins the boundary: an
// empty UnitToShard returns a nil slice (callers iterate it with
// range, so nil is correct).
func TestUniqueShardsFromPayload_EmptyPayload(t *testing.T) {
	payload := &ReindexTaskPayload{Collection: "C", UnitToShard: nil}
	require.Nil(t, uniqueShardsFromPayload(payload))

	payload = &ReindexTaskPayload{Collection: "C", UnitToShard: map[string]string{}}
	require.Nil(t, uniqueShardsFromPayload(payload))
}

// TestUniqueShardsFromPayload_SkipsEmptyShardName pins defensive
// handling of a UnitToShard entry whose value is an empty string —
// a malformed payload should not produce a zero-string registration
// that the gate would silently never match.
func TestUniqueShardsFromPayload_SkipsEmptyShardName(t *testing.T) {
	payload := &ReindexTaskPayload{
		Collection: "C",
		UnitToShard: map[string]string{
			"u1": "shardA",
			"u2": "",
		},
	}
	out := uniqueShardsFromPayload(payload)
	assert.ElementsMatch(t, []string{"shardA"}, out)
}

// A dropped collection is not a failed sweep, and a sweep that did not reach
// every shard is neither. Nothing was swept in either case, so the difference
// is only visible in what the operator is told.
func TestTerminalCleanupOutcome(t *testing.T) {
	tests := []struct {
		name     string
		outcome  terminalSweepOutcome
		wantWarn bool
		wantMsg  string
	}{
		{
			name:    "every shard swept",
			outcome: terminalSweepClean,
			wantMsg: "auto-cleanup after terminal status: partial sidecar state cleared on this node",
		},
		{
			name:    "the collection is not on this node",
			outcome: terminalSweepDropped,
			wantMsg: "auto-cleanup after terminal status: the collection is not on this node, so its partial sidecar state is not here either",
		},
		{
			name:     "a shard could not be swept",
			outcome:  terminalSweepFailed,
			wantWarn: true,
			wantMsg:  "auto-cleanup after terminal status: some shards could not be swept, so any partial sidecar state on them is still there",
		},
		{
			name:     "shards were never reached",
			outcome:  terminalSweepUnknown,
			wantWarn: true,
			wantMsg:  "auto-cleanup after terminal status: could not check every shard on this node, so any partial sidecar state on the shards it did not reach is still there",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, warn := terminalCleanupOutcome(tc.outcome)
			require.Equal(t, tc.wantMsg, msg)
			require.Equal(t, tc.wantWarn, warn)
		})
	}
}

// A post-terminal cleanup runs one sweep per (property, index type) and reports
// the worst of them, so the ordering decides which one speaks for the run.
func TestTerminalSweepOutcomeOrdering(t *testing.T) {
	require.Greater(t, terminalSweepFailed, terminalSweepUnknown,
		"knowing state is on disk outranks not knowing")
	require.Greater(t, terminalSweepUnknown, terminalSweepDropped,
		"a shard nobody looked at outranks a collection that is going away")
	require.Greater(t, terminalSweepDropped, terminalSweepClean)
}

// A sweep reports one error for the whole walk, and a collection deleted
// mid-walk can share it with a shard the sweep already failed on. What the
// operator is told about that shard has to survive the delete.
func TestClassifyTerminalSweep(t *testing.T) {
	shardFailed := func(inner error) error {
		return fmt.Errorf("%w: %w", ErrCleanupShardFailed,
			fmt.Errorf("shard %q: %w", "tenant-1", inner))
	}
	diskFull := errors.New("disk is full")
	unmarked := errors.New("something no marker covers")

	tests := []struct {
		name        string
		err         error
		wantOutcome terminalSweepOutcome
		wantFailure error
	}{
		{
			name:        "every shard swept",
			wantOutcome: terminalSweepClean,
		},
		{
			name:        "the collection is being deleted",
			err:         classifyCloseCause(errIndexDropped),
			wantOutcome: terminalSweepDropped,
		},
		{
			name:        "a shard could not be swept",
			err:         shardFailed(diskFull),
			wantOutcome: terminalSweepFailed,
			wantFailure: diskFull,
		},
		{
			name:        "the walk stopped before it reached every shard",
			err:         classifyCloseCause(errIndexShutdown),
			wantOutcome: terminalSweepUnknown,
			wantFailure: errIndexShutdown,
		},
		{
			name:        "the walk skipped a shard nothing explained",
			err:         classifyCloseCause(fmt.Errorf("%w: shard-b", errShardsSkipped)),
			wantOutcome: terminalSweepUnknown,
			wantFailure: errShardsSkipped,
		},
		{
			// No known producer; defaults to unknown rather than failed.
			name:        "an error carrying none of the markers",
			err:         unmarked,
			wantOutcome: terminalSweepUnknown,
			wantFailure: unmarked,
		},
		{
			name:        "a shard failed and then the collection was deleted",
			err:         errors.Join(shardFailed(diskFull), classifyCloseCause(errIndexDropped)),
			wantOutcome: terminalSweepFailed,
			wantFailure: diskFull,
		},
		{
			name:        "a shard failed and the walk was cut short",
			err:         errors.Join(shardFailed(diskFull), classifyCloseCause(errIndexShutdown)),
			wantOutcome: terminalSweepFailed,
			wantFailure: diskFull,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome, failure := classifyTerminalSweep(tc.err)
			require.Equal(t, tc.wantOutcome, outcome)
			if tc.wantFailure == nil {
				require.NoError(t, failure)
				return
			}
			require.ErrorIs(t, failure, tc.wantFailure,
				"a failure the operator has to act on must reach the log, whatever else "+
					"the same sweep reported")
		})
	}
}

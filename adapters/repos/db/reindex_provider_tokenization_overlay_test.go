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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test*TokenizationOverlay* pin the per-shard tokenization overlay
// lifecycle that [ReindexProvider.OnGroupCompleted] orchestrates for
// https://github.com/weaviate/0-weaviate-issues/issues/216 Gap B. The helpers
// [maybeWirePerPropOverlaySet] and
// [maybeClearTokenizationOverlayOnAllFailed] encapsulate the SET (per
// prop, atomic with the bucket-pointer flip via the task's
// onPropSwapped hook) and the defensive CLEAR (post-loop, all-failed) so
// the Gap B failure modes can be regression-tested without standing up a
// full provider + DB + index.
//
// The per-prop-atomic wiring is itself a correctness fix: the previous
// design set the overlay once-for-the-whole-shard BEFORE the per-task
// RunSwapOnShard loop, opening a disk-I/O-sized window (across
// RunSwapOnShard's tracker/sentinel/prop preamble) where overlay=NEW
// while the bucket was still OLD. A BM25 query in that window tokenized
// input with the new analyzer and looked it up in the still-old bucket,
// returning a transiently wrong count (0 for the reverse field→word
// case). These tests pin that the overlay is now established only when
// the per-prop hook fires — which production invokes immediately after
// each store.SwapBucketPointer.
//
// The most important regression to guard against is the original
// Copilot-review finding (PR https://github.com/weaviate/weaviate/pull/11322 review comment 3254170106):
// if every per-task RunSwapOnShard fails before flipping its bucket
// pointer, the migration transitions to FAILED, the cluster-wide
// schema flip is skipped, OnTaskCompleted's explicit clear hook
// never runs, and TokenizationFor's self-clear-on-catchup never
// fires (because the live schema stays at the pre-migration value
// and never matches the overlay's target). Under per-prop-atomic
// wiring the all-failed path never invokes the hook (no flip → no
// set), so the overlay is never written in the first place; the
// defensive clear remains as an idempotent backstop.

// fireAllPropHooks invokes the wired onPropSwapped hook for every prop
// on every task, simulating a swap loop where every prop's bucket
// pointer flipped. Returns the number of hooks fired.
func fireAllPropHooks(tasks []*ShardReindexTaskGeneric, props []string) int {
	fired := 0
	for _, task := range tasks {
		if task == nil || task.onPropSwapped == nil {
			continue
		}
		for _, propName := range props {
			task.onPropSwapped(propName)
			fired++
		}
	}
	return fired
}

func TestMaybeWirePerPropOverlaySet_TokenizationChange_WiresAndSets(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"change-tokenization migration with non-empty target must wire the per-prop hook")

	// Wiring alone must NOT set the overlay — the disk-I/O-sized window
	// the fix closes only exists when the overlay is set BEFORE the
	// flip. The overlay is established only when the hook fires.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"wiring must not pre-set the overlay; that's the bug being fixed")

	require.Equal(t, len(payload.Properties), fireAllPropHooks(tasks, payload.Properties),
		"hook must be wired on the task")
	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"after the per-prop hook fires, the overlay overrides the live schema value")
	assert.Equal(t, "field", s.TokenizationFor("description", "word"))
}

func TestMaybeWirePerPropOverlaySet_FilterableVariant_WiresAndSets(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		TargetTokenization: "word",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"change-tokenization-filterable migration must also wire the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "word", s.TokenizationFor("name", "field"))
}

func TestMaybeWirePerPropOverlaySet_NonTokenizationMigration_NoOp(t *testing.T) {
	for _, mt := range []ReindexMigrationType{
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeEnableRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			s := &Shard{}
			tasks := []*ShardReindexTaskGeneric{{}}
			payload := &ReindexTaskPayload{
				MigrationType:      mt,
				TargetTokenization: "field",
				Properties:         []string{"name"},
			}
			require.False(t, maybeWirePerPropOverlaySet(s, payload, tasks),
				"non-tokenization-changing migration must NOT wire the hook")
			assert.Nil(t, tasks[0].onPropSwapped,
				"no hook should be installed for a non-tokenization migration")
			assert.Equal(t, "word", s.TokenizationFor("name", "word"),
				"no overlay set → fall back to live schema")
		})
	}
}

func TestMaybeWirePerPropOverlaySet_EmptyTargetTokenization_NoOp(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "", // payload missing target
		Properties:         []string{"name"},
	}
	require.False(t, maybeWirePerPropOverlaySet(s, payload, tasks),
		"empty target tokenization must skip wiring — better than writing an empty override")
	assert.Nil(t, tasks[0].onPropSwapped)
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestMaybeWirePerPropOverlaySet_NilInputs_NoOp(t *testing.T) {
	// Pure guard against nil-deref under unexpected call sites; both
	// inputs are non-nil in production but defensive checks let the
	// helper be tested via unit tests without bringing up a real
	// shard.
	require.False(t, maybeWirePerPropOverlaySet(nil, &ReindexTaskPayload{}, nil))
	require.False(t, maybeWirePerPropOverlaySet(&Shard{}, nil, nil))
}

func TestMaybeWirePerPropOverlaySet_NilTaskInSlice_Skipped(t *testing.T) {
	// A nil task entry must not panic — defensive, mirrors the
	// production loop's nil guard.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{nil, {}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks))
	require.NotNil(t, tasks[1].onPropSwapped, "non-nil task must get the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AllFailed_Clears(t *testing.T) {
	// #216 Gap B regression. Under per-prop-atomic wiring the all-failed
	// path never fires the hook, so the overlay is never set. The
	// defensive clear is an idempotent backstop that must leave the
	// shard aligned with the live (OLD) schema either way.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// All swaps failed → no hook fired → overlay never set.
	require.Equal(t, "word", s.TokenizationFor("name", "word"),
		"all-failed: overlay must not have been set (no flip → no hook)")

	const anySwapped = false
	require.True(t, maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"defensive clear must apply when wasSet=true and anySwapped=false")

	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"after all-failed clear: TokenizationFor must return live (OLD) value")
	assert.Equal(t, "word", s.TokenizationFor("description", "word"))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AnySwapped_NoOp(t *testing.T) {
	// Partial success path: at least one per-task swap returned nil
	// → at least one bucket pointer flipped → its hook fired and set
	// the overlay. The overlay must STAY set so the swapped index
	// type's bucket content stays aligned with the query analyzer.
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	require.True(t, maybeWirePerPropOverlaySet(s, payload, tasks))
	fireAllPropHooks(tasks, payload.Properties) // a swap succeeded → hook fired

	const wasSet, anySwapped = true, true
	require.False(t, maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"clear must NOT apply when at least one swap succeeded")

	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"partial-success path: overlay must remain set")
}

func TestMaybeClearTokenizationOverlayOnAllFailed_WasNotSet_NoOp(t *testing.T) {
	// Symmetric to the wiring helper's no-op cases (non-tokenization
	// migrations, empty target): if wiring was skipped, CLEAR must also
	// be a no-op regardless of anySwapped — there's nothing to clear.
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable, // non-tokenization
		Properties:    []string{"name"},
	}

	for _, anySwapped := range []bool{true, false} {
		require.False(t, maybeClearTokenizationOverlayOnAllFailed(s, payload, false, anySwapped),
			"wasSet=false: clear must be a no-op (anySwapped=%v)", anySwapped)
	}
}

func TestMaybeClearTokenizationOverlayOnAllFailed_NilInputs_NoOp(t *testing.T) {
	require.False(t, maybeClearTokenizationOverlayOnAllFailed(nil, &ReindexTaskPayload{}, true, false))
	require.False(t, maybeClearTokenizationOverlayOnAllFailed(&Shard{}, nil, true, false))
}

// TestTokenizationOverlay_AllFailedSwap_EndToEndLifecycle pins the
// end-to-end behavior on the shard for the canonical #216 Gap B
// failure scenario: per-prop hook wired, every swap fails (so no hook
// fires), post-loop CLEAR. Mirrors runShardSwapPhase's per-shard branch.
func TestTokenizationOverlay_AllFailedSwap_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	// Step 1: runShardSwapPhase wires the per-prop hook.
	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// Step 2: simulate every per-task RunSwapOnShard returning an error
	// BEFORE flipping any bucket pointer — the hook never fires.
	const anySwapped = false

	// Step 3: runShardSwapPhase's post-loop defensive CLEAR.
	cleared := maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.True(t, cleared,
		"end-to-end: defensive clear must fire on all-failed path")

	// Final state: overlay empty, query path returns the live schema
	// value untouched. This is what prevents the permanent
	// misalignment the migration's FAILED transition would otherwise
	// leave behind.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"end-to-end: post all-failed clear, TokenizationFor returns live (untouched OLD) value")
}

// TestTokenizationOverlay_AnySwapped_EndToEndLifecycle pins the
// partial-success path: per-prop hook wired, ≥ 1 per-task swap
// succeeded (hook fired), post-loop CLEAR is a no-op so the overlay
// stays for the swapped index types. Mirrors the same
// runShardSwapPhase branch.
func TestTokenizationOverlay_AnySwapped_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	tasks := []*ShardReindexTaskGeneric{{}}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// At least one per-task swap succeeded → bucket pointer flipped
	// for that index type → its hook fired. anySwapped = true.
	fireAllPropHooks(tasks, payload.Properties)
	const anySwapped = true

	cleared := maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.False(t, cleared,
		"partial-success path: defensive clear must NOT fire")

	// Overlay stays set; queries tokenize input for the NEW value
	// (matching the swapped bucket).
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

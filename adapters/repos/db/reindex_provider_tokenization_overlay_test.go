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

// Unit coverage for the #216 Gap B overlay set/clear lifecycle without a
// full provider+DB+index. Key invariant: the overlay is set only when
// the per-prop hook fires, never eagerly at wiring time. The all-failed
// case (orig. Copilot finding, PR https://github.com/weaviate/weaviate/pull/11322 review comment 3254170106)
// is the subtle one; see [maybeClearTokenizationOverlayOnAllFailed].

// fireAllPropHooks simulates a swap loop where every prop flipped.
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

	// Wiring alone must NOT set the overlay; it is established only when
	// the hook fires. Setting it before the flip is the bug being fixed.
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

	wasSet := maybeWirePerPropOverlaySet(s, payload, tasks)
	require.True(t, wasSet)

	// No flip happens, so the hook never fires.
	const anySwapped = false

	cleared := maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.True(t, cleared,
		"end-to-end: defensive clear must fire on all-failed path")

	// Overlay cleared, so the query returns the untouched OLD value
	// rather than the misalignment the FAILED migration would leave.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"end-to-end: post all-failed clear, TokenizationFor returns live (untouched OLD) value")
}

// TestTokenizationOverlay_AnySwapped_EndToEndLifecycle: a partial
// success (≥1 flip) must leave the overlay set, so the defensive clear
// is a no-op.
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

	fireAllPropHooks(tasks, payload.Properties)
	const anySwapped = true

	cleared := maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.False(t, cleared,
		"partial-success path: defensive clear must NOT fire")

	// Overlay stays set; queries tokenize input for the NEW value
	// (matching the swapped bucket).
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

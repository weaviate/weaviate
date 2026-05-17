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
// [maybeSetTokenizationOverlayPreSwap] and
// [maybeClearTokenizationOverlayOnAllFailed] encapsulate the SET (pre-
// per-task-swap) and the defensive CLEAR (post-loop, all-failed) so
// the Gap B failure modes can be regression-tested without standing
// up a full provider + DB + index.
//
// The most important regression to guard against is the original
// Copilot-review finding (PR https://github.com/weaviate/weaviate/pull/11322 review comment 3254170106):
// if every per-task RunSwapOnShard fails before flipping its bucket
// pointer, the migration transitions to FAILED, the cluster-wide
// schema flip is skipped, OnTaskCompleted's explicit clear hook
// never runs, and TokenizationFor's self-clear-on-catchup never
// fires (because the live schema stays at the pre-migration value
// and never matches the overlay's target). Without the all-failed
// defensive clear, the overlay would stay set permanently, leaving
// queries to tokenize input against the target value while every
// bucket still has the source-tokenized content — wrong results
// until operator repair.

func TestMaybeSetTokenizationOverlayPreSwap_TokenizationChange_Sets(t *testing.T) {
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	require.True(t, maybeSetTokenizationOverlayPreSwap(s, payload),
		"change-tokenization migration with non-empty target must set the overlay")

	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"overlay should override the live schema value")
	assert.Equal(t, "field", s.TokenizationFor("description", "word"))
}

func TestMaybeSetTokenizationOverlayPreSwap_FilterableVariant_Sets(t *testing.T) {
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenizationFilterable,
		TargetTokenization: "word",
		Properties:         []string{"name"},
	}
	require.True(t, maybeSetTokenizationOverlayPreSwap(s, payload),
		"change-tokenization-filterable migration must also set the overlay")
	assert.Equal(t, "word", s.TokenizationFor("name", "field"))
}

func TestMaybeSetTokenizationOverlayPreSwap_NonTokenizationMigration_NoOp(t *testing.T) {
	s := &Shard{}
	for _, mt := range []ReindexMigrationType{
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeEnableRangeable,
	} {
		t.Run(string(mt), func(t *testing.T) {
			payload := &ReindexTaskPayload{
				MigrationType:      mt,
				TargetTokenization: "field",
				Properties:         []string{"name"},
			}
			require.False(t, maybeSetTokenizationOverlayPreSwap(s, payload),
				"non-tokenization-changing migration must NOT set the overlay")
			// Overlay should still be empty (no entries for "name").
			assert.Equal(t, "word", s.TokenizationFor("name", "word"),
				"no overlay set → fall back to live schema")
		})
	}
}

func TestMaybeSetTokenizationOverlayPreSwap_EmptyTargetTokenization_NoOp(t *testing.T) {
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "", // payload missing target
		Properties:         []string{"name"},
	}
	require.False(t, maybeSetTokenizationOverlayPreSwap(s, payload),
		"empty target tokenization must skip the SET — better than writing an empty override")
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestMaybeSetTokenizationOverlayPreSwap_NilInputs_NoOp(t *testing.T) {
	// Pure guard against nil-deref under unexpected call sites; both
	// inputs are non-nil in production but defensive checks let the
	// helper be tested via unit tests without bringing up a real
	// shard.
	require.False(t, maybeSetTokenizationOverlayPreSwap(nil, &ReindexTaskPayload{}))
	require.False(t, maybeSetTokenizationOverlayPreSwap(&Shard{}, nil))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AllFailed_Clears(t *testing.T) {
	// This is the central #216 Gap B regression: every per-task swap
	// failed → no bucket pointer flipped → overlay must be cleared so
	// the FAILED migration doesn't leave the shard permanently
	// misaligned.
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name", "description"},
	}
	require.True(t, maybeSetTokenizationOverlayPreSwap(s, payload))
	require.Equal(t, "field", s.TokenizationFor("name", "word"),
		"sanity: SET should have written the overlay")

	// Simulate the all-failed swap loop outcome.
	const wasSet, anySwapped = true, false
	require.True(t, maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"defensive clear must apply when wasSet=true and anySwapped=false")

	// The overlay is gone → TokenizationFor falls back to the live
	// schema value. The migration's FAILED transition skipped the
	// cluster-wide schema flip, so live is still the OLD value, and
	// the bucket is also still OLD → query correctly tokenizes input
	// for the OLD bucket content.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"after all-failed clear: TokenizationFor must return live (OLD) value")
	assert.Equal(t, "word", s.TokenizationFor("description", "word"))
}

func TestMaybeClearTokenizationOverlayOnAllFailed_AnySwapped_NoOp(t *testing.T) {
	// Partial success path: at least one per-task swap returned nil
	// → at least one bucket pointer flipped. The overlay must STAY
	// set so the swapped index type's bucket content stays aligned
	// with the query analyzer. The half-applied state surfaces via
	// #221's FAILED-task repair_command for operator repair.
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	require.True(t, maybeSetTokenizationOverlayPreSwap(s, payload))

	const wasSet, anySwapped = true, true
	require.False(t, maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped),
		"clear must NOT apply when at least one swap succeeded")

	assert.Equal(t, "field", s.TokenizationFor("name", "word"),
		"partial-success path: overlay must remain set")
}

func TestMaybeClearTokenizationOverlayOnAllFailed_WasNotSet_NoOp(t *testing.T) {
	// Symmetric to the SET helper's no-op cases (non-tokenization
	// migrations, empty target): if SET was skipped, CLEAR must also
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
// failure scenario: pre-swap SET, every swap fails, post-loop CLEAR.
// Mirrors OnGroupCompleted's per-shard branch exactly.
func TestTokenizationOverlay_AllFailedSwap_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	// Step 1: OnGroupCompleted's pre-swap SET.
	wasSet := maybeSetTokenizationOverlayPreSwap(s, payload)
	require.True(t, wasSet)

	// Step 2: simulate every per-task RunSwapOnShard returning an
	// error. anySwapped stays false.
	const anySwapped = false

	// Step 3: OnGroupCompleted's post-loop defensive CLEAR.
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
// partial-success path: pre-swap SET, ≥ 1 per-task swap succeeded,
// post-loop CLEAR is a no-op so the overlay stays for the swapped
// index types. Mirrors the same OnGroupCompleted branch.
func TestTokenizationOverlay_AnySwapped_EndToEndLifecycle(t *testing.T) {
	s := &Shard{}
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}

	wasSet := maybeSetTokenizationOverlayPreSwap(s, payload)
	require.True(t, wasSet)

	// At least one per-task swap succeeded → bucket pointer flipped
	// for that index type. anySwapped = true.
	const anySwapped = true

	cleared := maybeClearTokenizationOverlayOnAllFailed(s, payload, wasSet, anySwapped)
	require.False(t, cleared,
		"partial-success path: defensive clear must NOT fire")

	// Overlay stays set; queries tokenize input for the NEW value
	// (matching the swapped bucket).
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}

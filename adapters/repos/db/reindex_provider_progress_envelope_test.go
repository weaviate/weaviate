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

// Pins the per-task-to-per-unit envelope mapping. Closes
// weaviate/0-weaviate-issues#232 Finding 1 at the sender-side root
// cause: change-tokenization runs 2 sub-tasks per unit (searchable +
// filterable) and without composition the second task's iteration
// re-emitted progress from 0, looking like a regression on the same
// /v1/tasks field.
func TestComposeProgressEnvelope(t *testing.T) {
	t.Run("single sub-task is a passthrough capped at 0.99", func(t *testing.T) {
		// With N=1, the formula (idx + p) / N reduces to p; only the
		// 0.99 cap kicks in (so we don't pre-empt the FINISHED transition).
		assert.Equal(t, float32(0.0), composeProgressEnvelope(0, 1, 0.0))
		assert.Equal(t, float32(0.5), composeProgressEnvelope(0, 1, 0.5))
		assert.Equal(t, float32(0.99), composeProgressEnvelope(0, 1, 0.99))
		assert.Equal(t, float32(0.99), composeProgressEnvelope(0, 1, 1.0))
	})

	t.Run("two sub-tasks split [0, 0.99] monotonically", func(t *testing.T) {
		// Reproduces the QA recipe's shape (searchable then filterable
		// retokenize for change-tokenization with both index types).
		// Sub-task 0 climbs 0 → 0.495; sub-task 1 climbs 0.5 → 0.99.
		seq := []struct {
			idx      int
			progress float32
			want     float32
		}{
			{0, 0.0, 0.0},
			{0, 0.5, 0.25},
			{0, 0.99, 0.495},
			{1, 0.0, 0.5},
			{1, 0.5, 0.75},
			{1, 0.99, 0.99}, // cap kicks in: 1.99/2 = 0.995 → clamped to 0.99
			{1, 1.0, 0.99},
		}
		var prev float32
		for i, step := range seq {
			got := composeProgressEnvelope(step.idx, 2, step.progress)
			assert.InDeltaf(t, step.want, got, 1e-5,
				"idx=%d progress=%v: got=%v want=%v", step.idx, step.progress, got, step.want)
			// Self-check: each step must be ≥ the previous, never regress.
			if i > 0 {
				assert.GreaterOrEqual(t, got, prev,
					"envelope regressed at step %d: %v < %v", i, got, prev)
			}
			prev = got
		}
	})

	t.Run("three sub-tasks split into thirds", func(t *testing.T) {
		// Future-proofing: a migration spanning three sub-tasks
		// (e.g. searchable + filterable + sidecar) still composes.
		assert.InDelta(t, float32(0.0), composeProgressEnvelope(0, 3, 0.0), 1e-5)
		assert.InDelta(t, float32(0.333), composeProgressEnvelope(0, 3, 1.0), 1e-3)
		assert.InDelta(t, float32(0.333), composeProgressEnvelope(1, 3, 0.0), 1e-3)
		assert.InDelta(t, float32(0.667), composeProgressEnvelope(1, 3, 1.0), 1e-3)
		assert.InDelta(t, float32(0.667), composeProgressEnvelope(2, 3, 0.0), 1e-3)
		assert.InDelta(t, float32(0.99), composeProgressEnvelope(2, 3, 1.0), 1e-5)
	})

	t.Run("degenerate inputs are tolerated", func(t *testing.T) {
		// Defensive — production never reaches totalTasks=0 because
		// createReindexTasks always returns ≥1 task, but a programming
		// regression there must not crash the iteration loop with a
		// division-by-zero NaN reaching the FSM.
		require.Equal(t, float32(0), composeProgressEnvelope(0, 0, 0.5))
		// Negative envelope (impossible via the sub-task math, but guard
		// against a future caller that does subtraction) is clamped to 0.
		assert.GreaterOrEqual(t, composeProgressEnvelope(0, 2, -0.5), float32(0))
	})
}

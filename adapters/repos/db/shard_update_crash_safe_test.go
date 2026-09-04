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

//go:build integrationTest

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storobj"
)

// The crash-safe update tests mirror the crash-safe delete tests for the
// update path that CHANGES the docID: the row rewrite retires the old docID
// exactly like a delete retires it, so the old docID's inverted cleanup must
// be durable before the rewritten row can be. Without that ordering a crash
// can leave orphan postings for a docID that later reaches the free list.

// crashSafeUpdatedVersion builds a new version of obj with different props
// AND a different vector. The vector change is what forces a docID change
// (compareObjsForInsertStatus treats a prop-only change as docID-preserved).
func crashSafeUpdatedVersion(obj *storobj.Object, className string) *storobj.Object {
	updated := crashSafeTestObject(className)
	updated.Object.ID = obj.ID()
	updated.Object.Properties = map[string]interface{}{
		crashSafeTextProp: "charlie delta",
		crashSafeIntProp:  float64(7),
	}
	updated.Vector = []float32{0.9, 0.8, 0.7}
	return updated
}

// TestUpdateCrashSafe_NoOrphanPostingAtAnyPhase walks every phase of a
// docID-changing update and asserts the same invariant as the delete tests,
// transposed: at no intermediate state may an inverted posting reference the
// OLD docID once the object row no longer does. The old ordering (row
// rewrite first, inverted cleanup after, no barrier) violates this between
// the row write and the cleanup.
func TestUpdateCrashSafe_NoOrphanPostingAtAnyPhase(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "UpdateCrashSafeInvariant")

	obj := crashSafeTestObject("UpdateCrashSafeInvariant")
	obj.Vector = []float32{0.1, 0.2, 0.3}
	require.NoError(t, s.PutObject(ctx, obj))
	prevSt := crashSafeReadObjectState(t, s, obj.ID())

	// Sanity: postings exist for the initial docID.
	require.True(t, crashSafeAnyPostingPresent(t, ctx, s, prevSt))

	currentDocID := func() uint64 {
		return crashSafeReadObjectState(t, s, obj.ID()).docID
	}

	var phases []string
	s.testPutPhaseHook = func(phase string) {
		phases = append(phases, phase)
		// A posting for the outgoing docID may only exist while the row
		// still references that docID. Once the row was rewritten under the
		// new docID, a surviving old posting is an orphan.
		if crashSafeAnyPostingPresent(t, ctx, s, prevSt) {
			require.Equal(t, prevSt.docID, currentDocID(),
				"after phase %q: posting for retired docID %d present but the row no longer references it (orphan posting)",
				phase, prevSt.docID)
		}
	}
	defer func() { s.testPutPhaseHook = nil }()

	require.NoError(t, s.PutObject(ctx, crashSafeUpdatedVersion(obj, "UpdateCrashSafeInvariant")))

	require.Equal(t,
		[]string{putPhaseRetireCleanedUp, putPhaseRetireBarrier, putPhaseRowWritten},
		phases, "docID-retiring update phases must run in the crash-safe order")

	newSt := crashSafeReadObjectState(t, s, obj.ID())
	require.NotEqual(t, prevSt.docID, newSt.docID,
		"the updated vector must have forced a docID change")
	require.False(t, crashSafeAnyPostingPresent(t, ctx, s, prevSt),
		"no posting may remain for the retired docID")
	require.True(t, crashSafeAnyPostingPresent(t, ctx, s, newSt),
		"postings must exist for the new docID")
}

// TestUpdateCrashSafe_RetryAfterCrashBeforeRowWrite simulates a crash in the
// window the new ordering leaves open: the old docID's cleanup ran and was
// made durable, but the process died before the row rewrite. The state is
// old-row-without-postings, and retrying the update through the public API
// must converge (row under the new docID, postings only for the new docID)
// with no error — the retire cleanup is idempotent.
func TestUpdateCrashSafe_RetryAfterCrashBeforeRowWrite(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "UpdateCrashSafeRetry")

	obj := crashSafeTestObject("UpdateCrashSafeRetry")
	obj.Vector = []float32{0.1, 0.2, 0.3}
	require.NoError(t, s.PutObject(ctx, obj))
	prevSt := crashSafeReadObjectState(t, s, obj.ID())

	// Run the update only up to (and including) the barrier — the row write
	// never happens, simulating a crash right before it.
	bucket, err := s.objectsBucket()
	require.NoError(t, err)
	className, err := bucket.ClassName()
	require.NoError(t, err)
	prevObj, err := storobj.FromBinaryDisk(prevSt.row, className)
	require.NoError(t, err)
	require.NoError(t, s.retireOldDocIDLocked(ctx, prevObj, prevSt.docID))

	require.True(t, crashSafeRowPresent(t, s, prevSt.idBytes),
		"simulated crash: the old row must still exist")
	require.Empty(t, crashSafePostingsForDocID(t, ctx, s, prevSt),
		"simulated crash: the old docID's postings must already be removed")

	// Retry through the public API: must converge with no error.
	require.NoError(t, s.PutObject(ctx, crashSafeUpdatedVersion(obj, "UpdateCrashSafeRetry")))

	newSt := crashSafeReadObjectState(t, s, obj.ID())
	require.NotEqual(t, prevSt.docID, newSt.docID)
	require.Empty(t, crashSafePostingsForDocID(t, ctx, s, prevSt))
	require.True(t, crashSafeAnyPostingPresent(t, ctx, s, newSt))
}

// TestUpdateCrashSafe_PreservedDocIDSkipsRetire pins the boundary of the new
// ordering: a prop-only update keeps its docID, so the row rewrite retires
// nothing and no retire phases (and no barrier) may run.
func TestUpdateCrashSafe_PreservedDocIDSkipsRetire(t *testing.T) {
	ctx := testCtx()
	s := crashSafeTestShard(t, ctx, "UpdateCrashSafePreserved")

	obj := crashSafeTestObject("UpdateCrashSafePreserved")
	obj.Vector = []float32{0.1, 0.2, 0.3}
	require.NoError(t, s.PutObject(ctx, obj))
	prevSt := crashSafeReadObjectState(t, s, obj.ID())

	var phases []string
	s.testPutPhaseHook = func(phase string) { phases = append(phases, phase) }
	defer func() { s.testPutPhaseHook = nil }()

	// same vector, changed props: docID-preserved update
	updated := crashSafeTestObject("UpdateCrashSafePreserved")
	updated.Object.ID = obj.ID()
	updated.Object.Properties = map[string]interface{}{
		crashSafeTextProp: "echo foxtrot",
		crashSafeIntProp:  float64(9),
	}
	updated.Vector = []float32{0.1, 0.2, 0.3}
	require.NoError(t, s.PutObject(ctx, updated))

	require.Equal(t, []string{putPhaseRowWritten}, phases,
		"a docID-preserving update must not run the retire phases")

	newSt := crashSafeReadObjectState(t, s, obj.ID())
	require.Equal(t, prevSt.docID, newSt.docID)
	require.True(t, crashSafeAnyPostingPresent(t, ctx, s, newSt))
}

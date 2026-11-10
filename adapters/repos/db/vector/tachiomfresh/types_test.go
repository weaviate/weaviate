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

package tachiomfresh

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestTyped runs the tests it lists again with several token types:
// the helpers give the tokens of every document three types by position
// and give hand-built postings one type, so each test checks the same
// behaviour across groups, and checkInvariants adds the type invariants.
func TestTyped(t *testing.T) {
	testTyped = true
	defer func() { testTyped = false }()

	for _, tt := range []struct {
		name string
		fn   func(*testing.T)
	}{
		{"SearchByMultiVectorSelfRecall", TestSearchByMultiVectorSelfRecall},
		{"SearchMatchesBruteForceWhenEverythingIsProbed", TestSearchMatchesBruteForceWhenEverythingIsProbed},
		{"SplitKeepsEveryToken", TestSplitKeepsEveryToken},
		{"ConcurrentInsertAndSearch", TestConcurrentInsertAndSearch},
		{"InvariantsUnderMixedOperations", TestInvariantsUnderMixedOperations},
		{"Restart", TestRestart},
		{"InterruptedBuildIsRetried", TestInterruptedBuildIsRetried},
		{"InterruptedBuildSurvivesRestart", TestInterruptedBuildSurvivesRestart},
		{"BuildFailingOnClosedQueuesIsRebuiltAfterRestart", TestBuildFailingOnClosedQueuesIsRebuiltAfterRestart},
		{"RestoreByPhase", TestRestoreByPhase},
		{"CrashAfterSplitOrMergeKeepsEveryToken", TestCrashAfterSplitOrMergeKeepsEveryToken},
		{"CrashAfterFirstPostingKeepsTheIDSequence", TestCrashAfterFirstPostingKeepsTheIDSequence},
		{"CrashAfterFlushKeepsEveryToken", TestCrashAfterFlushKeepsEveryToken},
		{"SearchAfterDeletesMatchesBruteForce", TestSearchAfterDeletesMatchesBruteForce},
		{"DeletedEntriesAreCleaned", TestDeletedEntriesAreCleaned},
		{"Delete", TestDelete},
		{"SplitCentroidNotExists", TestSplitCentroidNotExists},
		{"SplitPostingThatDoesNotExist", TestSplitPostingThatDoesNotExist},
		{"SplitPostingBelowThreshold", TestSplitPostingBelowThreshold},
		{"SplitSuccessfully", TestSplitSuccessfully},
		{"SplitTaskQueueOperations", TestSplitTaskQueueOperations},
		{"SplitRetriesWhenDocDeletedDuringSplit", TestSplitRetriesWhenDocDeletedDuringSplit},
		{"SplitReassignsNeighbourTokens", TestSplitReassignsNeighbourTokens},
		{"ReassignStoreFailure", TestReassignStoreFailure},
		{"ReinsertSameID", TestReinsertSameID},
		{"DeleteThenReinsert", TestDeleteThenReinsert},
		{"InsertFailingPartWay", TestInsertFailingPartWay},
		{"InsertRacingDelete", TestInsertRacingDelete},
		{"ReassignLocks", TestReassignLocks},
		{"MergeCentroidThatDoesNotExist", TestMergeCentroidThatDoesNotExist},
		{"MergePostingThatDoesNotExist", TestMergePostingThatDoesNotExist},
		{"MergeWithoutCandidate", TestMergeWithoutCandidate},
		{"MergeCandidateTooLarge", TestMergeCandidateTooLarge},
		{"MergeTaskQueueOperations", TestMergeTaskQueueOperations},
		{"MergeSuccessfully", TestMergeSuccessfully},
		{"EmptyPostingLosesItsCentroid", TestEmptyPostingLosesItsCentroid},
		{"Reassign", TestReassign},
		{"AppendToMissingPosting", TestAppendToMissingPosting},
		{"WaitForMaintenanceDrainsQueuedTasks", TestWaitForMaintenanceDrainsQueuedTasks},
		{"WaitForMaintenanceDrainsFollowUpTasks", TestWaitForMaintenanceDrainsFollowUpTasks},
		{"ScanEnqueuesCleanupOverOneThirdDead", TestScanEnqueuesCleanupOverOneThirdDead},
		{"CleanupRemovesDeadEntries", TestCleanupRemovesDeadEntries},
		{"CleanupSkipsMissingPosting", TestCleanupSkipsMissingPosting},
		{"CleanupEnqueuesMergeBelowMinimum", TestCleanupEnqueuesMergeBelowMinimum},
		{"CleanupRetriesWhenPostingBusy", TestCleanupRetriesWhenPostingBusy},
		{"CleanupRacingSplitAndReassign", TestCleanupRacingSplitAndReassign},
	} {
		t.Run(tt.name, tt.fn)
	}
}

// newTypedTestIndex is a typed index on the dot distance with 4-dim tokens.
func newTypedTestIndex(t *testing.T, maxPostingSize uint32) testIndex {
	t.Helper()
	tf := newTestIndex(t, maxPostingSize, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	return tf
}

// typedPosting registers a posting of type typ holding n one-token
// documents (doc ids from startID), all equal to centroid.
func typedPosting(t *testing.T, tf *testIndex, typ uint32, centroid []float32, n int, startID uint64) (uint64, []tokenRef) {
	t.Helper()
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = centroid
	}
	id, refs := createPostingWithTokens(t, tf, vectors, startID)
	registerTypedPosting(t, tf, id, refs, centroid, typ)
	return id, refs
}

var (
	e1     = []float32{1, 0, 0, 0}
	e2     = []float32{0, 1, 0, 0}
	nearE2 = []float32{0.1, 0.995, 0, 0}
)

// A token goes to the nearest centroid of its type, even when a centroid of
// another type is nearer.
func TestTypedInsertRoutesWithinType(t *testing.T) {
	tf := newTypedTestIndex(t, 64)
	ownID, _ := typedPosting(t, &tf, 1, e1, 6, 100)
	otherID, _ := typedPosting(t, &tf, 2, e2, 6, 200)

	tf.mvStore.storeMultiVector(1000, [][]float32{nearE2})
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 1000, [][]float32{nearE2}, []uint32{1}))

	ref := tokenRef{DocID: 1000}
	require.Contains(t, tf.posting(ownID), ref)
	require.NotContains(t, tf.posting(otherID), ref)
}

// The first token of a type becomes the type's first centroid, and
// the second token of the type joins it.
func TestTypedFirstTokenOfATypeCreatesItsPosting(t *testing.T) {
	tf := newTypedTestIndex(t, 64)
	otherID, _ := typedPosting(t, &tf, 2, e2, 6, 200)

	doc := [][]float32{nearE2, e2}
	tf.mvStore.storeMultiVector(1000, doc)
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 1000, doc, []uint32{5, 5}))

	require.Len(t, tf.posting(otherID), 6)
	require.Equal(t, 1, tf.Index.types.groupSize(5))
	newID := tf.Index.types.group(5).ids[0]
	require.Equal(t, uint32(5), tf.Index.types.typeOf(newID))
	require.True(t, tf.Index.Centroids.Exists(newID))
	require.ElementsMatch(t, []tokenRef{{DocID: 1000, Pos: 0}, {DocID: 1000, Pos: 1}}, tf.posting(newID))
}

// A posting alone in its type is never merged, however small, unless it is
// empty: a search that probes it enqueues no merge, and a merge asked for by
// hand leaves it, next to a small posting of another type.
func TestTypedLonePostingIsNotMerged(t *testing.T) {
	tf := newTypedTestIndex(t, 64)
	loneID, loneRefs := typedPosting(t, &tf, 1, e1, 3, 100)
	otherID, otherRefs := typedPosting(t, &tf, 2, nearE2, 3, 200)

	// 3 < minPostingSize (21 at maxPostingSize 64)
	_, _, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{e1}, 1, nil)
	require.NoError(t, err)
	require.False(t, tf.Index.taskQueue.MergeContains(loneID))
	require.False(t, tf.Index.taskQueue.MergeContains(otherID))

	require.NoError(t, tf.Index.doMerge(t.Context(), loneID))
	tf.Index.waitForMaintenance(t)
	require.ElementsMatch(t, loneRefs, tf.posting(loneID))
	require.ElementsMatch(t, otherRefs, tf.posting(otherID))
	require.True(t, tf.Index.Centroids.Exists(loneID))
}

// A merge joins postings of one type only: the nearest posting of another
// type is not a candidate, a farther one of the same type is.
func TestTypedMergeStaysWithinType(t *testing.T) {
	tf := newTypedTestIndex(t, 64)
	tf.Index.opts.disableReassign = true
	smallID, smallRefs := typedPosting(t, &tf, 1, e1, 3, 100)
	nearOtherID, nearOtherRefs := typedPosting(t, &tf, 2, []float32{0.995, 0.1, 0, 0}, 6, 200)
	farSameID, farSameRefs := typedPosting(t, &tf, 1, e2, 6, 300)

	// a search that probes the small posting enqueues its merge: it is not
	// alone in its type
	_, _, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{e1}, 1, nil)
	require.NoError(t, err)
	tf.Index.waitForMaintenance(t)

	require.False(t, tf.Index.Centroids.Exists(smallID))
	require.ElementsMatch(t, append(farSameRefs, smallRefs...), tf.posting(farSameID))
	require.ElementsMatch(t, nearOtherRefs, tf.posting(nearOtherID))
	require.Equal(t, 1, tf.Index.types.groupSize(1))
	require.Equal(t, 1, tf.Index.types.groupSize(2))
}

// A reassigned token moves to the nearest posting of its type, the type of
// the posting it was in, not to a nearer posting of another type.
func TestTypedReassignStaysWithinType(t *testing.T) {
	tf := newTypedTestIndex(t, 64)
	aID, _ := typedPosting(t, &tf, 1, e1, 6, 100)
	sameID, _ := typedPosting(t, &tf, 1, []float32{0, 0.8, 0.6, 0}, 6, 200)
	otherID, _ := typedPosting(t, &tf, 2, e2, 6, 300)
	ref := placeToken(t, &tf, 1000, nearE2, aID)

	require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))

	require.Contains(t, tf.posting(sameID), ref)
	require.NotContains(t, tf.posting(aID), ref)
	require.NotContains(t, tf.posting(otherID), ref)
}

// Both postings of a split inherit the type of the split posting and
// replace it in the type's group.
func TestTypedSplitChildrenInheritType(t *testing.T) {
	tf := newTypedTestIndex(t, 4)
	vectors := [][]float32{{1, 0, 0, 0}, {0.99, 0.1, 0, 0}, {0.98, 0.2, 0, 0}, {0, 1, 0, 0}, {0.1, 0.99, 0, 0}, {0.2, 0.98, 0, 0}}
	id, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerTypedPosting(t, &tf, id, refs, meanVector(vectors), 3)
	_, _ = typedPosting(t, &tf, 9, e1, 2, 500)

	require.NoError(t, tf.Index.doSplit(t.Context(), id, false))

	require.False(t, tf.Index.Centroids.Exists(id))
	require.Equal(t, 2, tf.Index.types.groupSize(3))
	require.Equal(t, 1, tf.Index.types.groupSize(9))
	var got []tokenRef
	for _, child := range tf.Index.types.group(3).ids {
		require.NotEqual(t, id, child)
		require.Equal(t, uint32(3), tf.Index.types.typeOf(child))
		got = append(got, tf.posting(child)...)
	}
	require.ElementsMatch(t, refs, got)
}

// The type groups survive a restart, and a document without one token id
// per token is refused.
func TestTokenTypesArePersistedAndRequired(t *testing.T) {
	doc := [][]float32{e1, e2}

	t.Run("the groups come back after a restart", func(t *testing.T) {
		dir := t.TempDir()
		open := func() testIndex {
			return newTestIndexAt(t, dir, 64, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
		}
		add := func(tf *testIndex, id uint64) error {
			tf.mvStore.storeMultiVector(id, doc)
			return tf.Index.AddMultiWithTypes(t.Context(), id, doc, []uint32{4, 5})
		}

		tf := open()
		require.NoError(t, add(&tf, 0))
		tf.Index.waitForMaintenance(t)
		tf.close(t)

		reopened := open()
		require.NoError(t, add(&reopened, 2))
		// the new tokens joined the postings of their types instead of
		// opening new ones
		require.Equal(t, 1, reopened.Index.types.groupSize(4))
		require.Equal(t, 1, reopened.Index.types.groupSize(5))
		require.Len(t, reopened.posting(reopened.Index.types.group(4).ids[0]), 2)
		reopened.close(t)
	})

	t.Run("a document without token ids is refused", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		tf.mvStore.storeMultiVector(0, doc)
		tf.Index.tokenIDsForID = func(context.Context, uint64) ([]uint32, error) { return nil, nil }
		require.ErrorContains(t, tf.Index.AddMulti(t.Context(), 0, doc), "has no token ids")
		tf.Index.tokenIDsForID = nil
		require.ErrorContains(t, tf.Index.AddMulti(t.Context(), 0, doc), "no reader for them")
		tf.Index.tokenIDsForID = func(context.Context, uint64) ([]uint32, error) { return []uint32{4}, nil }
		require.ErrorContains(t, tf.Index.AddMulti(t.Context(), 0, doc), "2 tokens but 1 token ids")
		require.False(t, tf.Index.ContainsDoc(0))
	})

	t.Run("types and tokens must match in number", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		tf.mvStore.storeMultiVector(0, doc)
		require.Error(t, tf.Index.AddMultiWithTypes(t.Context(), 0, doc, []uint32{4}))
		require.Error(t, tf.Index.AddMultiWithTypes(t.Context(), 0, doc, nil))
	})
}

// The posting limits follow the size of the posting's type: the configured
// pair under spreadMinEntries entries, the square root of the size above
// while the spread is unknown, with hfresh's ratio for the minimum. A
// posting id with no type recorded gets the configured pair.
func TestTypedPostingLimits(t *testing.T) {
	const maxPostingSize = 4
	for _, tt := range []struct {
		name             string
		entries          int
		wantMin, wantMax uint32
	}{
		{"small type", 10, 3, 4},
		{"at the floor", 16, 3, 4},
		{"past the floor's square, under the spread minimum", 100, 3, 4},
		{"just under the spread minimum", 255, 3, 4},
		{"at the spread minimum", 256, 5, 16},
		{"large type", 400, 6, 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTypedTestIndex(t, maxPostingSize)
			id, _ := typedPosting(t, &tf, 3, e1, tt.entries, 100)
			// another type does not count
			otherID, _ := typedPosting(t, &tf, 9, e2, 2, 5000)

			minSize, maxSize := tf.Index.postingLimits(id)
			require.Equal(t, tt.wantMin, minSize)
			require.Equal(t, tt.wantMax, maxSize)

			minSize, maxSize = tf.Index.postingLimits(otherID)
			require.Equal(t, uint32(3), minSize)
			require.Equal(t, uint32(maxPostingSize), maxSize)
		})
	}

	t.Run("a posting id with no type recorded", func(t *testing.T) {
		tf := newTestIndex(t, maxPostingSize, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
		require.NoError(t, tf.Index.initDimensions(e1))
		vectors := make([][]float32, 400)
		for i := range vectors {
			vectors[i] = e1
		}
		id, refs := createPostingWithTokens(t, &tf, vectors, 100)
		require.NoError(t, tf.Index.putPosting(t.Context(), id, refs))
		require.NoError(t, tf.Index.Centroids.Insert(id, &hfresh.Centroid{Uncompressed: e1}))

		minSize, maxSize := tf.Index.postingLimits(id)
		require.Equal(t, uint32(3), minSize)
		require.Equal(t, uint32(maxPostingSize), maxSize)
	})
}

// largeAndSmallType inserts large one-token documents of type 1 (doc ids
// from 0) and small ones of type 2 (doc ids from large), with the queues
// drained at the end.
func largeAndSmallType(t *testing.T, tf *testIndex, rng *rand.Rand, large, small int) map[uint64][][]float32 {
	t.Helper()
	docs := make(map[uint64][][]float32)
	for i := range large + small {
		typ := uint32(1)
		if i >= large {
			typ = 2
		}
		doc := randomMultiVector(rng, 1, 16)
		docs[uint64(i)] = doc
		tf.mvStore.storeMultiVector(uint64(i), doc)
		require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), uint64(i), doc, []uint32{typ}))
	}
	tf.Index.waitForMaintenance(t)
	return docs
}

// A type that outgrows maxPostingSize^2 entries gets postings larger than
// maxPostingSize, so fewer centroids than its size over maxPostingSize, while
// a small type in the same index keeps the configured limit.
func TestTypedLargeTypeGetsLargerPostings(t *testing.T) {
	const (
		maxPostingSize = 4
		large          = 900
		small          = 12
	)
	tf := newTestIndex(t, maxPostingSize, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	largeAndSmallType(t, &tf, rand.New(rand.NewSource(1)), large, small)

	g := tf.Index.types.group(1)
	require.Equal(t, int64(large), g.entries.Load())
	require.Equal(t, int64(small), tf.Index.types.group(2).entries.Load())

	// with one limit for all, 900 entries in postings of at most 4 would be
	// at least 225 centroids
	require.Less(t, len(g.ids), large/maxPostingSize/2)
	var longest int
	for _, id := range g.ids {
		longest = max(longest, len(tf.posting(id)))
	}
	require.Greater(t, longest, maxPostingSize)

	// sqrt(900) = 30: the type alone sets the reference, so its relative
	// spread is 1
	_, maxSize := tf.Index.postingLimits(g.ids[0])
	require.GreaterOrEqual(t, maxSize, uint32(30))
	_, maxSize = tf.Index.postingLimits(tf.Index.types.group(2).ids[0])
	require.Equal(t, uint32(maxPostingSize), maxSize)
}

// After most documents of a large type are deleted, searches enqueue the
// merges that collect their entries: the type's size falls, and its centroid
// count with it, as in hfresh.
func TestTypedCentroidsDecreaseAfterDeletes(t *testing.T) {
	// large enough to stay above spreadMinEntries after the deletes: under
	// it the type returns to the configured limits
	const large = 3000
	tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	rng := rand.New(rand.NewSource(2))
	docs := largeAndSmallType(t, &tf, rng, large, 12)

	g := tf.Index.types.group(1)
	centroids := len(g.ids)

	for id := range uint64(large * 8 / 10) {
		require.NoError(t, tf.Index.DeleteMulti(id))
		tf.mvStore.deleteMultiVector(id)
	}
	// a delete only marks the document: the type's size is unchanged
	require.Equal(t, int64(large), g.entries.Load())

	for range 5 {
		for id := range uint64(large) {
			_, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 5, nil)
			require.NoError(t, err)
		}
		tf.Index.waitForMaintenance(t)
	}

	require.Less(t, g.entries.Load(), int64(large/2))
	require.Less(t, tf.Index.types.groupSize(1), centroids)
}

// observeAll feeds vectors to the spread sums of type typ, n times over.
func observeAll(tf *testIndex, typ uint32, vectors [][]float32, n int) {
	g := tf.Index.types.group(typ)
	for range n {
		for _, v := range vectors {
			g.observe(v)
		}
	}
}

// From spreadMinEntries entries on, the split threshold of a type is the
// square root of its size over its spread relative to the mean spread of
// the large types: a tight type gets larger postings than a
// spread-out one of the same size, a type of average spread the square root
// itself, and the ratio is clamped to [minRelativeSpread,
// maxRelativeSpread], so that a type more spread out than the
// mean gets the square root and no less. The spread is a variance:
// translating every vector leaves it as it is.
func TestTypedPostingLimitsFollowSpread(t *testing.T) {
	const maxPostingSize = 4
	e3 := []float32{0, 0, 1, 0}
	e4 := []float32{0, 0, 0, 1}
	shifted := func(v []float32, by float32) []float32 {
		out := make([]float32, len(v))
		for i := range v {
			out[i] = v[i] + by
		}
		return out
	}
	// two types of 400 entries each, sqrt 20
	for _, tt := range []struct {
		name               string
		vectors3, vectors5 [][]float32
		wantMin3, wantMax3 uint32
		wantMin5, wantMax5 uint32
	}{
		// equal variances, the mean is each type's own: s = 1, 20 / 1
		{"same spread", [][]float32{e1, e2}, [][]float32{e3, e4}, 6, 20, 6, 20},
		// variances 0 and 0.5, mean 0.25: s 0 clamped to 0.05, 20 / 0.05;
		// s 2 capped to maxRelativeSpread 1, 20 / 1
		{"tight and spread out", [][]float32{e1}, [][]float32{e1, e2}, 133, 400, 6, 20},
		// variances 0.5 and 0.75, mean 0.625: s 0.8, 20 / 0.8; s 1.2 capped
		// to 1, 20 / 1
		{"two and four directions", [][]float32{e1, e2}, [][]float32{e1, e2, e3, e4}, 8, 25, 6, 20},
		// the same vectors translated by 3 on every coordinate: the
		// variances do not move
		{"translated", [][]float32{shifted(e1, 3)}, [][]float32{shifted(e1, 3), shifted(e2, 3)}, 133, 400, 6, 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTypedTestIndex(t, maxPostingSize)
			id3, _ := typedPosting(t, &tf, 3, e1, 400, 100)
			id5, _ := typedPosting(t, &tf, 5, e1, 400, 5000)
			observeAll(&tf, 3, tt.vectors3, spreadRefresh)
			observeAll(&tf, 5, tt.vectors5, spreadRefresh)

			minSize, maxSize := tf.Index.postingLimits(id3)
			require.Equal(t, tt.wantMin3, minSize)
			require.Equal(t, tt.wantMax3, maxSize)
			minSize, maxSize = tf.Index.postingLimits(id5)
			require.Equal(t, tt.wantMin5, minSize)
			require.Equal(t, tt.wantMax5, maxSize)
		})
	}

	t.Run("under spreadMinEntries the spread is not used", func(t *testing.T) {
		tf := newTypedTestIndex(t, maxPostingSize)
		id, _ := typedPosting(t, &tf, 3, e1, 100, 100)
		typedPosting(t, &tf, 5, e1, 400, 5000)
		observeAll(&tf, 5, [][]float32{e1, e2}, spreadRefresh)
		// 100 entries: no sums were kept either (spreadObserveEntries)
		minSize, maxSize := tf.Index.postingLimits(id)
		require.Equal(t, uint32(3), minSize)
		require.Equal(t, uint32(4), maxSize)
	})

	t.Run("a type far more spread out than the rest is capped", func(t *testing.T) {
		tf := newTypedTestIndex(t, maxPostingSize)
		for typ := uint32(10); typ < 16; typ++ {
			typedPosting(t, &tf, typ, e1, 256, uint64(typ)*1000)
			observeAll(&tf, typ, [][]float32{e1}, spreadRefresh)
		}
		id, _ := typedPosting(t, &tf, 3, e1, 256, 100)
		tf.Index.types.group(3).entries.Store(10000)
		observeAll(&tf, 3, [][]float32{e1, e2}, spreadRefresh)
		// variances 0 six times and 0.5, mean 0.5 / 7: s 7, capped to
		// maxRelativeSpread 1: sqrt(10000) / 1
		_, maxSize := tf.Index.postingLimits(id)
		require.Equal(t, uint32(100), maxSize)
	})
}

// The running variance equals TACHIOM's spread measure computed directly:
// the sum of the per-dimension variances, whatever the scale of the vectors,
// and a translation of every vector leaves it as it is.
func TestTypedVarianceMatchesDirectComputation(t *testing.T) {
	for _, tt := range []struct {
		name  string
		scale float32
		shift float32
	}{
		{"unit scale", 1, 0},
		{"another scale", 7.5, 0},
		{"translated", 1, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const dim, n = 16, 500
			rng := rand.New(rand.NewSource(3))
			vectors := make([][]float32, n)
			for i := range vectors {
				vectors[i] = randomMultiVector(rng, 1, dim)[0]
				for j := range vectors[i] {
					vectors[i][j] = (vectors[i][j]-0.3)*tt.scale + tt.shift
				}
			}

			g := &typeGroup{}
			g.entries.Store(spreadObserveEntries)
			for _, v := range vectors {
				g.observe(v)
			}
			got, ok := g.variance()
			require.True(t, ok)

			var variance float64
			for j := range dim {
				var mean float64
				for _, v := range vectors {
					mean += float64(v[j])
				}
				mean /= n
				for _, v := range vectors {
					d := float64(v[j]) - mean
					variance += d * d / n
				}
			}
			require.InDelta(t, variance, got, 1e-9)
			if tt.shift != 0 {
				// the untranslated vectors give the same variance
				h := &typeGroup{}
				h.entries.Store(spreadObserveEntries)
				for _, v := range vectors {
					back := make([]float32, dim)
					for j := range back {
						back[j] = v[j] - tt.shift
					}
					h.observe(back)
				}
				base, ok := h.variance()
				require.True(t, ok)
				require.InDelta(t, base, got, 1e-6)
			}
		})
	}

	t.Run("nothing observed under spreadObserveEntries", func(t *testing.T) {
		g := &typeGroup{}
		g.observe(e1)
		_, ok := g.variance()
		require.False(t, ok)
	})
}

// Of two types of the same size, the one whose tokens are close together
// ends with fewer centroids than the one whose tokens are spread out.
func TestTypedTightTypeGetsFewerCentroids(t *testing.T) {
	const perType = 1500
	tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	rng := rand.New(rand.NewSource(5))
	for i := range 2 * perType {
		typ := uint32(1 + i%2)
		var vec []float32
		if typ == 1 {
			// tight: one direction with a little noise
			vec = []float32{1, 0.05 * rng.Float32(), 0.05 * rng.Float32(), 0.05 * rng.Float32()}
		} else {
			// spread out: any direction
			vec = []float32{rng.Float32() - 0.5, rng.Float32() - 0.5, rng.Float32() - 0.5, rng.Float32() - 0.5}
		}
		doc := [][]float32{vec}
		tf.mvStore.storeMultiVector(uint64(i), doc)
		require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), uint64(i), doc, []uint32{typ}))
	}
	tf.Index.waitForMaintenance(t)

	_, tight := tf.Index.postingLimits(tf.Index.types.group(1).ids[0])
	_, loose := tf.Index.postingLimits(tf.Index.types.group(2).ids[0])
	require.Greater(t, tight, loose)
	require.Less(t, tf.Index.types.groupSize(1), tf.Index.types.groupSize(2))
}

// The spread sums are persisted on shutdown and come back on open, so a
// reopened index keeps the posting limits it had.
func TestTypedSpreadIsPersisted(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, 4, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open()
	largeAndSmallType(t, &tf, rand.New(rand.NewSource(4)), 900, 12)
	g := tf.Index.types.group(1)
	g.spreadMu.Lock()
	seen, sumSq, sum := g.seen, g.sumSq, append([]float64(nil), g.sum...)
	g.spreadMu.Unlock()
	require.NotZero(t, seen)
	minSize, maxSize := tf.Index.postingLimits(g.ids[0])
	tf.close(t)

	reopened := open()
	g = reopened.Index.types.group(1)
	require.Equal(t, seen, g.seen)
	require.Equal(t, sumSq, g.sumSq)
	require.Equal(t, sum, g.sum)
	gotMin, gotMax := reopened.Index.postingLimits(g.ids[0])
	// the variance and the reference are refreshed from all the sums on
	// open, during the run every spreadRefresh tokens: the limits may
	// differ by that much
	require.InDelta(t, minSize, gotMin, 2)
	require.InDelta(t, maxSize, gotMax, 6)
	// the small type never reached spreadObserveEntries: it has no sums
	require.Nil(t, reopened.Index.types.group(2).sum)
	reopened.close(t)
}

// A backup carries the spread sums of tokens inserted after the last task
// batch. The steps are the shard's: pause compaction, the index's
// PrepareForBackup, flush the memtables, copy the files.
func TestTypedSpreadIsInBackup(t *testing.T) {
	dir := t.TempDir()
	open := func(dir string) testIndex {
		return newTestIndexAt(t, dir, 4, 8, 8, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open(dir)
	rng := rand.New(rand.NewSource(5))
	largeAndSmallType(t, &tf, rng, 900, 12)

	doc := randomMultiVector(rng, 1, 16)
	tf.mvStore.storeMultiVector(5000, doc)
	require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 5000, doc, []uint32{1}))

	g := tf.Index.types.group(1)
	g.spreadMu.Lock()
	seen, sumSq, sum := g.seen, g.sumSq, append([]float64(nil), g.sum...)
	g.spreadMu.Unlock()
	require.NotZero(t, seen)

	ctx := t.Context()
	require.NoError(t, tf.store.PauseCompaction(ctx))
	require.NoError(t, tf.Index.PrepareForBackup(ctx))
	require.NoError(t, tf.store.FlushMemtables(ctx))
	restored := t.TempDir()
	copyDir(t, dir, restored)
	require.NoError(t, tf.Index.ResumeAfterBackup(ctx))
	require.NoError(t, tf.store.ResumeCompaction(ctx))
	tf.close(t)

	reopened := open(restored)
	g = reopened.Index.types.group(1)
	require.Equal(t, seen, g.seen)
	require.Equal(t, sumSq, g.sumSq)
	require.Equal(t, sum, g.sum)
	reopened.close(t)
}

// A build that starts over forgets the spread sums, in memory and on disk: its
// tokens are observed again.
func TestResetGroupsForgetsTheSpreadSums(t *testing.T) {
	tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	largeAndSmallType(t, &tf, rand.New(rand.NewSource(6)), 900, 12)
	require.NotZero(t, tf.Index.types.group(1).seen)
	require.NoError(t, tf.Index.types.persistSpread())

	require.NoError(t, tf.Index.types.resetGroups())
	require.Nil(t, tf.Index.types.group(1))
	require.Zero(t, tf.Index.types.reference.Load())

	bucket, release, err := tf.Index.types.acquire()
	require.NoError(t, err)
	defer release()
	v, err := bucket.Get(spreadKey(1))
	require.NoError(t, err)
	require.Empty(t, v)
}

// TestResetGroupsWinsOverAConcurrentPersistSpread: a Flush's persistSpread
// that took the groups before a retried build reset them must not write
// their sums back after the reset deleted them.
func TestResetGroupsWinsOverAConcurrentPersistSpread(t *testing.T) {
	tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction)
	defer tf.close(t)
	vectors := createTestVectors(4, 3)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))
	g := tf.Index.types.group(testPostingType)
	g.spreadMu.Lock()
	g.sum, g.sumSq, g.seen, g.dirty = []float64{1, 2, 3, 4}, 30, 5, true
	g.spreadMu.Unlock()

	snapped, release := make(chan struct{}), make(chan struct{})
	// cleared before the deferred close, whose Flush calls persistSpread
	defer func() { tf.Index.types.afterSpreadSnapshot = nil }()
	tf.Index.types.afterSpreadSnapshot = func() {
		close(snapped)
		<-release
	}
	persisted := make(chan error, 1)
	enterrors.GoWrapper(func() { persisted <- tf.Index.types.persistSpread() }, tf.Index.logger)
	<-snapped
	reset := make(chan error, 1)
	enterrors.GoWrapper(func() { reset <- tf.Index.types.resetGroups() }, tf.Index.logger)
	// the reset either waits for the persist or, without the exclusion,
	// deletes the keys first
	select {
	case err := <-reset:
		require.NoError(t, err)
		close(release)
	case <-time.After(200 * time.Millisecond):
		close(release)
		require.NoError(t, <-reset)
	}
	require.NoError(t, <-persisted)
	tf.Index.types.afterSpreadSnapshot = nil

	bucket, done, err := tf.Index.types.acquire()
	require.NoError(t, err)
	defer done()
	val, err := bucket.Get(spreadKey(testPostingType))
	require.NoError(t, err)
	require.Nil(t, val, "the reset's delete was undone by the persist")
}

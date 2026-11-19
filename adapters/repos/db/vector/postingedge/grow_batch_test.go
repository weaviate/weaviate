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

package postingedge

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The inserts in batches of v5.2 (bench-report.md, "inserts con búsquedas en
// paralelo"): the searches of a batch run at once over the index as the batch
// found it, and then the writes of every insert are applied in order.

// A batch of one vector is the insert of always: next to the twin in memory
// at every insert, and with everything written.
func TestGrowBatchOfOneEqualsMemory(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		consolidate int
		cosine      bool
		bits        int
	}{
		{name: "edges value by value", bits: 64},
		{name: "edges value by value, cosine, the full code", cosine: true},
		{name: "consolidated edges", consolidate: 5, bits: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 1500, 700, 48
			f := newGrowFixtureWith(t, total, base, dims, tt.cosine, tt.bits, 60, tt.consolidate)
			f.batchOfOne = true
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				if (f.inserted-base)%250 == 0 {
					f.same(t, "on the way")
				}
			}
			require.Positive(t, f.g.stats.Splits)
			require.Equal(t, total-base, f.g.stats.Batches)
			require.Equal(t, total-base, f.g.stats.BatchInserts)
			require.Zero(t, f.g.stats.PlansReassign, "a batch of one sees everything before it")
			require.Zero(t, f.g.stats.StaleLists)
			require.NoError(t, f.g.flushPending())
			f.same(t, "with everything written")
			f.reopen(t)
			f.same(t, "opened again")
		})
	}
}

// growPlanCopy is what a plan holds, apart from the buffers it is used again
// with.
type growPlanCopy struct {
	entry     []uint64
	neighbors []uint64
	dists     []float32
	kept      map[uint32]growKept
	captured  map[uint64][]float32
	loads     int
	rounds    int
}

func copyGrowPlan(plan *growPlan) growPlanCopy {
	out := growPlanCopy{
		entry: slices.Clone(plan.entry), neighbors: slices.Clone(plan.neighbors), dists: slices.Clone(plan.dists),
		kept: make(map[uint32]growKept, len(plan.keptAt)), captured: make(map[uint64][]float32, len(plan.captured)),
		loads: plan.searched.Postings, rounds: plan.searched.Rounds,
	}
	for q, at := range plan.keptAt {
		kept := plan.kept[at]
		out.kept[q] = growKept{
			docIDs: slices.Clone(kept.docIDs), edgeStart: slices.Clone(kept.edgeStart), edges: slices.Clone(kept.edges),
		}
	}
	for docID, vector := range plan.captured {
		out.captured[docID] = slices.Clone(vector)
	}
	return out
}

// What the searches of a batch leave does not depend on how many goroutines
// run them: every plan is the search of its vector over the index as the
// batch found it. And a search writes nothing: the index is, after them, as
// it was.
func TestGrowBatchPlansDoNotDependOnSearchers(t *testing.T) {
	ctx := context.Background()
	for _, consolidate := range []int{0, 6} {
		name := "edges value by value"
		if consolidate > 0 {
			name = "consolidated edges"
		}
		t.Run(name, func(t *testing.T) {
			const total, base, dims, batch = 1400, 700, 48, 40
			f := newGrowFixtureWith(t, total, base, dims, false, 64, 60, consolidate)
			g := f.g
			newPlans := func() []*growPlan {
				plans := make([]*growPlan, batch)
				for i := range plans {
					plans[i] = newGrowPlan()
				}
				return plans
			}
			for from := base; from < total; from += batch {
				to := min(from+batch, total)
				docIDs := sequentialIDs(to-from, uint64(from))
				vectors := f.all[from:to]
				statsBefore, pendingBefore := g.stats, g.pendingMembers
				var want []growPlanCopy
				for _, searchers := range []int{1, 3, 8} {
					plans := newPlans()[:len(docIDs)]
					require.NoError(t, g.planBatch(ctx, plans, docIDs, vectors, searchers))
					got := make([]growPlanCopy, len(plans))
					for i, plan := range plans {
						got[i] = copyGrowPlan(plan)
						require.Equal(t, docIDs[i], plan.docID)
						require.Len(t, plan.neighbors, min(g.meta.Neighbors, len(plan.neighbors)))
						require.NotEmpty(t, plan.neighbors)
						require.Equal(t, len(plan.captured), plan.vectorReads, "one exact vector kept per vector read")
					}
					if want == nil {
						want = got
						continue
					}
					require.Equal(t, want, got, "%d searchers", searchers)
				}
				// Nothing was written: the counters of the inserts and what
				// waits are where they were.
				require.Equal(t, statsBefore, g.stats)
				require.Equal(t, pendingBefore, g.pendingMembers)
				require.NoError(t, g.insertBatch(ctx, docIDs, vectors, 4))
			}
			require.NoError(t, g.checkCoverage())
			require.Equal(t, total, g.meta.Live)
		})
	}
}

// Inserts in batches leave a whole index: every vector in one posting, the
// map and the rows telling the same, and every member with its list of
// edges. Every list an insert takes from what its plan kept is the one the
// index has. With batches as large as these and a threshold as low, plans
// meet both things a batch can do to them: their posting split before they
// were applied, and lists they kept changed.
func TestGrowBatchCoverage(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		batch       int
		searchers   int
		consolidate int
		cosine      bool
		bits        int
		threshold   int
		// flushEvery is in batches, 0 for never, and reopenAt closes the
		// index after that many inserts and opens it again.
		flushEvery int
		reopenAt   int
		// meets tells that the batches are large enough for a plan to find
		// its posting split and lists it kept changed.
		meets bool
	}{
		{name: "batches of two", batch: 2, searchers: 2, bits: 64, threshold: 60, flushEvery: 40},
		{name: "batches of seven, four searchers", batch: 7, searchers: 4, bits: 64, threshold: 60, flushEvery: 11, reopenAt: 406},
		{name: "batches of thirty-two, eight searchers", batch: 32, searchers: 8, bits: 64, threshold: 60, flushEvery: 3, meets: true},
		{name: "batches of thirty-two, cosine, the full code", batch: 32, searchers: 8, cosine: true, threshold: 60, meets: true},
		{name: "batches of a hundred", batch: 100, searchers: 8, bits: 64, threshold: 60, flushEvery: 2, reopenAt: 500, meets: true},
		{name: "more searchers than vectors", batch: 3, searchers: 16, bits: 64, threshold: 60},
		{name: "consolidated edges, batches of seven", batch: 7, searchers: 4, consolidate: 5, bits: 64, threshold: 60, flushEvery: 9, reopenAt: 406},
		{name: "consolidated edges, batches of thirty-two", batch: 32, searchers: 8, consolidate: 16, bits: 64, threshold: 60, flushEvery: 4, meets: true},
		{name: "consolidated edges, a row at every change", batch: 32, searchers: 8, consolidate: 1, bits: 64, threshold: 60, meets: true},
		{name: "consolidated edges, batches of a hundred, cosine", batch: 100, searchers: 8, consolidate: 8, cosine: true, bits: 128, threshold: 60, reopenAt: 500, meets: true},
		{name: "few splits", batch: 32, searchers: 8, bits: 64, threshold: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 2200, 1000, 64
			f := newGrowFixtureWith(t, total, base, dims, tt.cosine, tt.bits, tt.threshold, tt.consolidate)
			f.g.batchVerify = true
			var stats growStats
			verified, batches := 0, 0
			// keep adds up what the index counted before it is closed.
			keep := func() {
				stats.Splits += f.g.stats.Splits
				stats.Batches += f.g.stats.Batches
				stats.BatchInserts += f.g.stats.BatchInserts
				stats.PlansReassign += f.g.stats.PlansReassign
				stats.PlansStale += f.g.stats.PlansStale
				stats.StaleLists += f.g.stats.StaleLists
				stats.InverseEdges += f.g.stats.InverseEdges
				stats.InverseListReads += f.g.stats.InverseListReads
				verified += f.g.batchVerified
			}
			for from := base; from < total; from += tt.batch {
				to := min(from+tt.batch, total)
				docIDs := sequentialIDs(to-from, uint64(from))
				require.NoError(t, f.g.insertBatch(ctx, docIDs, f.all[from:to], tt.searchers), "docs %d to %d", from, to)
				require.Equal(t, docIDs, f.g.batchApplied, "the writes of the batch, in the order of its inserts")
				batches++
				require.Nil(t, f.g.batchDirty, "no batch is being applied")
				if tt.flushEvery > 0 && batches%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
				}
				if batches%5 == 0 {
					require.NoError(t, f.g.checkCoverage(), "after doc %d", to-1)
				}
				if tt.reopenAt > 0 && from-base < tt.reopenAt && to-base >= tt.reopenAt {
					keep()
					require.NoError(t, f.g.close(ctx))
					f.g = nil
					g, err := openGrowIndex(ctx, testLogger(), f.dir, f.provider, f.uc, growOpen{})
					require.NoError(t, err)
					g.batchVerify = true
					f.g = g
					require.NoError(t, f.g.checkCoverage(), "opened again")
				}
			}
			keep()
			require.Equal(t, total, f.g.meta.Live)
			require.NoError(t, f.g.checkCoverage(), "at the end")
			require.Equal(t, batches, stats.Batches)
			require.Equal(t, total-base, stats.BatchInserts)
			require.Positive(t, stats.InverseEdges)
			require.Positive(t, verified, "lists taken from the plans were checked against the index")
			if tt.threshold < 100 {
				require.Positive(t, stats.Splits)
			}
			if tt.meets {
				assert.Positive(t, stats.PlansReassign, "a plan whose posting an insert of its batch split")
				assert.Positive(t, stats.StaleLists, "lists a plan kept and an insert of its batch changed")
				assert.Positive(t, stats.PlansStale)
			}

			// With everything written and opened again, it is the same
			// index, and it finds what it holds.
			require.NoError(t, f.g.flushPending())
			require.NoError(t, f.g.checkCoverage(), "with everything written")
			require.NoError(t, f.g.close(ctx))
			f.g = nil
			g, err := openGrowIndex(ctx, testLogger(), f.dir, f.provider, f.uc, growOpen{})
			require.NoError(t, err)
			f.g = g
			require.NoError(t, g.checkCoverage(), "with everything written, opened again")
			found := 0
			probes := sequentialIDs(60, uint64(total-60))
			for _, docID := range probes {
				ids, _, err := g.search(ctx, f.all[docID], 10, nil)
				require.NoError(t, err)
				if slices.Contains(ids, docID) {
					found++
				}
			}
			require.GreaterOrEqual(t, found, len(probes)-3, "the last vectors inserted find themselves")
		})
	}
}

// A batch is refused whole, before any of it is searched, when it cannot be
// inserted.
func TestGrowBatchErrors(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 900, 700, 32
	f := newGrowFixtureWith(t, total, base, dims, false, 64, 60, 0)
	g := f.g
	next := uint64(base)
	tests := []struct {
		name      string
		docIDs    []uint64
		vectors   [][]float32
		searchers int
		wantErr   string
	}{
		{name: "more doc ids than vectors", docIDs: []uint64{next, next + 1}, vectors: f.all[base : base+1], searchers: 2, wantErr: "2 doc ids for 1 vectors"},
		{name: "no searcher", docIDs: []uint64{next}, vectors: f.all[base : base+1], wantErr: "0 searchers"},
		{name: "a doc twice", docIDs: []uint64{next, next + 1, next}, vectors: f.all[base : base+3], searchers: 2, wantErr: "is in the batch twice"},
		{name: "a doc the index holds", docIDs: []uint64{next, 5}, vectors: f.all[base : base+2], searchers: 2, wantErr: "doc 5 is in the index already"},
		{name: "a doc past the keys", docIDs: []uint64{next, total}, vectors: f.all[base : base+2], searchers: 2, wantErr: "is past the 900 keys"},
		{name: "a vector of another length", docIDs: []uint64{next, next + 1}, vectors: [][]float32{f.all[base], f.all[base+1][:5]}, searchers: 2, wantErr: "a vector of 5 dimensions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := g.stats
			err := g.insertBatch(ctx, tt.docIDs, tt.vectors, tt.searchers)
			require.ErrorContains(t, err, tt.wantErr)
			require.Equal(t, before, g.stats, "nothing of the batch was searched or written")
			require.Equal(t, base, g.meta.Live)
		})
	}
	t.Run("a search that is cancelled", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		err := g.insertBatch(canceled, sequentialIDs(8, next), f.all[base:base+8], 4)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, base, g.meta.Live, "no insert was applied")
	})
	// An empty batch is no batch, and the index takes the next one.
	require.NoError(t, g.insertBatch(ctx, nil, nil, 4))
	require.NoError(t, g.insertBatch(ctx, sequentialIDs(50, next), f.all[base:base+50], 4))
	require.Equal(t, base+50, g.meta.Live)
	require.NoError(t, g.checkCoverage())
	// What a plan kept is its own: the plans of a batch share nothing.
	seen := map[*growKept]bool{}
	for _, plan := range g.batch {
		for q := range maps.Keys(plan.keptAt) {
			kept := &plan.kept[plan.keptAt[q]]
			require.False(t, seen[kept])
			seen[kept] = true
		}
	}
}

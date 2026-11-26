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

//go:build postingedgebench

package postingedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cleanup of the deleted vectors on disk is the pass the experiments
// measured in memory (cell B of v6.2), without its merges: the same vectors
// leave, every edge that led to one of them gets the same substitute or is
// taken out, and the two indexes are the same index after it, as they were
// before, with the vectors only marked. They go on being the same through the
// inserts that follow, which read the distances the pass stored for the
// substitutes, and through more rounds of deletes, inserts and passes, with
// the store flushed and the index closed and opened on the way.
// TestGrowCleanEqualsMemory, in the package, cleans smaller indexes in fewer
// ways.
func TestGrowCleanEqualsMemoryAtScale(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		cosine      bool
		threshold   int
		consolidate int
		// rounds of: share of the vectors marked, a pass, and inserts.
		rounds     int
		share      float64
		inserts    int
		flushEvery int
		// halo has the postings carry their halo, with no codes in memory
		// unless resident.
		halo, resident bool
	}{
		{name: "30 % at once", threshold: 60, consolidate: 4, rounds: 1, share: 0.30, inserts: 300, flushEvery: 37},
		{name: "30 % at once, cosine", cosine: true, threshold: 60, consolidate: DefaultConsolidate, rounds: 1, share: 0.30, inserts: 300},
		{name: "six rounds of 5 %", threshold: 60, consolidate: 8, rounds: 6, share: 0.05, inserts: 100, flushEvery: 41},
		{name: "six rounds of 5 %, rows only at splits", threshold: 60, consolidate: DefaultConsolidate, rounds: 6, share: 0.05, inserts: 100, flushEvery: 29},
		{name: "four rounds of 10 %, few splits", threshold: 400, consolidate: 16, rounds: 4, share: 0.10, inserts: 120, flushEvery: 53},
		{name: "half at once", threshold: 60, consolidate: 4, rounds: 1, share: 0.50, inserts: 200},
		{name: "no codes in memory, 30 % at once", threshold: 60, consolidate: 4, rounds: 1, share: 0.30, inserts: 300, flushEvery: 37, halo: true},
		{
			name: "no codes in memory, six rounds of 5 %, cosine", cosine: true, threshold: 60, consolidate: DefaultConsolidate,
			rounds: 6, share: 0.05, inserts: 100, flushEvery: 29, halo: true,
		},
		{
			name: "the halo kept up with the codes in memory, four rounds of 10 %", threshold: 60, consolidate: 8, rounds: 4,
			share: 0.10, inserts: 120, flushEvery: 53, halo: true, resident: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const base, dims = 1000, 48
			total := base + 400 + tt.rounds*tt.inserts
			format := shardFormat(tt.consolidate)
			if tt.halo {
				format = haloFormat(tt.consolidate, tt.resident)
			}
			f := newCleanFixtureFormat(t, total, base, dims, tt.cosine, tt.threshold, format)
			f.insertSome(t, 400)
			f.same(t, "grown")
			for round := range tt.rounds {
				markedBefore := f.marked
				f.markShare(t, tt.share, uint64(round)+3)
				f.same(t, "marked")
				require.Positive(t, f.g.deletedCount)

				// What this pass does, on disk and in the twin: the index on
				// disk counts from when it was last opened.
				before, twinBefore := f.g.cleaned, f.d.sub
				require.NoError(t, f.d.eliminateSubstituting(ctx, 0, 0))
				require.NoError(t, f.g.clean(ctx, 0, 0))
				did, twin := f.g.cleaned, f.d.sub.minus(twinBefore)
				require.Zero(t, f.g.deletedCount, "nothing is left marked")
				require.Equal(t, f.marked-markedBefore, did.Removed-before.Removed, "every marked vector left")
				require.Equal(t, twin.Substituted, did.Substituted-before.Substituted, "the substitutes")
				require.Equal(t, twin.Without, did.Without-before.Without, "the edges left without one")
				require.Equal(t, twin.CodeDistances, did.CodeDistances-before.CodeDistances, "the distances between codes")
				require.Equal(t, did.Substituted+did.Without, did.DeadEdges)
				require.Equal(t, did.Substituted, did.ExactDistances, "an exact distance for every substitute")
				require.Positive(t, twin.Substituted, "edges led to the vectors that left")
				f.same(t, "cleaned")

				if round == tt.rounds/2 {
					f.reopen(t)
					f.same(t, "cleaned, opened again")
				}
				for start := 0; start < tt.inserts; start += 50 {
					f.insertSome(t, min(50, tt.inserts-start))
					if tt.flushEvery > 0 && start%tt.flushEvery < 50 {
						require.NoError(t, f.g.flushAll())
					}
				}
				f.same(t, "grown after the cleanup")
			}
			f.vectorsIntact(t, "at the end")
			_, err := f.g.compactAll()
			require.NoError(t, err)
			f.same(t, "compacted")
			f.reopen(t)
			f.same(t, "at the end, opened again")
			f.vectorsIntact(t, "at the end, opened again")
		})
	}
}

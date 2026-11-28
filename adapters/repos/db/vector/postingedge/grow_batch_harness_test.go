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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The coverage of inserts in batches of grow_batch_test.go at the scale it
// was written for: a base of 1 000 vectors of 64 dimensions, 1 200 inserts
// and eleven ways to batch them. The package keeps fewer ways over a few
// hundred vectors; this runs apart, with -tags postingedgebench.

// Inserts in batches leave a whole index: every vector in one posting, the
// map and the rows telling the same, and every member with its list of
// edges. Every list an insert takes from what its plan kept is the one the
// index has. With batches as large as these and a threshold as low, plans
// meet both things a batch can do to them: their posting split before they
// were applied, and lists they kept changed.
func TestGrowBatchCoverageAtScale(t *testing.T) {
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

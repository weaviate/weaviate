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

// The halo tests of grow_halo_test.go at the scale they were written for: a
// base of 1 000 vectors and 1 200 to 1 600 inserts, or twelve rounds of
// deletes and inserts. The package keeps them over a few hundred vectors;
// these run apart, with -tags postingedgebench.

// An index whose postings carry their halo is the same index with its codes
// in memory and without them: opened one way and the other over the same
// files it answers the same, by the same work as its twin in memory, and what
// it writes one way is an index the other way. Without its codes in memory
// it holds none, reads those its writes need from their bucket, and the halo
// its search left is the one its writes use; with them it reads no code and
// reads the halo of the posting of every insert.
func TestGrowHaloWithAndWithoutCodesAtScale(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 2200, 1000, 64
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, haloFormat(8, true))
	require.True(t, f.g.haloed())
	require.False(t, f.g.noCodes)
	f.same(t, "the base, with its codes in memory")
	grow := func(n int) {
		for range n {
			at := f.inserted
			difference, err := f.insert(ctx)
			require.NoError(t, err, "doc %d", at)
			require.Empty(t, difference, "doc %d", at)
		}
	}
	// reopened opens the index the other way and checks that it answers what
	// it answered.
	reopened := func(noCodes bool, at string) {
		ids, dists := haloAnswers(t, f)
		f.how.noCodes = noCodes
		f.reopen(t)
		require.Equal(t, noCodes, f.g.noCodes, at)
		if noCodes {
			require.Empty(t, f.g.st.side.codes, "%s: no code in memory", at)
		}
		again, againDists := haloAnswers(t, f)
		require.Equal(t, ids, again, "%s: the same docs", at)
		require.Equal(t, dists, againDists, "%s: at the same distances", at)
		f.same(t, at)
	}

	grow(300)
	f.same(t, "grown with its codes in memory")
	require.Zero(t, f.g.stats.RoutingReads.N, "the codes are in memory")
	require.Positive(t, f.g.stats.HaloReads.N, "an insert reads the halo of its posting: its search did not")
	require.Positive(t, f.g.stats.HaloAdded.N)
	require.Positive(t, f.g.stats.HaloRows.N, "the halves of a split get their halos")

	reopened(true, "opened with no codes in memory")
	splits := f.g.stats.Splits
	grow(400)
	f.same(t, "grown with no codes in memory")
	require.Positive(t, f.g.stats.Splits-splits, "postings split")
	require.Positive(t, f.g.stats.RoutingReads.N, "the codes of the neighbors of an insert are read from their bucket")
	// A plan whose posting split under it reads the halo of the one it goes
	// to; no other insert reads one.
	require.LessOrEqual(t, f.g.stats.HaloReads.N, int64(f.g.stats.PlansReassign), "the search of an insert leaves the halo of its posting")

	reopened(false, "opened with its codes in memory again")
	grow(300)
	reopened(true, "opened with no codes in memory again")
	grow(200)
	f.same(t, "at the end")
	require.NoError(t, f.g.flushAll())
	_, err := f.g.compactAll()
	require.NoError(t, err)
	f.same(t, "compacted")
}

// The halo of a posting that does not split does not grow for ever. In an
// index that loses vectors and takes as many, round after round, the postings
// stay the size they were, and every round adds entries to their halos: of
// the vectors that came, and of the substitutes of those that left. Once what
// was added is more than half of what the edges of a posting need, its halo
// is written whole again, with what they need and no more. The index stays
// its twin in memory through it, and once the store has compacted, what it
// holds of a halo is what is read of it. With that left out, the same rounds
// leave halos several times what their edges need.
func TestGrowHaloIsWrittenWholeAgainAtScale(t *testing.T) {
	ctx := context.Background()
	const base, dims, threshold = 1000, 48, 400
	const rounds, share, inserts = 12, 0.10, 100
	tests := []struct {
		name string
		// off leaves the halos as they grow.
		off bool
	}{
		{name: "written whole again as it grows"},
		{name: "left to grow", off: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total := base + 200 + rounds*inserts
			f := newCleanFixtureFormat(t, total, base, dims, false, threshold, haloFormat(8, false))
			g := f.g
			g.noHaloTrim = tt.off
			f.insertSome(t, 200)
			splits := g.stats.Splits
			for round := range rounds {
				f.markShare(t, share, uint64(round)+11)
				require.NoError(t, f.d.eliminateSubstituting(ctx, 0, 0))
				require.NoError(t, g.clean(ctx, 0, 0))
				f.insertSome(t, inserts)
				if round%4 == 3 {
					require.NoError(t, g.flushAll())
					f.same(t, "on the way")
				}
			}
			f.same(t, "at the end")
			require.LessOrEqual(t, g.stats.Splits-splits, 2, "the postings of this index do not split: their halos are not written whole by a split")

			// What every halo holds against what the edges of its posting need.
			worst, over := 0.0, 0
			live := 0
			for p := range uint32(g.meta.Postings) {
				if g.retired[p] {
					continue
				}
				live++
				held, needed := haloEntries(t, g, p)
				require.GreaterOrEqual(t, held, needed, "posting %d", p)
				ratio := float64(held) / float64(max(needed, 1))
				worst = max(worst, ratio)
				// Half more than what it needs, and what a posting gathers
				// between two times its row of edges is written.
				if held > needed+needed/2+2*growHaloTrimFloor {
					over++
				}
			}
			t.Logf("%d live postings: the largest halo holds %.2f times what its edges need, %d hold more than half again; "+
				"%d halos written whole again, %d values taken out, %d entries added", live, worst, over, g.stats.HaloTrims,
				g.stats.HaloTrimmed.N, g.stats.HaloAdded.N)
			if tt.off {
				require.Zero(t, g.stats.HaloTrims)
				require.Greater(t, worst, 2.0, "left to grow, a halo holds several times what its edges need")
				return
			}
			require.Positive(t, g.stats.HaloTrims, "halos were written whole again")
			require.Zero(t, over, "no halo holds more than half again what its edges need")

			// Compacted, the store holds of every halo what is read of it: the
			// values that were taken out are gone, and not only left out.
			require.NoError(t, g.flushAll())
			_, err := g.compactAll()
			require.NoError(t, err)
			f.same(t, "compacted")
			entry := 4 + 8*g.meta.RoutingCodeWords
			for p := range uint32(g.meta.Postings) {
				if g.retired[p] {
					continue
				}
				values, err := g.postings.SetRawList(diskKey(p, diskBlockHalo))
				require.NoError(t, err)
				stored := 0
				for _, value := range values {
					stored += len(value) / entry
				}
				held, _ := haloEntries(t, g, p)
				// The mark of the whole value is an entry of the store's too.
				require.LessOrEqual(t, stored, held+1, "posting %d: the store holds %d entries of a halo of %d", p, stored, held)
			}
			f.reopen(t)
			f.same(t, "opened again")
		})
	}
}

// The inserts of a batch are searched before any of them is written, and the
// search of each leaves the halo of its posting for its writes. A write of
// the batch that has that halo written whole again leaves out of it entries
// the plans that follow still hold: a plan whose halo was written whole since
// it was read reads it again. With rows written after a few changes and
// batches this large it happens all along, and the index stays covered.
func TestGrowHaloThroughBatchesAtScale(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 2600, 1000, 64
	tests := []struct {
		name        string
		batch       int
		consolidate int
		threshold   int
	}{
		{name: "batches of thirty-two, a row every four changes", batch: 32, consolidate: 4, threshold: 60},
		{name: "batches of a hundred, few splits", batch: 100, consolidate: 4, threshold: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newGrowFixtureFormat(t, total, base, dims, false, 64, tt.threshold, haloFormat(tt.consolidate, false))
			g := f.g
			batches := 0
			for from := base; from < total; from += tt.batch {
				to := min(from+tt.batch, total)
				docIDs := sequentialIDs(to-from, uint64(from))
				require.NoError(t, g.insertBatch(ctx, docIDs, f.all[from:to], 8), "docs %d to %d", from, to)
				if batches++; batches%4 == 0 {
					require.NoError(t, g.flushAll())
					require.NoError(t, g.cleanCentroidGraph())
					require.NoError(t, g.checkCoverage(), "after doc %d", to-1)
				}
			}
			require.NoError(t, g.cleanCentroidGraph())
			require.NoError(t, g.checkCoverage(), "at the end")
			t.Logf("%d halos written whole again, %d halos read again by a write, %d plans chose their posting again",
				g.stats.HaloTrims, g.stats.HaloReads.N, g.stats.PlansReassign)
			require.Positive(t, g.stats.HaloTrims, "halos were written whole again")
			require.Greater(t, g.stats.HaloReads.N, int64(g.stats.PlansReassign),
				"plans read again a halo that was written whole after their search, besides those whose posting split")

			// And it answers what it answers with its codes in memory.
			ids, dists := haloAnswers(t, f)
			f.how.noCodes = false
			require.NoError(t, g.close(ctx))
			f.g = nil
			f.open(t)
			require.NoError(t, f.g.cleanCentroidGraph())
			require.NoError(t, f.g.checkCoverage(), "opened with its codes in memory")
			resident, residentDists := haloAnswers(t, f)
			require.Equal(t, ids, resident)
			require.Equal(t, dists, residentDists)
		})
	}
}

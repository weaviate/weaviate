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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The equivalences of grow_equiv_test.go at the scale they were written for:
// a base of 1 000 vectors of 64 dimensions and 1 000 to 1 200 inserts per
// case. The package keeps them over a few hundred vectors; these run apart,
// with -tags postingedgebench.

// The index that grows on disk is the streaming index of v2.x with its
// postings in the store: grown by the same inserts, it is the same index at
// every step. The store is flushed and compacted on the way, so that rows
// are read across segments, lists that were replaced meet their tombstones
// in another segment, and the rows of split postings are dropped; and the
// index is closed and opened again in the middle, and goes on.
func TestGrowInsertsEqualMemoryAtScale(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		cosine    bool
		bits      int
		threshold int
		// flushEvery and compactEvery are in inserts, 0 for never.
		flushEvery, compactEvery int
		reopenAt                 int
		// leveled gives the store its leveled compaction, and one round of
		// it after every flush: what was replaced then stays in segments of
		// several levels, with its tombstones.
		leveled bool
	}{
		{name: "the full code in memory", threshold: 60, flushEvery: 37},
		{name: "a sketch in memory", bits: 64, threshold: 60, flushEvery: 53, compactEvery: 211, reopenAt: 500},
		{name: "a sketch in memory, cosine", bits: 128, cosine: true, threshold: 60, flushEvery: 29, compactEvery: 97, reopenAt: 333},
		{name: "everything in the memtable", bits: 64, threshold: 60},
		{name: "few splits", bits: 64, threshold: 400, flushEvery: 41, compactEvery: 173},
		{name: "the leveled compaction of the store", bits: 64, threshold: 60, flushEvery: 23, reopenAt: 700, leveled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 2200, 1000, 64
			f := newGrowFixture(t, total, base, dims, tt.cosine, tt.bits, tt.threshold)
			if tt.leveled {
				f.leveled = true
				f.reopen(t)
			}
			f.same(t, "the base")
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				done := f.inserted - base
				if tt.flushEvery > 0 && done%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
					if tt.leveled {
						f.g.compaction.CycleCallback(func() bool { return false })
					}
				}
				if tt.compactEvery > 0 && done%tt.compactEvery == 0 {
					_, err := f.g.compactAll()
					require.NoError(t, err)
				}
				if done == tt.reopenAt {
					f.same(t, "before closing")
					f.reopen(t)
					f.same(t, "opened again")
				}
				if done%300 == 0 {
					f.same(t, "on the way")
				}
			}
			f.same(t, "at the end")
			assert.Equal(t, total, f.g.meta.Live)
			if tt.threshold < 100 {
				assert.Greater(t, f.g.meta.Splits, 10, "a threshold of %d over postings of 40 splits", tt.threshold)
			}
			assert.Positive(t, f.g.stats.InverseEdges)
			assert.Zero(t, f.g.stats.InverseListReads, "the search of an insert loads the posting of every neighbor it finds")
			assert.Zero(t, f.g.stats.InverseVectors.N, "the rescore of an insert reads the vector of every neighbor it finds")

			if tt.leveled {
				segments, err := os.ReadDir(filepath.Join(f.dir, diskStoreDir, growBucketPostings))
				require.NoError(t, err)
				assert.Greater(t, len(segments), 4, "the leveled compaction leaves the bucket in several segments")
				// The compaction for good is the forced one.
				f.leveled = false
				f.reopen(t)
			}
			// Compacted, closed and opened again, it is still the same index.
			_, err := f.g.compactAll()
			require.NoError(t, err)
			f.same(t, "compacted")
			f.reopen(t)
			f.same(t, "compacted and opened again")
			require.Error(t, f.g.insert(ctx, 0, f.all[0]), "a doc goes in once")
			require.Error(t, f.g.insert(ctx, uint64(total), f.all[0]), "no room past the keys")
		})
	}
}

// An index laid out again is the same index: every posting in one segment of
// a bucket written anew, and with that the exact vectors in posting order.
// With a value per list of edges, as v5.0 stores them, and with the rows of
// edges consolidated, as v5.1 does: the combined control lays that one out.
func TestGrowReorganizedAtScale(t *testing.T) {
	tests := []struct {
		name        string
		consolidate int
		withMembers bool
	}{
		{name: "a value per list of edges"},
		{name: "the rows of edges consolidated", consolidate: 8},
		{name: "the rows of edges next to the members", consolidate: 8, withMembers: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { growReorganizedCaseAtScale(t, tt.consolidate, tt.withMembers) })
	}
}

func growReorganizedCaseAtScale(t *testing.T, consolidate int, withMembers bool) {
	ctx := context.Background()
	const total, base, dims = 2000, 1000, 64
	f := newGrowFixtureRows(t, total, base, dims, false, 64, 60, consolidate, withMembers)
	for f.inserted < total {
		difference, err := f.insert(ctx)
		require.NoError(t, err)
		require.Empty(t, difference)
		if f.inserted%47 == 0 {
			require.NoError(t, f.g.flushAll())
		}
	}
	if consolidate > 0 {
		// What waits in memory is not on disk yet: it is written first.
		require.NotEmpty(t, f.g.pending)
		require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), f.g, t.TempDir(), false), "waiting in memory")
		require.NoError(t, f.g.flushPending())
		require.Empty(t, f.g.pending)
	}
	f.same(t, "grown")
	before, err := os.ReadDir(filepath.Join(f.dir, diskStoreDir, growBucketPostings))
	require.NoError(t, err)

	tests := []struct {
		name  string
		order bool
	}{
		{name: "one segment"},
		{name: "one segment and the vectors in posting order", order: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, reorganizeGrowIndex(ctx, testLogger(), f.g, dir, tt.order))
			cell, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{})
			require.NoError(t, err)
			defer func() { require.NoError(t, cell.close(ctx)) }()

			segments := 0
			entries, err := os.ReadDir(filepath.Join(dir, diskStoreDir, growBucketPostings))
			require.NoError(t, err)
			for _, entry := range entries {
				if filepath.Ext(entry.Name()) == ".db" {
					segments++
				}
			}
			require.Equal(t, 1, segments, "the bucket of postings is one segment")
			require.Greater(t, len(before), len(entries), "the index it comes from had more")
			if consolidate > 0 {
				// The rows of edges are one segment too, each the row of its
				// posting with its lists in the order of the members: in
				// their own bucket, or in that of the postings.
				require.Equal(t, withMembers, cell.meta.RowsWithMembers)
				if withMembers {
					require.Nil(t, cell.edges)
					require.NoDirExists(t, filepath.Join(dir, diskStoreDir, growBucketEdges))
				} else {
					require.NotNil(t, cell.edges)
					rows := 0
					entries, err := os.ReadDir(filepath.Join(dir, diskStoreDir, growBucketEdges))
					require.NoError(t, err)
					for _, entry := range entries {
						if filepath.Ext(entry.Name()) == ".db" {
							rows++
						}
					}
					require.Equal(t, 1, rows, "the bucket of rows of edges is one segment")
				}
				var dp diskPosting
				var row growBlock
				for p := range uint32(cell.meta.Postings) {
					block, err := cell.getRow(p)
					require.NoError(t, err)
					if cell.retired[p] {
						require.Nil(t, block, "posting %d is retired", p)
						continue
					}
					require.NoError(t, cell.fetchMembers(p, &dp))
					require.NoError(t, row.decode(block))
					require.Len(t, row.ids, len(dp.docIDs))
					for i, docID := range dp.docIDs {
						require.Equal(t, docID, uint64(row.ids[i]), "member %d of posting %d", i, p)
					}
				}
			} else {
				require.Nil(t, cell.edges)
			}

			require.NoError(t, cell.cleanCentroidGraph())
			require.NoError(t, cell.checkCoverage())
			require.NoError(t, cell.sameAs(f.twin))
			for i, query := range f.queries {
				difference, err := growDifference(ctx, f.twin, cell, query, 10)
				require.NoError(t, err, "query %d", i)
				require.Empty(t, difference, "query %d", i)
			}
			require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), f.g, dir, tt.order), "is not empty")

			if !tt.order {
				require.Nil(t, cell.slotOf)
				return
			}
			// The vectors of every posting are in a row, in the order of its
			// members, and each is the vector of its doc.
			require.Len(t, cell.slotOf, total)
			next := uint32(0)
			var dp diskPosting
			for p := range uint32(cell.meta.Postings) {
				if cell.retired[p] {
					continue
				}
				require.NoError(t, cell.fetchMembers(p, &dp))
				for _, docID := range dp.docIDs {
					require.Equal(t, next, cell.slotOf[docID], "doc %d of posting %d", docID, p)
					next++
					_, vector, err := cell.exact(docID, nil, nil)
					require.NoError(t, err)
					require.Equal(t, f.all[docID], vector, "the vector of doc %d", docID)
				}
			}
			require.Equal(t, uint32(total), next)
			require.ErrorContains(t, cell.insert(ctx, 0, f.all[0]), "does not grow")
			require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), cell, t.TempDir(), false), "laid out again already")
		})
	}
}

// With consolidated edges (v5.1) the index is still the streaming index of
// v2.x at every insert, with the changes to its edges waiting in memory, and
// with everything written it is the same index in its store: the rows of
// edges are written whole after so many changes, when a posting splits and
// when the index closes, and nothing else writes an edge. With the rows in a
// bucket of their own (v5.1) and with them in the bucket of postings, next
// to the members (v5.3), where writing a row again takes the old one out.
func TestGrowConsolidatedEqualsMemoryAtScale(t *testing.T) {
	for _, withMembers := range []bool{false, true} {
		name := "rows in a bucket of their own"
		if withMembers {
			name = "rows next to the members"
		}
		t.Run(name, func(t *testing.T) { growConsolidatedCasesAtScale(t, withMembers) })
	}
}

func growConsolidatedCasesAtScale(t *testing.T, withMembers bool) {
	ctx := context.Background()
	tests := []struct {
		name string
		// k is the changes a row of edges is written after.
		k         int
		cosine    bool
		bits      int
		threshold int
		// flushEvery is in inserts, 0 for never; with leveled the store
		// compacts by levels, one round after every flush.
		flushEvery int
		leveled    bool
		// reopenAt closes the index, which writes what waits, and opens it.
		reopenAt int
	}{
		{name: "a row at every change", k: 1, bits: 64, threshold: 60, flushEvery: 37},
		{name: "a row every three changes", k: 3, bits: 64, threshold: 60, flushEvery: 41, reopenAt: 500},
		{name: "a row every sixteen, cosine", k: 16, bits: 128, cosine: true, threshold: 60, flushEvery: 29, reopenAt: 333},
		{name: "rows only at splits and at the end", k: 1 << 20, bits: 64, threshold: 60, flushEvery: 53},
		{name: "few splits, rows by their changes", k: 8, bits: 64, threshold: 400, flushEvery: 47},
		{name: "the full code, everything in the memtable", k: 5, threshold: 60},
		{name: "the leveled compaction of the store", k: 4, bits: 64, threshold: 60, flushEvery: 23, leveled: true, reopenAt: 700},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 2200, 1000, 64
			f := newGrowFixtureRows(t, total, base, dims, tt.cosine, tt.bits, tt.threshold, tt.k, withMembers)
			if tt.leveled {
				f.leveled = true
				f.reopen(t)
			}
			require.Equal(t, !withMembers, f.g.edges != nil, "the bucket of rows of edges")
			require.True(t, f.g.consolidated())
			f.same(t, "the base")
			var rows, atClose int
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				done := f.inserted - base
				if tt.flushEvery > 0 && done%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
					if tt.leveled {
						f.g.compaction.CycleCallback(func() bool { return false })
					}
				}
				if done == tt.reopenAt {
					f.same(t, "before closing, with changes waiting")
					rows += f.g.stats.RowWritesFull + f.g.stats.RowWritesSplit
					f.reopen(t)
					require.Empty(t, f.g.pending, "an index that was closed has nothing waiting")
					f.same(t, "opened again")
				}
				if done%300 == 0 {
					f.same(t, "on the way, with changes waiting")
				}
			}
			f.same(t, "at the end, with changes waiting")
			if tt.k < 100 {
				assert.Positive(t, f.g.stats.RowWritesFull, "rows are written after %d changes", tt.k)
				assert.Positive(t, f.g.stats.PendingPeak)
				if withMembers {
					assert.Positive(t, f.g.stats.RowTombstones.N, "a row written again takes the old one out")
				} else {
					assert.Zero(t, f.g.stats.RowTombstones.N)
				}
			} else {
				assert.Zero(t, f.g.stats.RowWritesFull, "no posting gets to %d changes", tt.k)
				assert.NotEmpty(t, f.g.pending, "the changes since the last split wait")
			}
			if tt.threshold < 100 {
				assert.Positive(t, f.g.stats.RowWritesSplit)
			}
			assert.Zero(t, f.g.stats.InverseTombs.N, "no list is taken out of a row value by value")
			for p, pend := range f.g.pending {
				assert.Less(t, pend.changes, max(tt.k, 1), "posting %d waits with fewer changes than a row is written after", p)
			}

			// With everything written, the store holds the index: nothing
			// waits, and it is the twin, as it is and opened again.
			require.NoError(t, f.g.flushPending())
			atClose = f.g.stats.RowWritesClose
			require.Empty(t, f.g.pending)
			require.Zero(t, f.g.pendingMembers)
			require.Zero(t, f.g.pendingBytes)
			f.same(t, "with everything written")
			f.reopen(t)
			f.same(t, "with everything written, opened again")
			if tt.leveled {
				f.leveled = false
				f.reopen(t)
			}
			_, err := f.g.compactAll()
			require.NoError(t, err)
			f.same(t, "compacted")
			if tt.k > 1 {
				assert.Positive(t, atClose+rows, "rows are written")
			}
			// No list lives in the bucket of postings. With the rows of
			// edges next to the members, the key of the lists of a live
			// posting holds its row, one value. A retired one holds
			// nothing after the compaction, which skips its keys.
			for p := range uint32(f.g.meta.Postings) {
				lists, err := f.g.postings.SetList(diskKey(p, diskBlockEdges))
				require.NoError(t, err)
				if withMembers && !f.g.retired[p] {
					require.Len(t, lists, 1, "posting %d", p)
					var row growBlock
					require.NoError(t, row.decode(lists[0]), "posting %d", p)
					require.Len(t, row.ids, int(f.g.size[p]), "posting %d", p)
					continue
				}
				require.Empty(t, lists, "posting %d", p)
			}
		})
	}
}

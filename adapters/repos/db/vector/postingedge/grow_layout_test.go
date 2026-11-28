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
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shardFormat is the format the index a shard opens writes its base with,
// opened as a shard opens it: with what waits of its edges in a bucket, and
// with every write through its journal.
func shardFormat(consolidate int) growConfig {
	return growConfig{
		consolidate: consolidate, rowsWithMembers: true, slots: true, open: growOpen{keepPending: true, journal: true},
	}
}

// haloFormat is shardFormat with the halo of every posting written and kept
// up, and the index opened with no codes in memory, or, with resident, with
// them: the halo is then written and nobody reads it.
func haloFormat(consolidate int, resident bool) growConfig {
	format := shardFormat(consolidate)
	format.halo, format.open.noCodes = true, !resident
	return format
}

// vectorsIntact checks that the index reads, for every doc it holds, the
// vector it was given.
func (f *growFixture) vectorsIntact(t *testing.T, at string) {
	t.Helper()
	var raw []byte
	var vector []float32
	for docID, held := range f.g.live {
		if !held {
			continue
		}
		var err error
		raw, vector, err = f.g.exact(uint64(docID), raw, vector)
		require.NoError(t, err, "%s: doc %d", at, docID)
		require.Equal(t, f.all[docID], vector, "%s: the vector of doc %d", at, docID)
	}
}

// fewRescored is the rescore limit of the tests that grow small indexes, under
// the one of the fixture's configuration: every candidate a search rescores
// is a read of the file of vectors.
const fewRescored = 20

// rescoring has the twin and the index on disk rescore at most limit
// candidates of a search, and the index on disk keep that limit when it opens
// again.
func (f *growFixture) rescoring(limit int) {
	f.uc.RescoreLimit = limit
	f.g.ix.storeSearchConfig(f.uc)
	f.twin.ix.storeSearchConfig(f.uc)
}

// laidOut checks what a pass over the vectors with nothing next to it leaves:
// no slot without a vector, and the vectors of every posting in a row.
func (f *growFixture) laidOut(t *testing.T, at string) {
	t.Helper()
	g := f.g
	require.Equal(t, g.meta.Live, int(g.nextSlot), "%s: a slot per vector", at)
	require.Equal(t, g.meta.Live, g.meta.VectorsLaidOut, at)
	first := make(map[uint32]uint32)
	last := make(map[uint32]uint32)
	for docID, held := range g.live {
		if !held {
			continue
		}
		p, slot := g.st.side.postingOf[docID], g.slotOf[docID]
		if _, seen := first[p]; !seen {
			first[p], last[p] = slot, slot
		}
		first[p], last[p] = min(first[p], slot), max(last[p], slot)
	}
	for p, from := range first {
		require.Equal(t, g.size[p], last[p]-from+1, "%s: the vectors of posting %d are in a row", at, p)
	}
}

// generationFiles returns the files of vectors and of slots the directory of
// the index holds.
func generationFiles(t *testing.T, dir string) []string {
	t.Helper()
	var names []string
	for _, pattern := range []string{"vectors*.f32", "slots*.u32"} {
		found, err := filepath.Glob(filepath.Join(dir, pattern))
		require.NoError(t, err)
		for _, path := range found {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names
}

// An index whose vectors have slots, and one that keeps in a bucket what
// waits of its edges, is the streaming index in memory at every insert, as
// the index of v5.0 is: where a vector is on disk and where a change waits
// change nothing of what the index is. Its vectors are laid out again on the
// way, at once and spread over twenty inserts, with postings splitting under
// the pass, and it is closed and opened with changes waiting.
// TestGrowSlotsAndPendingEqualMemoryAtScale, of the bench harness, grows
// larger indexes over more formats.
func TestGrowSlotsAndPendingEqualMemory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name       string
		format     growConfig
		cosine     bool
		bits       int
		flushEvery int
	}{
		{name: "a value per list, vectors by slot", format: growConfig{slots: true}, bits: 64, flushEvery: 19},
		{name: "rows next to the members", format: shardFormat(4), bits: 64, flushEvery: 17},
		{
			name:   "rows in a bucket of their own, vectors at their doc ids",
			format: growConfig{consolidate: 8, open: growOpen{keepPending: true}}, bits: 64, flushEvery: 23,
		},
		{
			name: "no codes in memory, cosine, the full code, everything in the memtable", format: haloFormat(DefaultConsolidate, false),
			cosine: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The vectors are laid out at once after layOutAt inserts, the
			// index is opened again after reopenAt, and a pass lays them out
			// next to the inserts from passFrom to passTo.
			const base, inserts, dims, threshold = 60, 60, 32, 24
			const layOutAt, reopenAt, passFrom, passTo = 10, 20, 30, 50
			f := newGrowFixtureFormat(t, base+inserts, base, dims, tt.cosine, tt.bits, threshold, tt.format)
			f.rescoring(fewRescored)
			f.queries = f.queries[:3]
			slots := tt.format.slots
			require.Equal(t, tt.format.open.keepPending && tt.format.consolidate > 0, f.g.waiting != nil)
			f.same(t, "the base")
			f.vectorsIntact(t, "the base")
			if slots {
				require.Equal(t, 1, f.g.meta.VectorGen)
				f.laidOut(t, "the base")
			} else {
				require.Zero(t, f.g.meta.VectorGen)
				require.Nil(t, f.g.slotOf)
			}

			var pass *growLayout
			splitsBefore := 0
			for f.inserted < base+inserts {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				done := f.inserted - base
				if tt.flushEvery > 0 && done%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
				}
				switch {
				case done == layOutAt && slots:
					require.NoError(t, f.g.layOut())
					f.laidOut(t, "laid out at once")
					f.vectorsIntact(t, "laid out at once")
					f.same(t, "laid out at once")
				case done == reopenAt:
					waited := f.g.pendingMembers
					f.reopen(t)
					if f.g.waiting != nil {
						require.Equal(t, waited, f.g.pendingMembers, "what waited when it closed waits when it opens")
					}
					f.same(t, "opened again")
					f.vectorsIntact(t, "opened again")
				case done == passFrom && slots:
					pass, err = f.g.beginLayout()
					require.NoError(t, err)
					splitsBefore = f.g.stats.Splits
				case done == passTo && slots:
					require.NoError(t, pass.finish())
					pass = nil
					require.Equal(t, f.g.meta.Live, int(f.g.nextSlot), "a slot per vector")
					require.Equal(t, f.g.meta.Live-(passTo-passFrom), f.g.meta.VectorsLaidOut,
						"those that came in under the pass are after the others")
					require.Greater(t, f.g.stats.Splits, splitsBefore, "postings split under the pass")
					f.vectorsIntact(t, "laid out next to the inserts")
					f.same(t, "laid out next to the inserts")
				}
				if pass != nil {
					// Fewer vectors than the pass started with: finish copies
					// the rest.
					_, err := pass.step(2)
					require.NoError(t, err)
				}
			}
			f.same(t, "at the end")
			f.vectorsIntact(t, "at the end")
			// What the index counted since it was opened, halfway.
			if f.g.waiting != nil {
				assert.Positive(t, f.g.stats.PendingLog.N, "the changes that wait are written to their bucket")
				if tt.format.consolidate <= 8 {
					assert.Positive(t, f.g.stats.RowWritesFull, "rows are written after %d changes", tt.format.consolidate)
				}
			} else {
				assert.Zero(t, f.g.stats.PendingLog.N)
			}
			f.reopen(t)
			f.same(t, "at the end, opened again")
			f.vectorsIntact(t, "at the end, opened again")
			if slots {
				require.Equal(t, 3, f.g.meta.VectorGen, "two passes after the base")
				require.Equal(t, []string{"slots-3.u32", "vectors-3.f32"}, generationFiles(t, f.dir))
			} else {
				require.Equal(t, []string{"vectors.f32"}, generationFiles(t, f.dir))
			}
		})
	}
}

// What a sync hands to the system is the whole index when what waits of its
// edges is kept in a bucket: an index opened from a copy of its files, taken
// with the index still open, is the index it was. Without that bucket what
// waited is nowhere, and the copy has members without edges: the experiments
// close every index they grow, and a shard does not.
func TestGrowOpensWhatASyncLeft(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name        string
		keepPending bool
	}{
		{name: "what waits is kept in a bucket", keepPending: true},
		{name: "what waits is in memory alone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const total, base, dims, threshold = 100, 60, 32, 24
			format := shardFormat(DefaultConsolidate)
			format.open.keepPending = tt.keepPending
			f := newGrowFixtureFormat(t, total, base, dims, false, 64, threshold, format)
			f.rescoring(fewRescored)
			f.queries = f.queries[:3]
			for f.inserted < total {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
				if (f.inserted-base)%17 == 0 {
					require.NoError(t, f.g.flushAll())
				}
			}
			require.Positive(t, f.g.stats.Splits, "postings split")
			require.NotEmpty(t, f.g.pending, "changes wait")
			require.NoError(t, f.g.sync())

			dir := t.TempDir()
			require.NoError(t, os.CopyFS(dir, os.DirFS(f.dir)))
			c, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{keepPending: tt.keepPending})
			require.NoError(t, err)
			defer func() { require.NoError(t, c.release(ctx)) }()
			if !tt.keepPending {
				require.Empty(t, c.pending)
				require.ErrorContains(t, c.checkCoverage(), "has no list of edges")
				return
			}
			require.Len(t, c.pending, len(f.g.pending))
			require.Equal(t, f.g.pendingMembers, c.pendingMembers)
			for p, pend := range f.g.pending {
				require.NotNil(t, c.pending[p], "posting %d", p)
				require.Equal(t, pend.lists, c.pending[p].lists, "posting %d", p)
				require.Equal(t, pend.dists, c.pending[p].dists, "posting %d", p)
			}
			require.NoError(t, c.cleanCentroidGraph())
			require.NoError(t, c.checkCoverage())
			for i, query := range f.queries {
				wantIDs, wantDists, err := f.g.search(ctx, query, 10, nil)
				require.NoError(t, err)
				ids, dists, err := c.search(ctx, query, 10, nil)
				require.NoError(t, err)
				require.Equal(t, wantIDs, ids, "query %d", i)
				require.Equal(t, wantDists, dists, "query %d", i)
			}
		})
	}
}

// A row of edges is handed to the system before the changes it holds leave
// the bucket of what waits. Every bucket has a log of its own, written when
// it fills: here the log of what waits is written right after a row is, and
// that of the rows is not, as when the first fills before the second. An
// index opened from a copy of the files as they are then is whole. With the
// deletes on disk and the row still in memory, the members that waited would
// have no edges.
func TestGrowRowIsHandedBeforeItsChangesLeave(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const total, base, dims = 80, 60, 32
	// Rows are written only when a posting splits, and none does: every
	// change of the inserts waits.
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 400, shardFormat(1<<20))
	f.rescoring(fewRescored)
	f.queries = f.queries[:3]
	for f.inserted < total {
		_, err := f.insert(ctx)
		require.NoError(t, err)
	}
	g := f.g
	require.NoError(t, g.sync())
	require.NotEmpty(t, g.pending)
	rows := 0
	for p := range uint32(g.meta.Postings) {
		if g.pending[p] == nil {
			continue
		}
		require.NoError(t, g.writeRow(p, &rows))
		require.Nil(t, g.pending[p])
	}
	require.Positive(t, rows)
	require.Empty(t, g.pending)
	require.NoError(t, g.waiting.WriteWAL())

	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(f.dir)))
	c, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{keepPending: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.release(ctx)) }()
	require.Empty(t, c.pending, "what waited is in the rows")
	require.NoError(t, c.cleanCentroidGraph())
	require.NoError(t, c.checkCoverage())
	for i, query := range f.queries {
		wantIDs, wantDists, err := g.search(ctx, query, 10, nil)
		require.NoError(t, err)
		ids, dists, err := c.search(ctx, query, 10, nil)
		require.NoError(t, err)
		require.Equal(t, wantIDs, ids, "query %d", i)
		require.Equal(t, wantDists, dists, "query %d", i)
	}
}

// A pass over the vectors that did not end leaves the index as it was: what
// it wrote is removed when it is told to stop, and when the index opens next
// if nobody told it. So are the files of a generation that was left behind.
func TestGrowLayoutThatDoesNotEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const total, base, dims, threshold = 80, 60, 32, 24
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, threshold, shardFormat(8))
	f.rescoring(fewRescored)
	f.queries = f.queries[:3]
	for f.inserted < base+10 {
		difference, err := f.insert(ctx)
		require.NoError(t, err)
		require.Empty(t, difference)
	}

	// Stopped halfway.
	pass, err := f.g.beginLayout()
	require.NoError(t, err)
	done, err := pass.step(30)
	require.NoError(t, err)
	require.False(t, done)
	require.Contains(t, generationFiles(t, f.dir), "vectors-2.f32")
	pass.abort()
	pass.abort()
	require.Equal(t, []string{"slots-1.u32", "vectors-1.f32"}, generationFiles(t, f.dir))
	require.Equal(t, 1, f.g.meta.VectorGen)
	f.vectorsIntact(t, "after a pass that was stopped")

	// Left halfway by a process that died: the copy of its files opens as
	// the generation it was, and lays its vectors out when asked.
	pass, err = f.g.beginLayout()
	require.NoError(t, err)
	_, err = pass.step(30)
	require.NoError(t, err)
	require.NoError(t, pass.settle())
	require.NoError(t, f.g.sync())
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(f.dir)))
	pass.abort()
	require.Contains(t, generationFiles(t, dir), "vectors-2.f32")
	c, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{keepPending: true})
	require.NoError(t, err)
	require.Equal(t, []string{"slots-1.u32", "vectors-1.f32"}, generationFiles(t, dir))
	require.NoError(t, c.cleanCentroidGraph())
	require.NoError(t, c.checkCoverage())
	require.NoError(t, c.layOut())
	require.Equal(t, []string{"slots-2.u32", "vectors-2.f32"}, generationFiles(t, dir))
	require.NoError(t, c.checkCoverage())
	var raw []byte
	var vector []float32
	for docID, held := range c.live {
		if held {
			raw, vector, err = c.exact(uint64(docID), raw, vector)
			require.NoError(t, err)
			require.Equal(t, f.all[docID], vector, "doc %d", docID)
		}
	}
	// The files of a generation that is not the current one go when it opens.
	require.NoError(t, c.close(ctx))
	for _, name := range []string{"vectors-1.f32", "slots-1.u32", "vectors-7.f32", "slots-3.u32"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("left behind"), 0o644))
	}
	c, err = openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{keepPending: true})
	require.NoError(t, err)
	require.Equal(t, []string{"slots-2.u32", "vectors-2.f32"}, generationFiles(t, dir))
	require.NoError(t, c.close(ctx))

	// The index goes on, and is still its twin.
	for f.inserted < total {
		difference, err := f.insert(ctx)
		require.NoError(t, err)
		require.Empty(t, difference)
	}
	require.NoError(t, f.g.layOut())
	f.same(t, "at the end")
	f.vectorsIntact(t, "at the end")
}

// An index does not open over a table of slots that would have it read the
// wrong vector, or none: it says what is wrong with it. And only an index
// whose vectors have slots lays them out again. The cases harm copies of the
// files of one index, closed.
func TestGrowSlotsStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const total, base, dims = 50, 40, 32
	entry := func(slot uint32) []byte {
		raw := make([]byte, 4)
		binary.LittleEndian.PutUint32(raw, slot+1)
		return raw
	}
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, shardFormat(8))
	f.rescoring(fewRescored)
	for f.inserted < total {
		_, err := f.insert(ctx)
		require.NoError(t, err)
	}
	t.Run("the copy of the experiments is not for an index with slots", func(t *testing.T) {
		err := reorganizeGrowIndex(ctx, testLogger(), f.g, t.TempDir(), true)
		require.ErrorContains(t, err, "lays them out again itself")
	})
	slotOf := slices.Clone(f.g.slotOf)
	require.NoError(t, f.g.close(ctx))
	f.g = nil

	tests := []struct {
		name string
		// harm changes the files of a closed index, which held the slot of
		// every doc at its doc id's place of the table.
		harm func(t *testing.T, dir string)
		want string
	}{
		{
			name: "two docs share a slot",
			harm: func(t *testing.T, dir string) {
				file, err := os.OpenFile(filepath.Join(dir, "slots-1.u32"), os.O_RDWR, 0o644)
				require.NoError(t, err)
				_, err = file.WriteAt(entry(slotOf[3]), 4*5)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			},
			want: "and so does another doc",
		},
		{
			name: "a doc of the map has no slot",
			harm: func(t *testing.T, dir string) {
				file, err := os.OpenFile(filepath.Join(dir, "slots-1.u32"), os.O_RDWR, 0o644)
				require.NoError(t, err)
				_, err = file.WriteAt(make([]byte, 4), 4*5)
				require.NoError(t, err)
				require.NoError(t, file.Close())
			},
			want: "doc 5 is in the map and has no vector in the file",
		},
		{
			name: "the file of vectors does not reach the last slot",
			harm: func(t *testing.T, dir string) {
				require.NoError(t, os.Truncate(filepath.Join(dir, "vectors-1.f32"), 4*dims*(base+5)))
			},
			want: "the file of vectors takes",
		},
		{
			name: "a slot for a doc id the index has no room for",
			harm: func(t *testing.T, dir string) {
				file, err := os.OpenFile(filepath.Join(dir, "slots-1.u32"), os.O_RDWR, 0o644)
				require.NoError(t, err)
				_, err = file.WriteAt(entry(5000), 4*(total+7))
				require.NoError(t, err)
				require.NoError(t, file.Close())
			},
			want: "the index has room for",
		},
		{
			name: "the table of slots is missing",
			harm: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(filepath.Join(dir, "slots-1.u32")))
			},
			want: "read where the vectors are",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.CopyFS(dir, os.DirFS(f.dir)))
			tt.harm(t, dir)
			_, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{keepPending: true})
			require.ErrorContains(t, err, tt.want)
		})
	}
	// The files the cases copied open.
	c, err := openGrowIndex(ctx, testLogger(), f.dir, f.provider, f.uc, growOpen{keepPending: true})
	require.NoError(t, err)
	require.NoError(t, c.close(ctx))

	t.Run("vectors at their doc ids are not laid out again", func(t *testing.T) {
		f := newGrowFixture(t, base, base, dims, false, 64, 60)
		require.False(t, f.g.layoutDue())
		require.ErrorContains(t, f.g.layOut(), "at their doc ids")
		_, err := f.g.beginLayout()
		require.ErrorContains(t, err, "at their doc ids")
	})
}

// The vectors are due to be laid out again when as many have come in since
// the last time as were laid out then.
func TestGrowLayoutDue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const base, dims = 24, 32
	const total = 2*base + 1
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 400, shardFormat(8))
	f.rescoring(fewRescored)
	for f.inserted < 2*base-1 {
		require.False(t, f.g.layoutDue(), "with %d vectors", f.inserted)
		_, err := f.insert(ctx)
		require.NoError(t, err)
	}
	_, err := f.insert(ctx)
	require.NoError(t, err)
	require.True(t, f.g.layoutDue(), "the index has doubled")
	require.NoError(t, f.g.layOut())
	require.False(t, f.g.layoutDue())
	require.Equal(t, 2*base, f.g.meta.VectorsLaidOut)
	// What is due is known to an index that opens.
	f.reopen(t)
	require.Equal(t, 2*base, f.g.meta.VectorsLaidOut)
	require.False(t, f.g.layoutDue())
}

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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The halo of a posting, decoded: an entry is found by the doc id of its
// endpoint, of two entries for a doc id the later one counts, and the table
// grows with the entries without losing any.
func TestGrowHaloEntries(t *testing.T) {
	const words = 3
	code := func(seed uint64) []uint64 { return []uint64{seed, seed * 31, ^seed} }

	t.Run("an entry is found by its endpoint, and one that is not there is not", func(t *testing.T) {
		var h growHalo
		h.reset(words)
		require.Nil(t, h.find(7), "an empty halo")
		h.add(7, code(1))
		h.add(9, code(2))
		require.Equal(t, code(1), h.find(7))
		require.Equal(t, code(2), h.find(9))
		require.Nil(t, h.find(8))
		require.True(t, h.has(7, code(1)))
		require.False(t, h.has(7, code(2)), "another code")
		require.False(t, h.has(8, code(1)), "no entry")
	})

	t.Run("of two entries for an endpoint the later one counts", func(t *testing.T) {
		var h growHalo
		h.reset(words)
		h.add(7, code(1))
		h.add(9, code(2))
		h.add(7, code(3))
		require.Equal(t, code(3), h.find(7))
		require.False(t, h.has(7, code(1)), "the code of the entry that came first")
		require.Equal(t, code(2), h.find(9))
		require.Len(t, h.slots, 3, "the first entry is still there")
	})

	t.Run("the table grows with the entries", func(t *testing.T) {
		var h growHalo
		h.reset(words)
		// Doc ids that fall in the same places of a small table.
		for i := range uint32(3000) {
			h.add(i*64, code(uint64(i)+1))
		}
		for i := range uint32(3000) {
			require.Equal(t, code(uint64(i)+1), h.find(i*64), "entry %d", i)
			require.Nil(t, h.find(i*64+1))
		}
		// The halo is used again for a posting with fewer entries.
		h.reset(words)
		require.Nil(t, h.find(64))
		h.add(64, code(5))
		require.Equal(t, code(5), h.find(64))
		require.Nil(t, h.find(128))
	})

	t.Run("a row is its values in order, each with one entry or several", func(t *testing.T) {
		first := growHaloEntries([]uint32{4, 5, 6}, slices.Concat(code(1), code(2), code(3)), words)
		second := growHaloEntries([]uint32{5}, code(9), words)
		third := growHaloEntries([]uint32{8}, code(4), words)
		require.Len(t, first, 3*(4+8*words))
		var h growHalo
		require.NoError(t, h.decode([][]byte{first, second, third, third}, words))
		require.Equal(t, []uint32{4, 5, 6, 5, 8, 8}, h.slots)
		require.Equal(t, code(1), h.find(4))
		require.Equal(t, code(9), h.find(5), "the entry of the later value")
		require.Equal(t, code(3), h.find(6))
		require.Equal(t, code(4), h.find(8), "the same entry twice")

		var copied growHalo
		copied.copyFrom(&h)
		require.NoError(t, h.decode(nil, words))
		require.Nil(t, h.find(4), "a row with no value")
		require.Equal(t, code(9), copied.find(5), "a copy does not follow what it was copied from")

		require.ErrorContains(t, h.decode([][]byte{first[:len(first)-1]}, words), "an entry takes")
	})

	t.Run("a halo written whole leaves out what came before it", func(t *testing.T) {
		before := growHaloEntries([]uint32{4, 5}, slices.Concat(code(1), code(2)), words)
		whole := growHaloWholeValue([]uint32{5, 6}, slices.Concat(code(7), code(3)), words)
		after := growHaloEntries([]uint32{8}, code(4), words)
		require.Len(t, whole, 3*(4+8*words), "the mark takes the place of an entry")
		var h growHalo
		require.NoError(t, h.decode([][]byte{before, whole, after}, words))
		require.Equal(t, []uint32{5, 6, 8}, h.slots)
		require.Nil(t, h.find(4), "an entry from before the halo was written whole")
		require.Equal(t, code(7), h.find(5))
		require.Equal(t, code(3), h.find(6))
		require.Equal(t, code(4), h.find(8), "an entry added after it")
		// The values it took out, which the store still returns until it
		// compacts them away, and a second whole one.
		empty := growHaloWholeValue(nil, nil, words)
		require.NoError(t, h.decode([][]byte{before, whole, after, before, empty, after}, words))
		require.Equal(t, []uint32{8}, h.slots)
		require.Nil(t, h.find(5))
	})

	t.Run("an endpoint without an entry cannot be scored", func(t *testing.T) {
		var h growHalo
		h.reset(words)
		h.add(7, code(1))
		err := h.score(nil, []uint32{3}, make([]float32, 1))
		require.ErrorContains(t, err, "is not in the halo")
	})
}

// haloAnswers returns what the index on disk answers to every query of the
// fixture.
func haloAnswers(t *testing.T, f *growFixture) (ids [][]uint64, dists [][]float32) {
	t.Helper()
	for i, query := range f.queries {
		found, at, err := f.g.search(context.Background(), query, 10, nil)
		require.NoError(t, err, "query %d", i)
		ids, dists = append(ids, found), append(dists, at)
	}
	return ids, dists
}

// An index whose postings carry their halo is the same index with its codes
// in memory and without them: opened one way and the other over the same
// files it answers the same, by the same work as its twin in memory, and what
// it writes one way is an index the other way. Without its codes in memory
// it holds none, reads those its writes need from their bucket, and the halo
// its search left is the one its writes use; with them it reads no code and
// reads the halo of the posting of every insert.
func TestGrowHaloWithAndWithoutCodes(t *testing.T) {
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

// checkCoverage tells when the halo of a posting is not what its edges need,
// and when an index that has none holds one. Every break is made in the
// store, behind the index's back.
func TestGrowHaloCoverageCatches(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1300, 1000, 64
	// member returns a member of a live posting with edges, and its place.
	member := func(t *testing.T, g *growIndex) (p uint32, docID uint64, edges []uint32) {
		t.Helper()
		var dp diskPosting
		for p = range uint32(g.meta.Postings) {
			if g.retired[p] {
				continue
			}
			require.NoError(t, g.fetchMembers(p, &dp))
			require.NoError(t, g.fetchLists(p, &dp))
			require.NoError(t, g.joinLists(p, &dp))
			for i, docID := range dp.docIDs {
				if edges := dp.edgesOfMember(i); len(edges) > 1 {
					return p, docID, slices.Clone(edges)
				}
			}
		}
		require.Fail(t, "no member with edges")
		return 0, 0, nil
	}
	tests := []struct {
		name   string
		format growConfig
		// harm breaks the store and returns what the check must say.
		harm func(t *testing.T, g *growIndex) string
	}{
		{name: "an edge to an endpoint the halo does not have", format: haloFormat(8, false), harm: func(t *testing.T, g *growIndex) string {
			p, docID, edges := member(t, g)
			var halo growHalo
			_, err := g.fetchHalo(p, &halo)
			require.NoError(t, err)
			stranger := uint32(0)
			for g.st.side.postingOf[stranger] == p || halo.find(stranger) != nil {
				stranger++
			}
			dists, _, err := g.distsOf(p, docID)
			require.NoError(t, err)
			updated := append(slices.Clone(edges[:len(edges)-1]), stranger)
			require.NoError(t, g.edgeStore.replace(p, docID, edges, updated, dists))
			return "which is not in the halo of the posting"
		}},
		{name: "a code in the halo that is not the one of its endpoint", format: haloFormat(8, false), harm: func(t *testing.T, g *growIndex) string {
			p, _, edges := member(t, g)
			words := g.meta.RoutingCodeWords
			entry := growHaloEntries(edges[:1], make([]uint64, words), words)
			require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockHalo), [][]byte{entry}))
			return "has another code for doc"
		}},
		{name: "the same, with the codes in memory", format: haloFormat(8, true), harm: func(t *testing.T, g *growIndex) string {
			p, _, edges := member(t, g)
			words := g.meta.RoutingCodeWords
			entry := growHaloEntries(edges[:1], make([]uint64, words), words)
			require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockHalo), [][]byte{entry}))
			return "has another code for doc"
		}},
		{name: "a halo in an index that has none", format: shardFormat(8), harm: func(t *testing.T, g *growIndex) string {
			p, _, edges := member(t, g)
			words := g.meta.RoutingCodeWords
			entry := growHaloEntries(edges[:1], make([]uint64, words), words)
			require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockHalo), [][]byte{entry}))
			return "of an index without halos"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, tt.format)
			for f.inserted < total {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
			}
			require.NoError(t, f.g.cleanCentroidGraph())
			require.NoError(t, f.g.checkCoverage())
			want := tt.harm(t, f.g)
			require.ErrorContains(t, f.g.checkCoverage(), want)
			// The store is left as it is: the fixture only closes it.
		})
	}
}

// haloEntries returns, for a live posting, the entries its halo holds as it
// is read and the endpoints the edges of its members lead to, each once.
func haloEntries(t *testing.T, g *growIndex, p uint32) (held, needed int) {
	t.Helper()
	var dp diskPosting
	require.NoError(t, g.fetchMembers(p, &dp))
	require.NoError(t, g.fetchLists(p, &dp))
	require.NoError(t, g.joinLists(p, &dp))
	_, err := g.fetchHalo(p, &dp.halo)
	require.NoError(t, err)
	endpoints := map[uint32]struct{}{}
	for i := range dp.docIDs {
		for _, endpoint := range dp.edgesOfMember(i) {
			endpoints[endpoint] = struct{}{}
		}
	}
	return len(dp.halo.slots), len(endpoints)
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
func TestGrowHaloIsWrittenWholeAgain(t *testing.T) {
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
func TestGrowHaloThroughBatches(t *testing.T) {
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

// An index is opened with no codes in memory only if its postings carry
// their halo, and they carry it only in the format of a shard.
func TestGrowHaloErrors(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1100, 1000, 64
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, shardFormat(8))
	require.False(t, f.g.haloed())
	require.NoError(t, f.g.close(ctx))
	f.g = nil
	how := f.how
	how.noCodes = true
	_, err := openGrowIndex(ctx, testLogger(), f.dir, f.provider, f.uc, how)
	require.ErrorContains(t, err, "do not carry their halo")
	f.open(t)
	f.same(t, "opened as it was written")

	st := f.index.state.Load()
	vectorOf := func(id uint64) []float32 { return f.all[id] }
	for _, format := range []growConfig{{halo: true}, {halo: true, consolidate: 8}} {
		format.keys, format.neighbors, format.threshold = total, f.uc.Edges, 60
		err := writeGrowIndex(ctx, testLogger(), t.TempDir(), st, format, f.provider, vectorOf)
		require.ErrorContains(t, err, "carry their halo only with their rows of edges next to their members")
	}
}

// A doc id that left the index for good and is given again, with another
// vector, has another routing code, and postings that had an edge to the
// vector it was hold an entry with the old one. When an edge of such a
// posting leads to it again its new code goes into the halo, after the old
// one, and it is the one that counts: the index is covered, and it answers
// with no codes in memory what it answers with them.
func TestGrowHaloOfADocGivenAgain(t *testing.T) {
	ctx := context.Background()
	const total, base, dims, threshold = 1500, 1000, 48, 60
	f := newCleanFixtureFormat(t, total, base, dims, false, threshold, haloFormat(8, false))
	f.insertSome(t, 300)
	g := f.g

	// Every sixth vector leaves, for good.
	var gone []uint64
	for docID := range g.live {
		if g.holds(uint64(docID)) && docID%6 == 0 {
			gone = append(gone, uint64(docID))
		}
	}
	for _, docID := range gone {
		f.mark(t, docID)
	}
	require.NoError(t, f.d.eliminateSubstituting(ctx, 0, 0))
	require.NoError(t, g.clean(ctx, 0, 0))
	f.same(t, "cleaned")

	// Their doc ids come back with vectors near the ones they had, so that
	// the postings around them get edges to them again. The twin does not
	// follow from here on.
	rng := rand.New(rand.NewPCG(17, 5))
	again := make(map[uint64][]float32, len(gone))
	for _, docID := range gone {
		vector := slices.Clone(f.all[docID])
		for i := range vector {
			vector[i] += float32(rng.NormFloat64()) * 0.05
		}
		again[docID] = vector
		require.NoError(t, g.insert(ctx, docID, vector), "doc %d again", docID)
	}
	require.NoError(t, g.cleanCentroidGraph())
	require.NoError(t, g.checkCoverage())

	// The case the test is for did happen: a halo with two codes for one
	// endpoint, the later of them the one the index keeps for it now.
	conflicts := 0
	var halo growHalo
	words := g.meta.RoutingCodeWords
	for p := range uint32(g.meta.Postings) {
		if g.retired[p] {
			continue
		}
		_, err := g.fetchHalo(p, &halo)
		require.NoError(t, err)
		first := map[uint32][]uint64{}
		for i, endpoint := range halo.slots {
			code := halo.codes[i*words : (i+1)*words]
			if old, found := first[endpoint]; !found {
				first[endpoint] = code
			} else if !slices.Equal(old, code) {
				conflicts++
				kept, _, err := g.routingCode(uint64(endpoint))
				require.NoError(t, err)
				require.Equal(t, kept, halo.find(endpoint), "posting %d, doc %d: the code that counts is the one of the vector it is now", p, endpoint)
				_, given := again[uint64(endpoint)]
				require.True(t, given, "doc %d has two codes and was not given again", endpoint)
			}
		}
	}
	require.Positive(t, conflicts, "no posting holds the old code of a doc that was given again: the test tests nothing")
	t.Logf("%d docs given again, %d entries of a halo with a code older than the one that counts", len(gone), conflicts)

	// With its codes in memory the index reads no halo to search: it answers
	// the same.
	ids, dists := haloAnswers(t, f.growFixture)
	f.how.noCodes = false
	f.reopen(t)
	require.False(t, f.g.noCodes)
	require.NoError(t, f.g.cleanCentroidGraph())
	require.NoError(t, f.g.checkCoverage())
	resident, residentDists := haloAnswers(t, f.growFixture)
	assert.Equal(t, ids, resident, "the same docs with the codes in memory")
	assert.Equal(t, dists, residentDists, "at the same distances")
}

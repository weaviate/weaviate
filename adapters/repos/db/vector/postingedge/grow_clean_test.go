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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// cleanFixture is an index that grows on disk, in the format of a shard, and
// its twin in memory with the deletes of the experiments over it
// (deleteIndex): what one marks and takes out, so does the other.
type cleanFixture struct {
	*growFixture
	d *deleteIndex
	// marked counts the vectors marked deleted so far.
	marked int
}

// newCleanFixture is newGrowFixtureFormat for an index that deletes, with the
// full code in its sidecar: the distance between two codes of the twin is
// that of the full code.
func newCleanFixture(t *testing.T, total, base, dims int, cosine bool, threshold, consolidate int) *cleanFixture {
	t.Helper()
	return newCleanFixtureFormat(t, total, base, dims, cosine, threshold, shardFormat(consolidate))
}

// newCleanFixtureFormat is newCleanFixture with the format of the index on
// disk given: with the halo of its postings, and with or without its codes in
// memory.
func newCleanFixtureFormat(t *testing.T, total, base, dims int, cosine bool, threshold int, format growConfig) *cleanFixture {
	t.Helper()
	format.open.deletes = true
	f := &cleanFixture{growFixture: newGrowFixtureFormat(t, total, base, dims, cosine, 0, threshold, format)}
	f.d = newDeleteIndex(f.twin, 0, 0)
	// A marked vector goes on routing, in the twin as on disk (cell 1b).
	f.d.deletedAreSources(true)
	f.d.remember()
	return f
}

// mark deletes a vector in the twin and on disk: it stays in its posting.
func (f *cleanFixture) mark(t *testing.T, docID uint64) {
	t.Helper()
	require.NoError(t, f.d.remove(docID))
	marked, err := f.g.remove(docID)
	require.NoError(t, err)
	require.True(t, marked, "doc %d", docID)
	f.marked++
}

// markShare deletes a share of the vectors the index holds, drawn at random.
func (f *cleanFixture) markShare(t *testing.T, share float64, seed uint64) {
	t.Helper()
	var held []uint64
	for docID := range f.g.live {
		if f.g.holds(uint64(docID)) {
			held = append(held, uint64(docID))
		}
	}
	rng := rand.New(rand.NewPCG(seed, 31))
	rng.Shuffle(len(held), func(i, j int) { held[i], held[j] = held[j], held[i] })
	for _, docID := range held[:int(share*float64(len(held)))] {
		f.mark(t, docID)
	}
}

// same checks that the index on disk is its twin: the coverage of the one on
// disk, which vectors each holds, every posting with its members in order,
// their codes and their edges, and what the two return for every query, by
// the same work.
func (f *cleanFixture) same(t *testing.T, at string) {
	t.Helper()
	ctx := context.Background()
	g, s := f.g, f.twin
	require.NoError(t, g.cleanCentroidGraph(), at)
	require.NoError(t, g.checkCoverage(), at)
	lists := s.st.postings.lists
	require.Equal(t, len(lists), g.meta.Postings, at)
	require.Equal(t, s.retired, g.retired, "%s: the retired postings", at)
	held := 0
	for docID := range g.live {
		require.Equal(t, s.live[docID], g.holds(uint64(docID)), "%s: doc %d is held", at, docID)
		if g.holds(uint64(docID)) {
			held++
			require.Equal(t, s.st.side.postingOf[docID], g.st.side.postingOf[docID], "%s: the posting of doc %d", at, docID)
		}
	}
	require.Equal(t, s.liveCount, held, at)
	var dp diskPosting
	for p := range uint32(len(lists)) {
		if g.retired[p] {
			continue
		}
		l := &lists[p]
		require.NoError(t, g.fetchMembers(p, &dp), at)
		require.NoError(t, g.fetchLists(p, &dp), at)
		require.NoError(t, g.joinLists(p, &dp), at)
		require.Equal(t, l.docIDs, dp.docIDs, "%s: the members of posting %d", at, p)
		require.Equal(t, l.codes, dp.codes, "%s: the codes of posting %d", at, p)
		for i, docID := range dp.docIDs {
			require.True(t, slices.Equal(l.edgesOf(i), dp.edgesOfMember(i)),
				"%s: doc %d, in posting %d, has the edges %v on disk and %v in memory", at, docID, p, dp.edgesOfMember(i), l.edgesOf(i))
		}
	}
	for i, query := range f.queries {
		difference, err := growDifference(ctx, s, g, query, 10)
		require.NoError(t, err, "%s, query %d", at, i)
		require.Empty(t, difference, "%s, query %d", at, i)
	}
}

// insertSome adds the next vectors to the twin and to the index on disk, which
// must do the same with each: its posting, its edges and their work.
func (f *cleanFixture) insertSome(t *testing.T, n int) {
	t.Helper()
	ctx := context.Background()
	for range n {
		at := f.inserted
		difference, err := f.insert(ctx)
		require.NoError(t, err, "doc %d", at)
		require.Empty(t, difference, "doc %d", at)
	}
}

// The cleanup of the deleted vectors on disk is the pass the experiments
// measured in memory (cell B of v6.2), without its merges: the same vectors
// leave, every edge that led to one of them gets the same substitute or is
// taken out, and the two indexes are the same index after it, as they were
// before, with the vectors only marked. They go on being the same through the
// inserts that follow, which read the distances the pass stored for the
// substitutes, and through more rounds of deletes, inserts and passes, with
// the store flushed and the index closed and opened on the way.
// TestGrowCleanEqualsMemoryAtScale, of the bench harness, cleans larger
// indexes in more ways.
func TestGrowCleanEqualsMemory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name        string
		cosine      bool
		threshold   int
		consolidate int
		// rounds of: share of the vectors marked, a pass, and inserts, with
		// the store flushed after every fifteen of them if flush is set.
		rounds  int
		share   float64
		inserts int
		flush   bool
		// halo has the postings carry their halo, and the index no codes in
		// memory.
		halo bool
	}{
		{name: "30 % at once", threshold: 20, consolidate: 4, rounds: 1, share: 0.30, inserts: 30, flush: true},
		{
			name: "two rounds of 10 %, cosine, rows only at splits", cosine: true, threshold: 20, consolidate: DefaultConsolidate,
			rounds: 2, share: 0.10, inserts: 15,
		},
		{name: "no codes in memory, 30 % at once", threshold: 20, consolidate: 4, rounds: 1, share: 0.30, inserts: 30, flush: true, halo: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const base, grown, dims = 60, 30, 32
			total := base + grown + tt.rounds*tt.inserts
			format := shardFormat(tt.consolidate)
			if tt.halo {
				format = haloFormat(tt.consolidate, false)
			}
			f := newCleanFixtureFormat(t, total, base, dims, tt.cosine, tt.threshold, format)
			f.rescoring(fewRescored)
			f.queries = f.queries[:3]
			f.insertSome(t, grown)
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
				for start := 0; start < tt.inserts; start += 15 {
					f.insertSome(t, min(15, tt.inserts-start))
					if tt.flush {
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

// A posting that a cleanup leaves under mergeUnder members joins the one
// whose centroid is nearest, if the two fit in sumWithin: its members go to
// the end of that one with their codes and their lists, no edge changes, and
// it is retired. One left with no member is retired. What is left is an
// index: covered, searched, opened again, and grown.
func TestGrowCleanMerges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name string
		// whole deletes every member of some postings besides the share.
		share float64
		whole int
		// halo has the postings carry their halo, and the index no codes in
		// memory.
		halo bool
	}{
		{name: "30 % at random and three whole postings", share: 0.30, whole: 3},
		{name: "no codes in memory, 40 % at random", share: 0.40, halo: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const base, dims, threshold = 60, 32, 24
			total := base + 60
			under, within := mergeUnder(threshold), threshold
			format := shardFormat(8)
			if tt.halo {
				format = haloFormat(8, false)
			}
			f := newCleanFixtureFormat(t, total, base, dims, false, threshold, format)
			f.rescoring(fewRescored)
			f.queries = f.queries[:3]
			f.insertSome(t, 30)
			g := f.g
			f.markShare(t, tt.share, 5)
			emptied := 0
			for p := range uint32(g.meta.Postings) {
				if emptied == tt.whole {
					break
				}
				if g.retired[p] {
					continue
				}
				for docID := range g.live {
					if g.holds(uint64(docID)) && g.st.side.postingOf[docID] == p {
						marked, err := g.remove(uint64(docID))
						require.NoError(t, err)
						require.True(t, marked)
						f.marked++
					}
				}
				emptied++
			}
			// What every vector that stays has before the pass: its code, and
			// the edges it will keep.
			var dp diskPosting
			words := g.meta.ScanCodeWords
			codes := map[uint64][]uint64{}
			kept := map[uint64][]uint32{}
			sizes := map[uint32]int{}
			for p := range uint32(g.meta.Postings) {
				if g.retired[p] {
					continue
				}
				require.NoError(t, g.fetchMembers(p, &dp))
				require.NoError(t, g.fetchLists(p, &dp))
				require.NoError(t, g.joinLists(p, &dp))
				for i, docID := range dp.docIDs {
					if !g.holds(docID) {
						continue
					}
					sizes[p]++
					codes[docID] = slices.Clone(dp.codes[i*words : (i+1)*words])
					for _, endpoint := range dp.edgesOfMember(i) {
						if g.holds(uint64(endpoint)) {
							kept[docID] = append(kept[docID], endpoint)
						}
					}
				}
			}
			postings, live := g.meta.Postings, g.meta.Live-g.deletedCount
			retired := func() int {
				count := 0
				for _, gone := range g.retired {
					if gone {
						count++
					}
				}
				return count
			}
			retiredBefore := retired()

			require.NoError(t, g.clean(ctx, under, within))
			did := g.cleaned
			require.Equal(t, f.marked, did.Removed)
			require.Zero(t, g.deletedCount)
			require.Equal(t, live, g.meta.Live)
			require.Equal(t, postings, g.meta.Postings, "a merge gives out no posting")
			require.Positive(t, did.Merges, "postings were left under %d members", under)
			require.GreaterOrEqual(t, did.EmptyRetired, tt.whole)
			require.Equal(t, did.Merges+did.EmptyRetired, retired()-retiredBefore, "a posting retired for each that joined or emptied")
			require.NoError(t, g.cleanCentroidGraph())
			require.NoError(t, g.checkCoverage())

			// Every vector that stayed is in one posting, with its code and
			// the edges it kept in their order, and then its substitutes.
			moved, seen := 0, 0
			for p := range uint32(g.meta.Postings) {
				if g.retired[p] {
					continue
				}
				require.NoError(t, g.fetchMembers(p, &dp))
				require.NoError(t, g.fetchLists(p, &dp))
				require.NoError(t, g.joinLists(p, &dp))
				for i, docID := range dp.docIDs {
					seen++
					require.Equal(t, codes[docID], dp.codes[i*words:(i+1)*words], "the code of doc %d", docID)
					edges := dp.edgesOfMember(i)
					require.GreaterOrEqual(t, len(edges), len(kept[docID]), "doc %d", docID)
					require.True(t, slices.Equal(kept[docID], edges[:len(kept[docID])]),
						"doc %d kept the edges %v, and has %v", docID, kept[docID], edges)
				}
				// The members a merge brought are after those it had, and no
				// posting is left past what two that merge may add up to.
				require.LessOrEqual(t, len(dp.docIDs), within, "posting %d", p)
				if len(dp.docIDs) > sizes[p] {
					moved += len(dp.docIDs) - sizes[p]
				}
			}
			require.Equal(t, live, seen)
			require.Equal(t, did.Moved, moved, "the members the merges moved")

			f.vectorsIntact(t, "merged")
			answers := make([][]uint64, len(f.queries))
			for i, query := range f.queries {
				ids, _, err := g.search(ctx, query, 10, nil)
				require.NoError(t, err)
				for _, id := range ids {
					require.True(t, g.holds(id), "query %d returned doc %d, which left", i, id)
				}
				answers[i] = ids
			}
			f.reopen(t)
			g = f.g
			require.NoError(t, g.cleanCentroidGraph())
			require.NoError(t, g.checkCoverage())
			for i, query := range f.queries {
				ids, _, err := g.search(ctx, query, 10, nil)
				require.NoError(t, err)
				require.Equal(t, answers[i], ids, "query %d, opened again", i)
			}
			// It grows from there.
			for docID := f.inserted; docID < total; docID++ {
				require.NoError(t, g.insert(ctx, uint64(docID), f.all[docID]), "doc %d", docID)
			}
			require.NoError(t, g.cleanCentroidGraph())
			require.NoError(t, g.checkCoverage())
			// The twin was left behind when the merges started.
			f.twin.st.centroids = f.own
		})
	}
}

// A pass over the deleted vectors that stops between two postings leaves an
// index, wherever it stops: covered, searched without a vector that left, and
// cleaned whole by the pass that starts again. And a vector whose doc id is
// given again after it left is the new one.
func TestGrowCleanInSteps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// again is how many doc ids are given again at the end, to vectors past
	// those inserted.
	const base, inserts, again, dims, threshold = 60, 30, 10, 32, 24
	total := base + inserts + again
	f := newCleanFixture(t, total, base, dims, false, threshold, 8)
	f.rescoring(fewRescored)
	f.queries = f.queries[:3]
	f.insertSome(t, inserts)
	g := f.g
	for stopAfter := 1; ; stopAfter += 7 {
		// A new round of marks, and a pass that is dropped after some steps.
		var held []uint64
		for docID := range g.live {
			if g.holds(uint64(docID)) {
				held = append(held, uint64(docID))
			}
		}
		for i, docID := range held {
			if i%15 == stopAfter%15 {
				_, err := g.remove(docID)
				require.NoError(t, err)
			}
		}
		marked := g.deletedCount
		pass, err := g.beginClean(mergeUnder(threshold), threshold)
		require.NoError(t, err)
		done := false
		for steps := 0; steps < stopAfter && !done; steps++ {
			done, err = pass.step(ctx, 1)
			require.NoError(t, err)
		}
		require.NoError(t, g.cleanCentroidGraph())
		require.NoError(t, g.checkCoverage(), "stopped after %d steps", stopAfter)
		for i, query := range f.queries {
			ids, _, err := g.search(ctx, query, 10, nil)
			require.NoError(t, err)
			for _, id := range ids {
				require.True(t, g.holds(id), "query %d returned doc %d, which is deleted", i, id)
			}
		}
		if done {
			require.Zero(t, g.deletedCount)
			break
		}
		// The pass that starts again takes out what the one before left.
		left := g.deletedCount
		require.LessOrEqual(t, left, marked)
		require.NoError(t, g.clean(ctx, mergeUnder(threshold), threshold))
		require.Zero(t, g.deletedCount)
		require.NoError(t, g.cleanCentroidGraph())
		require.NoError(t, g.checkCoverage(), "cleaned after a pass that stopped at %d steps", stopAfter)
	}

	// A doc id that left is free: given again, it is the new vector, in the
	// index and in the file of vectors. Also when it leaves and comes back
	// under a pass that lays the vectors out, which had copied the old one.
	var given []uint64
	for docID := range g.live {
		if g.holds(uint64(docID)) && len(given) < again && docID%3 == 0 {
			given = append(given, uint64(docID))
		}
	}
	require.Len(t, given, again)
	layout, err := g.beginLayout()
	require.NoError(t, err)
	copied, err := layout.step(g.meta.Live)
	require.NoError(t, err)
	require.True(t, copied, "the pass copied every vector there was")
	for _, docID := range given {
		_, err := g.remove(docID)
		require.NoError(t, err)
	}
	require.NoError(t, g.clean(ctx, mergeUnder(threshold), threshold))
	for i, docID := range given {
		require.False(t, g.live[docID], "doc %d left", docID)
		f.all[docID] = f.all[total-1-i]
		require.NoError(t, g.insert(ctx, docID, f.all[docID]), "doc %d, given again", docID)
	}
	require.NoError(t, layout.finish())
	require.NoError(t, g.cleanCentroidGraph())
	require.NoError(t, g.checkCoverage())
	check := func(at string) {
		var raw []byte
		var vector []float32
		for docID, held := range f.g.live {
			if !held {
				continue
			}
			raw, vector, err = f.g.exact(uint64(docID), raw, vector)
			require.NoError(t, err)
			require.Equal(t, f.all[docID], vector, "%s: the vector of doc %d is the one it was given last", at, docID)
		}
	}
	check("laid out")
	f.reopen(t)
	check("opened again")
}

// The distance between two codes of the sidecar is the one the experiments
// used where the sidecar has the full code, and with the first bits of it an
// estimate of the same distance: it ranks a near vector before a far one.
func TestCodecBetween(t *testing.T) {
	t.Parallel()
	const dims = 256
	vectors := clusteredVectors(200, dims, 6, 991)
	provider := distancer.NewL2SquaredProvider()
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index, err := New(testConfig(t, vectors, 0, provider), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(context.Background(), sequentialIDs(len(vectors), 0), vectors))
	require.NoError(t, index.Build(context.Background()))
	full := index.state.Load().quantizer
	for _, bits := range []int{0, 128, 64} {
		codec := full
		if bits > 0 {
			codec, err = full.withBits(bits)
			require.NoError(t, err)
		}
		codes := make([][]uint64, len(vectors))
		for i, vector := range vectors {
			codes[i] = codec.Encode(vector)
		}
		// Of every vector, the nearest and the farthest of a few others: the
		// estimate must tell them apart most of the time.
		agree, pairs := 0, 0
		for i := 0; i+8 < len(vectors); i += 8 {
			near, far := i+1, i+1
			var nearDist, farDist float32
			for j := i + 1; j < i+8; j++ {
				dist, err := provider.SingleDist(vectors[i], vectors[j])
				require.NoError(t, err)
				if j == i+1 || dist < nearDist {
					near, nearDist = j, dist
				}
				if j == i+1 || dist > farDist {
					far, farDist = j, dist
				}
			}
			if near == far {
				continue
			}
			toNear, err := codec.between(codes[i], codes[near])
			require.NoError(t, err)
			toFar, err := codec.between(codes[i], codes[far])
			require.NoError(t, err)
			pairs++
			if toNear < toFar {
				agree++
			}
		}
		require.Positive(t, pairs)
		assert.Greater(t, float64(agree)/float64(pairs), 0.9, "with %d bits, %d of %d pairs", bits, agree, pairs)
	}
}

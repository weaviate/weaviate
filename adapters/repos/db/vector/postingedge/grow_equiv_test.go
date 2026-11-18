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
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// growFixture is an index that grows on disk next to its twin in memory: the
// streaming index of v2.x over the same base, with the same configuration.
// They share one centroid graph, the one of the index on disk: the twin asks
// it and never changes it. A graph draws its levels at random, so two graphs
// grown apart would not be the same graph, and neither would the indexes.
type growFixture struct {
	g        *growIndex
	twin     *streamIndex
	index    *Index
	own      *centroidIndex // the graph the twin was built with
	all      [][]float32
	queries  [][]float32
	provider distancer.Provider
	uc       UserConfig
	dir      string
	inserted int
	// opened is what the twin had counted when the index on disk was last
	// opened: the index on disk counts from there.
	opened streamStats
	// written is what the twin wrote for its last insert.
	written struct {
		docID   uint64
		posting uint32
		edges   []uint32
	}
}

// newGrowFixture builds the base in batch over the first vectors, writes it
// in the format that grows and opens it, with its twin. bits is the code the
// endpoints are ranked by, 0 for the full code.
func newGrowFixture(t *testing.T, total, base, dims int, cosine bool, bits, threshold int) *growFixture {
	t.Helper()
	ctx := context.Background()
	f := &growFixture{
		all:      clusteredVectors(total, dims, 12, 4107),
		queries:  clusteredVectors(40, dims, 12, 4108),
		provider: distancer.NewL2SquaredProvider(),
		uc:       smallConfig(),
		inserted: base,
	}
	f.uc.Distance, f.uc.EF = "l2-squared", 48
	if cosine {
		f.provider = distancer.NewCosineDistanceProvider()
		f.uc.Distance = "cosine"
		for i, vector := range f.all {
			f.all[i] = distancer.Normalize(vector)
		}
	}
	vectorOf := func(id uint64) []float32 { return f.all[id] }
	index, err := New(testConfig(t, f.all, 0, f.provider), f.uc)
	require.NoError(t, err)
	f.index = index
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(base, 0), f.all[:base]))
	require.NoError(t, index.Build(ctx))
	st := index.state.Load()
	if bits > 0 {
		routing, err := st.quantizer.withBits(bits)
		require.NoError(t, err)
		st, err = haloState(ctx, testLogger(), st, routing, vectorOf)
		require.NoError(t, err)
		index.state.Store(st)
	}

	f.dir = t.TempDir()
	cfg := growConfig{keys: total, neighbors: f.uc.Edges, threshold: threshold, seed: 7}
	require.NoError(t, writeGrowIndex(ctx, testLogger(), f.dir, st, cfg, f.provider, vectorOf))
	f.twin, err = newStreamIndex(index, total, streamConfig{threshold: threshold, inverse: true, seed: 7, followGraph: true}, vectorOf)
	require.NoError(t, err)
	f.own = f.twin.st.centroids
	f.twin.onWrite = func(docID uint64, p uint32, edges []uint32) error {
		f.written.docID, f.written.posting, f.written.edges = docID, p, slices.Clone(edges)
		return nil
	}
	f.open(t)
	t.Cleanup(func() {
		if f.g != nil {
			require.NoError(t, f.g.close(context.Background()))
		}
		// The index shuts down the graph it was built with.
		f.twin.st.centroids = f.own
		require.NoError(t, index.Shutdown(context.Background()))
	})
	return f
}

// open opens the index on disk and gives its graph to the twin.
func (f *growFixture) open(t *testing.T) {
	t.Helper()
	g, err := openGrowIndex(context.Background(), testLogger(), f.dir, f.provider, f.uc, nil, false)
	require.NoError(t, err)
	g.work = &SearchStats{}
	f.g = g
	f.twin.st.centroids = g.st.centroids
	f.opened = f.twin.stats.clone()
}

// reopen closes the index on disk and opens it again from its files.
func (f *growFixture) reopen(t *testing.T) {
	t.Helper()
	require.NoError(t, f.g.close(context.Background()))
	f.g = nil
	f.open(t)
}

// insert adds the next vector to the twin and to the index on disk, and
// tells how they differ on it, "" if they do not: its posting, its edges and
// what the inserts add up to so far.
func (f *growFixture) insert(ctx context.Context) (string, error) {
	docID := uint64(f.inserted)
	f.inserted++
	// The twin goes first: it leaves the graph as it found it, and the index
	// on disk then sees the graph the twin saw.
	if err := f.twin.insert(ctx, docID, f.all[docID]); err != nil {
		return "", errors.Wrap(err, "insert in memory")
	}
	if err := f.g.insert(ctx, docID, f.all[docID]); err != nil {
		return "", errors.Wrap(err, "insert on disk")
	}
	// What the twin counted since the index on disk was opened.
	mem, disk := f.twin.stats.clone(), &f.g.stats
	mem.Splits -= f.opened.Splits
	mem.SplitsFailed -= f.opened.SplitsFailed
	mem.InverseEdges -= f.opened.InverseEdges
	mem.InverseChecks -= f.opened.InverseChecks
	mem.SearchLoads -= f.opened.SearchLoads
	mem.SearchRounds -= f.opened.SearchRounds
	switch {
	case f.written.docID != docID:
		return "the twin did not write the doc", nil
	case f.written.posting != f.g.lastPosting:
		return errors.Errorf("doc %d went to posting %d on disk, %d in memory", docID, f.g.lastPosting, f.written.posting).Error(), nil
	case !slices.Equal(f.written.edges, f.g.lastEdges):
		return errors.Errorf("doc %d got the edges %v on disk, %v in memory", docID, f.g.lastEdges, f.written.edges).Error(), nil
	case mem.Splits != disk.Splits || mem.SplitsFailed != disk.SplitsFailed:
		return errors.Errorf("after doc %d, %d splits and %d failed on disk, %d and %d in memory",
			docID, disk.Splits, disk.SplitsFailed, mem.Splits, mem.SplitsFailed).Error(), nil
	case mem.InverseEdges != disk.InverseEdges || mem.InverseChecks != disk.InverseChecks:
		return errors.Errorf("after doc %d, %d inverse edges of %d checks on disk, %d of %d in memory",
			docID, disk.InverseEdges, disk.InverseChecks, mem.InverseEdges, mem.InverseChecks).Error(), nil
	case mem.SearchLoads != disk.SearchLoads || mem.SearchRounds != disk.SearchRounds:
		return errors.Errorf("after doc %d, the searches of the inserts loaded %d postings in %d rounds on disk, %d in %d in memory",
			docID, disk.SearchLoads, disk.SearchRounds, mem.SearchLoads, mem.SearchRounds).Error(), nil
	}
	return "", nil
}

// growDifference searches one query in the twin and on disk and tells how
// the two differ, "" if they do not.
func growDifference(ctx context.Context, twin *streamIndex, g *growIndex, query []float32, k int) (string, error) {
	var memory SearchStats
	wantIDs, wantDists, err := twin.ix.search(ctx, query, k, nil, &memory)
	if err != nil {
		return "", errors.Wrap(err, "search in memory")
	}
	stats := &diskSearchStats{Work: &SearchStats{}}
	ids, dists, err := g.search(ctx, query, k, stats)
	if err != nil {
		return "", errors.Wrap(err, "search from disk")
	}
	switch {
	case !slices.Equal(wantIDs, ids):
		return errors.Errorf("ids %v from disk, %v in memory", ids, wantIDs).Error(), nil
	case !slices.Equal(wantDists, dists):
		return errors.Errorf("distances %v from disk, %v in memory", dists, wantDists).Error(), nil
	case diskWorkOf(&memory) != diskWorkOf(stats.Work):
		return errors.Errorf("work %+v from disk, %+v in memory", diskWorkOf(stats.Work), diskWorkOf(&memory)).Error(), nil
	}
	return "", nil
}

// same checks everything the two indexes can be compared by: the coverage of
// each, their state, and what they return for every query.
func (f *growFixture) same(t *testing.T, at string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, f.g.cleanCentroidGraph(), at)
	require.NoError(t, f.twin.checkCoverage(), at)
	require.NoError(t, f.g.checkCoverage(), at)
	require.NoError(t, f.g.sameAs(f.twin), at)
	for i, query := range f.queries {
		difference, err := growDifference(ctx, f.twin, f.g, query, 10)
		require.NoError(t, err, "%s, query %d", at, i)
		require.Empty(t, difference, "%s, query %d", at, i)
	}
}

// The index that grows on disk is the streaming index of v2.x with its
// postings in the store: grown by the same inserts, it is the same index at
// every step. The store is flushed and compacted on the way, so that rows
// are read across segments, lists that were replaced meet their tombstones
// in another segment, and the rows of split postings are dropped; and the
// index is closed and opened again in the middle, and goes on.
func TestGrowInsertsEqualMemory(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		cosine    bool
		bits      int
		threshold int
		// flushEvery and compactEvery are in inserts, 0 for never.
		flushEvery, compactEvery int
		reopenAt                 int
	}{
		{name: "the full code in memory", threshold: 60, flushEvery: 37},
		{name: "a sketch in memory", bits: 64, threshold: 60, flushEvery: 53, compactEvery: 211, reopenAt: 500},
		{name: "a sketch in memory, cosine", bits: 128, cosine: true, threshold: 60, flushEvery: 29, compactEvery: 97, reopenAt: 333},
		{name: "everything in the memtable", bits: 64, threshold: 60},
		{name: "few splits", bits: 64, threshold: 400, flushEvery: 41, compactEvery: 173},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 2200, 1000, 64
			f := newGrowFixture(t, total, base, dims, tt.cosine, tt.bits, tt.threshold)
			f.same(t, "the base")
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				done := f.inserted - base
				if tt.flushEvery > 0 && done%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
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

// growRowValues returns the values the store holds for a row.
func growRowValues(t *testing.T, g *growIndex, p uint32, block byte) [][]byte {
	t.Helper()
	values, err := g.postings.SetList(diskKey(p, block))
	require.NoError(t, err)
	return values
}

// After a compaction the store holds what the index is made of and nothing
// else: the rows of the postings that were split are gone, and so are the
// lists that were replaced, with their tombstones.
func TestGrowCompactionDropsWhatNothingReads(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 2000, 1000, 64
	f := newGrowFixture(t, total, base, dims, false, 64, 60)
	for f.inserted < total {
		difference, err := f.insert(ctx)
		require.NoError(t, err)
		require.Empty(t, difference)
		if f.inserted%61 == 0 {
			require.NoError(t, f.g.flushAll())
		}
	}
	g := f.g
	require.Positive(t, g.stats.Splits)
	require.Positive(t, g.stats.InverseEdges)
	before, err := sizeOnDisk(g.dir + "/" + diskStoreDir + "/" + growBucketPostings)
	require.NoError(t, err)
	rounds, err := g.compactAll()
	require.NoError(t, err)
	require.Positive(t, rounds)
	after, err := sizeOnDisk(g.dir + "/" + diskStoreDir + "/" + growBucketPostings)
	require.NoError(t, err)
	assert.Less(t, after, before, "the compaction makes the bucket smaller")

	retired, payload := 0, int64(0)
	for p := range uint32(g.meta.Postings) {
		members, lists := growRowValues(t, g, p, diskBlockMembers), growRowValues(t, g, p, diskBlockEdges)
		if g.retired[p] {
			retired++
			assert.Empty(t, members, "the members of retired posting %d", p)
			assert.Empty(t, lists, "the edges of retired posting %d", p)
			continue
		}
		assert.Len(t, members, int(g.size[p]), "posting %d", p)
		assert.Len(t, lists, int(g.size[p]), "posting %d has one list per member", p)
		for _, value := range append(members, lists...) {
			payload += int64(len(value))
		}
	}
	assert.Equal(t, g.stats.Splits, retired)
	// Every value is framed by 9 bytes, and a row by its key and 12 more:
	// what is left of the bucket is that and the index of its segment.
	values := int64(2 * total)
	assert.Less(t, after, payload+9*values+int64(g.meta.Postings)*2*64+64*1024,
		"the bucket takes %d bytes for %d of values", after, payload)
	f.same(t, "compacted")
}

// checkCoverage reads the store, and tells when what is there is not an
// index: every break below is made in the store, behind the index's back.
func TestGrowCoverageCatches(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1400, 1000, 64
	// A doc and its posting, the same in every case: the fixture is built
	// from the same vectors.
	tests := []struct {
		name string
		// harm breaks the store and returns what the check must say.
		harm func(t *testing.T, g *growIndex, docID uint64, p uint32) string
	}{
		{name: "a doc the map puts elsewhere", harm: func(t *testing.T, g *growIndex, docID uint64, p uint32) string {
			other := (p + 1) % uint32(g.meta.Postings)
			require.NoError(t, g.posts.Put(growDocKey(docID), growUint32Value(other)))
			return "the map on disk puts doc"
		}},
		{name: "a doc in two postings", harm: func(t *testing.T, g *growIndex, docID uint64, p uint32) string {
			other := (p + 1) % uint32(g.meta.Postings)
			for g.retired[other] {
				other = (other + 1) % uint32(g.meta.Postings)
			}
			code := make([]uint64, g.meta.ScanCodeWords)
			require.NoError(t, g.postings.SetAdd(diskKey(other, diskBlockMembers), [][]byte{growMemberValue(docID, code)}))
			require.NoError(t, g.postings.SetAdd(diskKey(other, diskBlockEdges), [][]byte{growEdgesValue(docID, nil)}))
			g.size[other]++
			return "which the map puts in"
		}},
		{name: "a member without a list of edges", harm: func(t *testing.T, g *growIndex, docID uint64, p uint32) string {
			for _, value := range growRowValues(t, g, p, diskBlockEdges) {
				if uint64(binary.LittleEndian.Uint32(value)) == docID {
					require.NoError(t, g.postings.SetDeleteSingle(diskKey(p, diskBlockEdges), slices.Clone(value)))
				}
			}
			return "lists of edges"
		}},
		{name: "a member with two lists of edges", harm: func(t *testing.T, g *growIndex, docID uint64, p uint32) string {
			require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{growEdgesValue(docID, []uint32{1, 2, 3})}))
			return "lists of edges"
		}},
		{name: "an edge into its own posting", harm: func(t *testing.T, g *growIndex, docID uint64, p uint32) string {
			var mate uint64
			for _, value := range growRowValues(t, g, p, diskBlockMembers) {
				if id := binary.LittleEndian.Uint64(value); id != docID {
					mate = id
				}
			}
			for _, value := range growRowValues(t, g, p, diskBlockEdges) {
				if uint64(binary.LittleEndian.Uint32(value)) == docID {
					require.NoError(t, g.postings.SetDeleteSingle(diskKey(p, diskBlockEdges), slices.Clone(value)))
				}
			}
			require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{growEdgesValue(docID, []uint32{uint32(mate)})}))
			return "an edge into its own posting"
		}},
		{name: "distances that do not match the edges", harm: func(t *testing.T, g *growIndex, docID uint64, _ uint32) string {
			require.NoError(t, g.dists.Put(growDocKey(docID), growDistsValue(make([]float32, 40))))
			return "bytes of distances"
		}},
		{name: "a routing code that changed", harm: func(t *testing.T, g *growIndex, docID uint64, _ uint32) string {
			require.NoError(t, g.routing.Put(growDocKey(docID), growCodeValue(make([]uint64, g.meta.RoutingCodeWords))))
			return "another routing code on disk"
		}},
		{name: "a live posting out of the graph", harm: func(t *testing.T, g *growIndex, _ uint64, p uint32) string {
			require.NoError(t, g.st.centroids.graph.Delete(uint64(p)))
			return "is not in the centroid graph"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newGrowFixture(t, total, base, dims, false, 64, 60)
			for f.inserted < total {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
			}
			require.NoError(t, f.g.cleanCentroidGraph())
			require.NoError(t, f.g.checkCoverage())
			// A doc that was inserted, in a posting with more than one member.
			docID := uint64(total - 1)
			p := f.g.st.side.postingOf[docID]
			for f.g.size[p] < 2 {
				docID--
				p = f.g.st.side.postingOf[docID]
			}
			want := tt.harm(t, f.g, docID, p)
			require.ErrorContains(t, f.g.checkCoverage(), want)
			// The store is left as it is: the fixture only closes it.
		})
	}
}

// An index is written from one built in batch, and opened only as what it
// is.
func TestGrowIndexErrors(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1200, 1000, 64
	f := newGrowFixture(t, total, base, dims, false, 64, 60)
	st := f.twin.st
	vectorOf := func(id uint64) []float32 { return f.all[id] }

	err := writeGrowIndex(ctx, testLogger(), t.TempDir(), st, growConfig{keys: total, neighbors: 16, threshold: 60}, f.provider, vectorOf)
	require.ErrorContains(t, err, "keyed by doc id already")
	require.ErrorIs(t, writeGrowIndex(ctx, testLogger(), t.TempDir(), nil, growConfig{}, f.provider, vectorOf), ErrNotBuilt)

	_, err = openGrowIndex(ctx, testLogger(), t.TempDir(), f.provider, f.uc, nil, false)
	require.ErrorContains(t, err, "read the meta")
	require.NoError(t, f.g.close(ctx))
	f.g = nil
	_, err = openGrowIndex(ctx, testLogger(), f.dir, distancer.NewCosineDistanceProvider(), f.uc, nil, false)
	require.ErrorContains(t, err, "was written for distance")
	f.open(t)
	require.ErrorContains(t, f.g.insert(ctx, uint64(base), make([]float32, dims+1)), "dimensions")
}

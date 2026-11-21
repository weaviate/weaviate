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
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
	// leveled opens the store with its leveled compaction.
	leveled bool
	// batchOfOne inserts every vector on disk as a batch of one (v5.2),
	// which is the same insert.
	batchOfOne bool
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
	return newGrowFixtureWith(t, total, base, dims, cosine, bits, threshold, 0)
}

// newGrowFixtureWith is newGrowFixture with consolidated edges when
// consolidate is positive: the changes a row of edges is written after.
func newGrowFixtureWith(t *testing.T, total, base, dims int, cosine bool, bits, threshold, consolidate int) *growFixture {
	t.Helper()
	return newGrowFixtureRows(t, total, base, dims, cosine, bits, threshold, consolidate, growRowsOwnBucket)
}

// growRows tells where an index with consolidated edges keeps its rows of
// edges.
type growRows int

const (
	// growRowsOwnBucket has them in a bucket of their own (v5.1).
	growRowsOwnBucket growRows = iota
	// growRowsInSet has them in the bucket of postings, next to the members,
	// and that bucket a Set (v5.3).
	growRowsInSet
	// growRowsInMap has them next to the members too, in a Map (v5.4).
	growRowsInMap
)

func (r growRows) String() string {
	switch r {
	case growRowsOwnBucket:
		return "rows in a bucket of their own"
	case growRowsInSet:
		return "rows next to the members, in a Set"
	case growRowsInMap:
		return "rows next to the members, in a Map"
	}
	return "rows nowhere"
}

// newGrowFixtureRows is newGrowFixtureWith, with the rows of edges where rows
// tells.
func newGrowFixtureRows(t *testing.T, total, base, dims int, cosine bool, bits, threshold, consolidate int, rows growRows,
) *growFixture {
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
	cfg := growConfig{
		keys: total, neighbors: f.uc.Edges, threshold: threshold, seed: 7, consolidate: consolidate,
		rowsWithMembers: rows != growRowsOwnBucket, mapRows: rows == growRowsInMap,
	}
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
	g, err := openGrowIndex(context.Background(), testLogger(), f.dir, f.provider, f.uc, growOpen{leveled: f.leveled})
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
	if f.batchOfOne {
		if err := f.g.insertBatch(ctx, []uint64{docID}, [][]float32{f.all[docID]}, 1); err != nil {
			return "", errors.Wrap(err, "insert on disk, as a batch of one")
		}
	} else if err := f.g.insert(ctx, docID, f.all[docID]); err != nil {
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

	_, err = openGrowIndex(ctx, testLogger(), t.TempDir(), f.provider, f.uc, growOpen{})
	require.ErrorContains(t, err, "read the meta")
	require.NoError(t, f.g.close(ctx))
	f.g = nil
	_, err = openGrowIndex(ctx, testLogger(), f.dir, distancer.NewCosineDistanceProvider(), f.uc, growOpen{})
	require.ErrorContains(t, err, "was written for distance")
	f.open(t)
	require.ErrorContains(t, f.g.insert(ctx, uint64(base), make([]float32, dims+1)), "dimensions")
}

// An index laid out again is the same index: every posting in one segment of
// a bucket written anew, and with that the exact vectors in posting order.
// With a value per list of edges, as v5.0 stores them, and with the rows of
// edges consolidated, as v5.1 does: the combined control lays that one out.
// With the rows next to the members, in a Set (v5.3) and in a Map (v5.4), and
// the copy of the index of a Set written over a Map: the gate of v5.4.
func TestGrowReorganized(t *testing.T) {
	tests := []struct {
		name        string
		consolidate int
		rows        growRows
	}{
		{name: "a value per list of edges"},
		{name: "the rows of edges consolidated", consolidate: 8},
		{name: "the rows of edges next to the members", consolidate: 8, rows: growRowsInSet},
		{name: "the rows of edges next to the members, in a Map", consolidate: 8, rows: growRowsInMap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { growReorganizedCase(t, tt.consolidate, tt.rows) })
	}
}

func growReorganizedCase(t *testing.T, consolidate int, rows growRows) {
	ctx := context.Background()
	const total, base, dims = 2000, 1000, 64
	withMembers := rows != growRowsOwnBucket
	f := newGrowFixtureRows(t, total, base, dims, false, 64, 60, consolidate, rows)
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
		require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), f.g, t.TempDir(), false, false), "waiting in memory")
		require.NoError(t, f.g.flushPending())
		require.Empty(t, f.g.pending)
	}
	f.same(t, "grown")
	before, err := os.ReadDir(filepath.Join(f.dir, diskStoreDir, growBucketPostings))
	require.NoError(t, err)

	tests := []struct {
		name  string
		order bool
		// toMap writes the copy over a Map, whatever the bucket of the index.
		toMap bool
	}{
		{name: "one segment"},
		{name: "one segment and the vectors in posting order", order: true},
	}
	if !withMembers {
		// Only an index with its rows next to the members goes over a Map.
		require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), f.g, t.TempDir(), true, true), "only if the index has its rows of edges next to the members")
	} else {
		tests = append(tests, struct {
			name  string
			order bool
			toMap bool
		}{name: "over a Map, and the vectors in posting order", order: true, toMap: true})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, reorganizeGrowIndex(ctx, testLogger(), f.g, dir, tt.order, tt.toMap))
			cell, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, growOpen{})
			require.NoError(t, err)
			defer func() { require.NoError(t, cell.close(ctx)) }()
			inMap := rows == growRowsInMap || tt.toMap
			require.Equal(t, inMap, cell.meta.MapRows)
			wantStrategy := lsmkv.StrategySetCollection
			if inMap {
				wantStrategy = lsmkv.StrategyMapCollection
			}
			require.Equal(t, wantStrategy, cell.postings.Strategy(), "the bucket of postings")

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
			require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), f.g, dir, tt.order, tt.toMap), "is not empty")

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
			require.ErrorContains(t, reorganizeGrowIndex(ctx, testLogger(), cell, t.TempDir(), false, false), "laid out again already")
		})
	}
}

// With consolidated edges (v5.1) the index is still the streaming index of
// v2.x at every insert, with the changes to its edges waiting in memory, and
// with everything written it is the same index in its store: the rows of
// edges are written whole after so many changes, when a posting splits and
// when the index closes, and nothing else writes an edge. With the rows in a
// bucket of their own (v5.1) and with them in the bucket of postings, next
// to the members: in a Set (v5.3), where writing a row again takes the old
// one out, and in a Map (v5.4), where the new row replaces it and a retired
// posting is taken out pair by pair.
func TestGrowConsolidatedEqualsMemory(t *testing.T) {
	for _, rows := range []growRows{growRowsOwnBucket, growRowsInSet, growRowsInMap} {
		t.Run(rows.String(), func(t *testing.T) { growConsolidatedCases(t, rows) })
	}
}

func growConsolidatedCases(t *testing.T, rows growRows) {
	ctx := context.Background()
	withMembers := rows != growRowsOwnBucket
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
			f := newGrowFixtureRows(t, total, base, dims, tt.cosine, tt.bits, tt.threshold, tt.k, rows)
			if tt.leveled {
				f.leveled = true
				f.reopen(t)
			}
			require.Equal(t, !withMembers, f.g.edges != nil, "the bucket of rows of edges")
			require.True(t, f.g.consolidated())
			f.same(t, "the base")
			var written, atClose int
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
					written += f.g.stats.RowWritesFull + f.g.stats.RowWritesSplit
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
				if rows == growRowsInSet {
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
			if rows == growRowsInMap && tt.reopenAt == 0 {
				// Every pair of a retired posting was taken out: its
				// members, more than the threshold, and its row.
				splits := int64(f.g.stats.Splits)
				assert.Equal(t, splits > 0, f.g.stats.RetireTombstones.N > 0)
				assert.GreaterOrEqual(t, f.g.stats.RetireTombstones.N, splits*int64(tt.threshold+2))
			} else if rows != growRowsInMap {
				assert.Zero(t, f.g.stats.RetireTombstones.N)
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
				assert.Positive(t, atClose+written, "rows are written")
			}
			// No list lives in the bucket of postings. With the rows of
			// edges next to the members, the key of the lists of a live
			// posting holds its row, one value. A retired one holds
			// nothing after the compaction, which skips its keys.
			for p := range uint32(f.g.meta.Postings) {
				if rows == growRowsInMap {
					// In a Map the key of the row of a live posting holds
					// one pair, the row, and that of its members a pair
					// each, keyed by their places. A retired one holds
					// nothing, compacted or not: its pairs were taken out.
					pairs, err := f.g.postings.MapList(ctx, diskKey(p, diskBlockEdges))
					require.NoError(t, err)
					members, err := f.g.postings.MapList(ctx, diskKey(p, diskBlockMembers))
					require.NoError(t, err)
					if f.g.retired[p] {
						require.Empty(t, pairs, "posting %d", p)
						require.Empty(t, members, "posting %d", p)
						continue
					}
					require.Len(t, pairs, 1, "posting %d", p)
					require.Equal(t, growRowPairKey, pairs[0].Key)
					var row growBlock
					require.NoError(t, row.decode(pairs[0].Value), "posting %d", p)
					require.Len(t, row.ids, int(f.g.size[p]), "posting %d", p)
					require.Len(t, members, int(f.g.size[p]), "posting %d", p)
					for at, member := range members {
						key, err := growMemberPairKey(at)
						require.NoError(t, err)
						require.Equal(t, key, member.Key, "member %d of posting %d", at, p)
					}
					continue
				}
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

// A row of edges as one value comes back as it was written, and tells what
// it is not.
func TestGrowEdgesBlock(t *testing.T) {
	tests := []struct {
		name  string
		ids   []uint64
		lists [][]uint32
	}{
		{name: "no member"},
		{name: "one member without edges", ids: []uint64{7}, lists: [][]uint32{nil}},
		{name: "members with and without edges", ids: []uint64{9, 3, 400000, 12}, lists: [][]uint32{{1, 2, 3}, nil, {5}, {8, 6, 7, 5, 3, 0, 9}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block, err := growEdgesBlock(tt.ids, tt.lists)
			require.NoError(t, err)
			var row growBlock
			require.NoError(t, row.decode(block))
			require.Len(t, row.ids, len(tt.ids))
			for i, id := range tt.ids {
				require.Equal(t, uint32(id), row.ids[i])
				require.ElementsMatch(t, tt.lists[i], row.edges[row.start[i]:row.start[i+1]])
			}
			for cut := 0; cut < len(block); cut++ {
				require.Error(t, row.decode(block[:cut]), "a row cut at %d bytes", cut)
			}
		})
	}
	_, err := growEdgesBlock([]uint64{1}, [][]uint32{make([]uint32, 256)})
	require.ErrorContains(t, err, "256 edges")
}

// A search over the index that grows tells what the reads of its rows took,
// the row of members and the row of edges apart, with one value per list and
// with the row of edges in one value.
func TestGrowSearchTimesItsRows(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		consolidate int
	}{
		{name: "one value per list"},
		{name: "the row of edges in one value", consolidate: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newGrowFixtureWith(t, 1300, 1000, 64, false, 64, 60, tt.consolidate)
			for f.inserted < 1300 {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
			}
			for i, query := range f.queries {
				var stats diskSearchStats
				_, _, err := f.g.search(ctx, query, 10, &stats)
				require.NoError(t, err)
				require.Positive(t, stats.Postings, "query %d", i)
				require.Positive(t, stats.MemberTime, "query %d", i)
				require.Positive(t, stats.EdgeTime, "query %d", i)
				require.Zero(t, stats.HaloTime, "query %d", i)
			}
		})
	}
}

// Counting what a fetch reads row by row changes nothing of a search, and
// what it counts by row adds up to what the store counted for the search:
// the reads are those of a fetch that asks for its rows all at once, whatever
// row goes first.
func TestGrowRowCount(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		consolidate int
		rows        growRows
	}{
		{name: "a value per list of edges"},
		{name: "the rows of edges consolidated", consolidate: 8},
		{name: "the rows of edges next to the members", consolidate: 8, rows: growRowsInSet},
		{name: "the rows of edges next to the members, in a Map", consolidate: 8, rows: growRowsInMap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims, k = 1600, 1000, 64, 10
			f := newGrowFixtureRows(t, total, base, dims, false, 64, 60, tt.consolidate, tt.rows)
			for f.inserted < total {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
				if f.inserted%47 == 0 {
					require.NoError(t, f.g.flushAll())
				}
			}
			// Everything in segments: what is read from a memtable is not
			// counted.
			require.NoError(t, f.g.flushPending())
			require.NoError(t, f.g.flushAll())
			require.NoError(t, f.g.close(ctx))
			metrics, err := diskStoreMetrics()
			require.NoError(t, err)
			g, err := openGrowIndex(ctx, testLogger(), f.dir, f.provider, f.uc, growOpen{metrics: metrics, pread: true})
			require.NoError(t, err)
			// The fixture closes the index it holds.
			f.g = g

			type answer struct {
				ids   []uint64
				dists []float32
			}
			// run searches every query and returns what came back, the
			// postings loaded and what the store counted meanwhile.
			run := func() ([]answer, int, uint64, uint64) {
				reads, bytes, err := diskStoreReads()
				require.NoError(t, err)
				answers, loads := make([]answer, len(f.queries)), 0
				for i, query := range f.queries {
					stats := &diskSearchStats{Work: &SearchStats{}}
					ids, dists, err := g.search(ctx, query, k, stats)
					require.NoError(t, err)
					answers[i] = answer{ids: ids, dists: dists}
					loads += stats.Postings
				}
				readsAfter, bytesAfter, err := diskStoreReads()
				require.NoError(t, err)
				return answers, loads, readsAfter - reads, bytesAfter - bytes
			}
			plain, loads, reads, bytes := run()
			require.Positive(t, loads)
			require.Positive(t, reads)

			for _, edgesFirst := range []bool{false, true} {
				count := &growRowCount{edgesFirst: edgesFirst, read: func() ([growCounters]uint64, error) {
					reads, bytes, err := diskStoreReads()
					return [growCounters]uint64{reads, bytes}, err
				}}
				g.rowCount = count
				counted, countedLoads, countedReads, countedBytes := run()
				g.rowCount = nil
				require.Equal(t, plain, counted, "the answers, with the rows counted")
				require.Equal(t, loads, countedLoads)
				require.Equal(t, loads, count.loads)
				require.Equal(t, reads, countedReads, "the store reads the same, one row at a time")
				require.Equal(t, bytes, countedBytes)
				members, edges := count.rows[diskBlockMembers], count.rows[diskBlockEdges]
				require.Equal(t, countedReads, members[0]+edges[0], "the reads of the two rows are those of the search")
				require.Equal(t, countedBytes, members[1]+edges[1])
				require.GreaterOrEqual(t, members[0], uint64(loads), "a load reads its row of members")
				require.GreaterOrEqual(t, edges[0], uint64(loads), "a load reads its row of edges")
				require.Positive(t, members[1])
				require.Positive(t, edges[1])
			}

			// A counter that fails stops the search.
			g.rowCount = &growRowCount{read: func() ([growCounters]uint64, error) {
				return [growCounters]uint64{}, errors.New("no counter")
			}}
			_, _, err = g.search(ctx, f.queries[0], k, &diskSearchStats{Work: &SearchStats{}})
			g.rowCount = nil
			require.ErrorContains(t, err, "no counter")
		})
	}
}

// With the rows of edges next to the members (v5.3) the key of the row of a
// posting holds one value. A store that holds two stops whatever reads the
// row: a search that loads the posting, and the write of its row. And the
// format is told apart when the index is written: it needs consolidated
// edges.
func TestGrowRowsNextToMembersStop(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1100, 1000, 64
	f := newGrowFixtureRows(t, total, base, dims, false, 64, 60, 4, growRowsInSet)
	g := f.g
	require.Nil(t, g.edges)
	require.True(t, g.meta.RowsWithMembers)
	require.False(t, g.meta.MapRows)
	require.NoDirExists(t, filepath.Join(f.dir, diskStoreDir, growBucketEdges))

	// A row written again leaves one row, and the count of what it took out.
	p := uint32(0)
	old, err := g.getRow(p)
	require.NoError(t, err)
	require.NotNil(t, old)
	var row growBlock
	require.NoError(t, row.decode(old))
	require.NoError(t, g.putRow(p, old, old), "the same row written again writes nothing")
	require.Zero(t, g.stats.RowTombstones.N)
	changed := slices.Clone(old)
	changed[len(changed)-1] ^= 1
	require.NoError(t, g.putRow(p, old, changed))
	require.Equal(t, int64(1), g.stats.RowTombstones.N)
	require.Equal(t, int64(len(old)), g.stats.RowTombstones.Bytes)
	got, err := g.getRow(p)
	require.NoError(t, err)
	require.Equal(t, changed, got)
	require.NoError(t, g.putRow(p, changed, old))
	got, err = g.getRow(p)
	require.NoError(t, err)
	require.Equal(t, old, got)
	f.same(t, "with the row of a posting written three times")

	// A second value under the key of the row: the store holds two.
	require.NoError(t, g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{changed}))
	_, err = g.getRow(p)
	require.ErrorContains(t, err, "STOP: the store holds 2 rows of edges for posting 0")
	var dp diskPosting
	require.ErrorContains(t, g.fetchLists(p, &dp), "STOP: the store holds 2 rows")
	require.NoError(t, g.postings.SetDeleteSingle(diskKey(p, diskBlockEdges), changed))
	got, err = g.getRow(p)
	require.NoError(t, err)
	require.Equal(t, old, got)

	// The rows next to the members are rows: without consolidated edges
	// there is none.
	st := f.index.state.Load()
	err = writeGrowIndex(ctx, testLogger(), t.TempDir(), st, growConfig{
		keys: total, neighbors: f.uc.Edges, threshold: 60, seed: 7, rowsWithMembers: true,
	}, f.provider, func(id uint64) []float32 { return f.all[id] })
	require.ErrorContains(t, err, "only with consolidated edges")
}

// With the bucket of postings a Map (v5.4) the row of edges of a posting is
// one pair, and writing it again replaces it, with no tombstone, across
// flushes and compactions. A store that holds another pair under the key of
// the row stops whatever reads it, and so does a member that is not at its
// place. A retired posting leaves nothing to read. And the format is told
// apart when the index is written: it needs the rows next to the members.
func TestGrowMapRowsStop(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 1300, 1000, 64
	f := newGrowFixtureRows(t, total, base, dims, false, 64, 60, 4, growRowsInMap)
	g := f.g
	require.Nil(t, g.edges)
	require.True(t, g.meta.RowsWithMembers)
	require.True(t, g.meta.MapRows)
	require.Equal(t, lsmkv.StrategyMapCollection, g.postings.Strategy())
	require.NoDirExists(t, filepath.Join(f.dir, diskStoreDir, growBucketEdges))

	// A row written again leaves one row, the last, and no tombstone: in the
	// memtable, in a segment, in two, and compacted.
	p := uint32(0)
	old, err := g.getRow(p)
	require.NoError(t, err)
	require.NotNil(t, old)
	var row growBlock
	require.NoError(t, row.decode(old))
	changed := slices.Clone(old)
	changed[len(changed)-1] ^= 1
	// What the bucket takes on disk tells whether a row that was replaced is
	// still there: nothing reads the versions of a pair one by one.
	onDisk := func() int64 {
		size, err := sizeOnDisk(filepath.Join(f.dir, diskStoreDir, growBucketPostings))
		require.NoError(t, err)
		return size
	}
	_, err = g.compactAll()
	require.NoError(t, err)
	before := onDisk()
	require.NoError(t, g.putRow(p, old, old))
	require.NoError(t, g.flushAll())
	require.Equal(t, before, onDisk(), "the same row written again writes nothing")
	steps := []struct {
		name  string
		write []byte
		then  func() error
	}{
		{name: "in the memtable", write: changed, then: func() error { return nil }},
		{name: "flushed", write: old, then: g.flushAll},
		{name: "flushed again", write: changed, then: g.flushAll},
		{name: "compacted", write: old, then: func() error { _, err := g.compactAll(); return err }},
	}
	was := old
	for _, step := range steps {
		require.NoError(t, g.putRow(p, was, step.write), step.name)
		require.NoError(t, step.then(), step.name)
		got, err := g.getRow(p)
		require.NoError(t, err, step.name)
		require.Equal(t, step.write, got, step.name)
		was = step.write
	}
	require.Less(t, onDisk(), before+int64(len(old)), "compacted, the store holds the last row and none it replaced")
	require.Zero(t, g.stats.RowTombstones.N)
	f.same(t, "with the row of a posting written five times")

	// Another pair under the key of the row: the store holds two.
	stray := lsmkv.MapPair{Key: []byte{7}, Value: changed}
	require.NoError(t, g.postings.MapSet(diskKey(p, diskBlockEdges), stray))
	_, err = g.getRow(p)
	require.ErrorContains(t, err, "STOP: the store holds 2 pairs for the row of edges of posting 0")
	var dp diskPosting
	require.ErrorContains(t, g.fetchLists(p, &dp), "STOP: the store holds 2 pairs")
	// And one pair that is not the row's.
	require.NoError(t, g.postings.MapDeleteKey(diskKey(p, diskBlockEdges), growRowPairKey))
	_, err = g.getRow(p)
	require.ErrorContains(t, err, "STOP: the store holds 1 pairs for the row of edges of posting 0, the first with key 07")
	require.NoError(t, g.postings.MapDeleteKey(diskKey(p, diskBlockEdges), stray.Key))
	got, err := g.getRow(p)
	require.NoError(t, err)
	require.Nil(t, got, "with its pair taken out the posting has no row")
	require.NoError(t, g.putRow(p, nil, old))
	got, err = g.getRow(p)
	require.NoError(t, err)
	require.Equal(t, old, got)

	// A member that is not at its place: the first one taken out and one more
	// added at the end leave as many members, none of them at place 0.
	require.NoError(t, g.fetchMembers(p, &dp))
	n := len(dp.docIDs)
	firstKey, err := growMemberPairKey(0)
	require.NoError(t, err)
	members, err := g.postings.MapList(ctx, diskKey(p, diskBlockMembers))
	require.NoError(t, err)
	require.Len(t, members, n)
	require.Equal(t, firstKey, members[0].Key)
	first := slices.Clone(members[0].Value)
	lastKey, err := growMemberPairKey(n)
	require.NoError(t, err)
	require.NoError(t, g.postings.MapDeleteKey(diskKey(p, diskBlockMembers), firstKey))
	require.ErrorContains(t, g.fetchMembers(p, &dp), "the index counts")
	require.NoError(t, g.postings.MapSet(diskKey(p, diskBlockMembers), lsmkv.MapPair{Key: lastKey, Value: first}))
	require.ErrorContains(t, g.fetchMembers(p, &dp), "STOP: member 0 of posting 0 has the key 00 01 in the store, not its place")
	require.NoError(t, g.postings.MapDeleteKey(diskKey(p, diskBlockMembers), lastKey))
	require.NoError(t, g.postings.MapSet(diskKey(p, diskBlockMembers), lsmkv.MapPair{Key: firstKey, Value: first}))
	require.NoError(t, g.fetchMembers(p, &dp))
	f.same(t, "with the members of a posting as they were")

	// No pair of the format is written without a value, whatever the store
	// would make of one: an empty row and an empty member stop.
	require.ErrorIs(t, g.putRow(p, old, []byte{}), errEmptyMapValue)
	require.ErrorIs(t, g.putRow(p, nil, nil), errEmptyMapValue)
	require.ErrorIs(t, g.addMembers(p, n, [][]byte{first, {}}), errEmptyMapValue)
	require.NoError(t, g.fetchMembers(p, &dp), "nothing was written by the write that stopped")
	got, err = g.getRow(p)
	require.NoError(t, err)
	require.Equal(t, old, got)

	// A row that does not fit the value of a pair, and a member past the keys.
	require.ErrorContains(t, g.putRow(p, old, make([]byte, math.MaxUint16)), "takes less than 65535")
	_, err = growMemberPairKey(math.MaxUint16 + 1)
	require.ErrorContains(t, err, "holds up to 65535")
	_, err = growMemberPairKey(-1)
	require.Error(t, err)

	// Postings split, and of a retired one nothing is read, before any
	// compaction and after: its pairs were taken out one by one.
	for f.inserted < total {
		difference, err := f.insert(ctx)
		require.NoError(t, err)
		require.Empty(t, difference)
	}
	require.Positive(t, g.stats.Splits)
	require.NoError(t, g.flushPending())
	retired := 0
	nothingOfRetired := func(at string) {
		retired = 0
		for q := range uint32(g.meta.Postings) {
			if !g.retired[q] {
				continue
			}
			retired++
			for _, block := range []byte{diskBlockMembers, diskBlockEdges} {
				pairs, err := g.postings.MapList(ctx, diskKey(q, block))
				require.NoError(t, err)
				require.Empty(t, pairs, "%s: row %d of retired posting %d", at, block, q)
			}
		}
	}
	nothingOfRetired("in the memtable")
	require.Equal(t, int(g.stats.Splits), retired)
	require.GreaterOrEqual(t, g.stats.RetireTombstones.N, int64(retired*(60+2)), "a tombstone for every pair of a retired posting")
	require.NoError(t, g.flushAll())
	nothingOfRetired("flushed")
	_, err = g.compactAll()
	require.NoError(t, err)
	nothingOfRetired("compacted")
	f.same(t, "at the end")

	// The Map is of the format with the rows next to the members.
	st := f.index.state.Load()
	err = writeGrowIndex(ctx, testLogger(), t.TempDir(), st, growConfig{
		keys: total, neighbors: f.uc.Edges, threshold: 60, seed: 7, consolidate: 4, mapRows: true,
	}, f.provider, func(id uint64) []float32 { return f.all[id] })
	require.ErrorContains(t, err, "is a Map only with the rows of edges next to the members")
}

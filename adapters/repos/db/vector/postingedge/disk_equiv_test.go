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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// diskCopyNodeOp is the operation the store counts the read of a block under
// when it reads with pread.
const diskCopyNodeOp = "ReadFromSegmentcopyNode"

// diskStoreMetrics gives a store the metrics its reads are counted through.
// The store keeps one read observer per operation for the whole process, the
// first one asked for: a store that reads with pread must have metrics from
// its first read on, or no later one is counted.
func diskStoreMetrics() (*lsmkv.Metrics, error) {
	prom := *monitoring.GetMetrics()
	prom.Registerer = monitoring.NoopRegisterer
	return lsmkv.NewMetrics(&prom, "postingedge", "disk")
}

// diskStoreReads tells how many block reads the stores of the process have
// counted so far, and their bytes. The count is global: a caller takes the
// difference around its own reads, with no other store reading.
func diskStoreReads() (reads, bytes uint64, err error) {
	// The vec has one child per operation, a few dozen at most.
	ch := make(chan prometheus.Metric, 4096)
	monitoring.GetMetrics().FileIOReads.Collect(ch)
	close(ch)
	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	for _, m := range metrics {
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			return 0, 0, errors.Wrap(err, "read the store's read summary")
		}
		for _, label := range out.GetLabel() {
			if label.GetName() == "operation" && label.GetValue() == diskCopyNodeOp {
				reads += out.GetSummary().GetSampleCount()
				bytes += uint64(out.GetSummary().GetSampleSum())
			}
		}
	}
	return reads, bytes, nil
}

// diskFixture is a small index built in batch that cells are derived from.
type diskFixture struct {
	index    *Index
	built    *state
	provider distancer.Provider
	uc       UserConfig
	// raw are the vectors as they were added, which is what the rescore
	// reads, and stored as the index encodes them.
	raw, stored [][]float32
	queries     [][]float32
}

func newDiskFixture(t *testing.T, n, dims, clusters int, seed uint64, cosine bool) *diskFixture {
	ctx := context.Background()
	f := &diskFixture{
		raw:      clusteredVectors(n, dims, clusters, seed),
		queries:  clusteredVectors(40, dims, clusters, seed+1),
		provider: distancer.NewL2SquaredProvider(),
		uc:       smallConfig(),
	}
	f.uc.Distance = "l2-squared"
	f.stored = f.raw
	if cosine {
		f.provider = distancer.NewCosineDistanceProvider()
		f.uc.Distance = "cosine"
		f.stored = make([][]float32, n)
		for i, vector := range f.raw {
			f.stored[i] = distancer.Normalize(vector)
		}
	}
	index, err := New(testConfig(t, f.raw, 0, f.provider), f.uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), f.raw))
	require.NoError(t, index.Build(ctx))
	f.index, f.built = index, index.state.Load()
	return f
}

// cell derives the state of a cell from the build: the endpoints ranked by a
// sketch of bits bits, 0 for the full code, over the edges a pruning leaves.
func (f *diskFixture) cell(t *testing.T, bits int, onePer bool, degree int) *state {
	ctx := context.Background()
	st := f.built
	if bits > 0 {
		routing, err := f.built.quantizer.withBits(bits)
		require.NoError(t, err)
		st, err = haloState(ctx, testLogger(), f.built, routing, func(id uint64) []float32 { return f.stored[id] })
		require.NoError(t, err)
	}
	if !onePer && degree == 0 {
		return st
	}
	store := f.built.postings
	start, kept := pruneEdges(store.edgeStart, store.edges, f.built.side.postingOf, onePer, degree)
	require.Less(t, len(kept), len(store.edges), "the pruning must take edges out of the fixture")
	f.index.state.Store(st)
	defer f.index.state.Store(f.built)
	require.NoError(t, f.index.replaceEdges(start, kept))
	return f.index.state.Load()
}

// withCentroids is the state searched through another centroid graph.
func withCentroids(st *state, centroids *centroidIndex) *state {
	next := &state{
		dims:          st.dims,
		quantizer:     st.quantizer,
		scanQuantizer: st.scanQuantizer,
		centroids:     centroids,
		postings:      st.postings,
		side:          st.side,
		visited:       st.visited,
		build:         st.build,
	}
	next.scratch.New = st.scratch.New
	return next
}

// diskWork is the part of the stats of a search that says what it did.
type diskWork struct {
	Rounds, PostingsLoaded, VectorsScanned, ScannedAgain             int
	SourcesCut, EdgesFollowed, EdgesCut, EndpointsInLoaded, Rescored int
	EndpointsScored                                                  int
}

func diskWorkOf(s *SearchStats) diskWork {
	return diskWork{
		Rounds: s.Rounds, PostingsLoaded: s.PostingsLoaded, VectorsScanned: s.VectorsScanned,
		ScannedAgain: s.ScannedAgain, SourcesCut: s.SourcesCut, EdgesFollowed: s.EdgesFollowed, EdgesCut: s.EdgesCut,
		EndpointsInLoaded: s.EndpointsInLoaded, Rescored: s.Rescored, EndpointsScored: s.EndpointsScored,
	}
}

// diskDifference searches one query in memory and from disk and tells how
// the two differ, or "" when they return the same results with the same
// work. The index in memory must have the state of the cell, through the
// centroid graph of the index on disk, and the same search tunables.
func diskDifference(ctx context.Context, index *Index, disk *diskIndex, query []float32, k int,
) (string, *diskSearchStats, error) {
	var memory SearchStats
	wantIDs, wantDists, err := index.search(ctx, query, k, nil, &memory)
	if err != nil {
		return "", nil, errors.Wrap(err, "search in memory")
	}
	stats := &diskSearchStats{Work: &SearchStats{}}
	ids, dists, err := disk.search(ctx, query, k, stats)
	if err != nil {
		return "", nil, errors.Wrap(err, "search from disk")
	}
	switch {
	case !slices.Equal(wantIDs, ids):
		return errors.Errorf("ids %v from disk, %v in memory", ids, wantIDs).Error(), stats, nil
	case !slices.Equal(wantDists, dists):
		return errors.Errorf("distances %v from disk, %v in memory", dists, wantDists).Error(), stats, nil
	case diskWorkOf(&memory) != diskWorkOf(stats.Work):
		return errors.Errorf("work %+v from disk, %+v in memory", diskWorkOf(stats.Work), diskWorkOf(&memory)).Error(), stats, nil
	}
	return "", stats, nil
}

// Every cell searched from disk returns what its equivalent in memory
// returns: the same ids in the same order, the same distances and the same
// work. And what it read is what its postings hold.
func TestDiskSearchEqualsMemory(t *testing.T) {
	ctx := context.Background()
	const n, dims = 3000, 64

	cells := []struct {
		name   string
		cosine bool
		// bits is the sketch the endpoints are ranked by, 0 for the full
		// code, and halo tells that it lives in the postings.
		bits   int
		halo   bool
		onePer bool
		degree int
	}{
		{name: "the full code in memory"},
		{name: "the full code in memory, cosine", cosine: true},
		{name: "a sketch in memory", bits: 64},
		{name: "a sketch in memory, cosine", bits: 128, cosine: true},
		{name: "a halo", bits: 64, halo: true},
		{name: "a halo, cosine", bits: 128, halo: true, cosine: true},
		{name: "a halo over pruned edges", bits: 64, halo: true, onePer: true, degree: 4},
		{name: "a halo of full codes", halo: true},
		{name: "pruned edges, the full code in memory", onePer: true},
	}
	searches := []struct {
		name    string
		prepare func(uc *UserConfig)
		k       int
	}{
		{name: "the defaults of the tests", k: 10},
		{name: "a narrow search", prepare: func(uc *UserConfig) { uc.EF = 10 }, k: 10},
		{name: "a wide search", prepare: func(uc *UserConfig) { uc.EF, uc.RescoreLimit = 400, 400 }, k: 10},
		{name: "one result", prepare: func(uc *UserConfig) { uc.EF = 32 }, k: 1},
		{name: "no rescore", prepare: func(uc *UserConfig) { uc.RescoreLimit = 0 }, k: 10},
		{name: "ef under the rescore limit", prepare: func(uc *UserConfig) { uc.EF, uc.RescoreLimit = 16, 64 }, k: 10},
		{name: "rounds of one posting", prepare: func(uc *UserConfig) { uc.LoadBatch = 1 }, k: 10},
		{name: "rounds of eight postings", prepare: func(uc *UserConfig) { uc.LoadBatch, uc.EF = 8, 64 }, k: 10},
		{name: "three entry postings", prepare: func(uc *UserConfig) { uc.EntryPostings = 3 }, k: 10},
		{name: "a source margin", prepare: func(uc *UserConfig) { uc.SourceMargin, uc.EF = 0.5, 64 }, k: 10},
	}

	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			f := newDiskFixture(t, n, dims, 6, 801, cell.cosine)
			st := f.cell(t, cell.bits, cell.onePer, cell.degree)
			layout, err := haloPostings(st)
			require.NoError(t, err)
			dir := t.TempDir()
			meta, err := writeDiskIndex(ctx, testLogger(), dir, st, diskWriteOptions{Halo: cell.halo}, f.provider,
				func(id uint64) []float32 { return f.raw[id] })
			require.NoError(t, err)
			require.Equal(t, cell.halo, meta.Halo)
			require.Equal(t, cell.bits, meta.RoutingBits)

			for _, pread := range []bool{false, true} {
				name := "mapped"
				if pread {
					name = "pread"
				}
				t.Run(name, func(t *testing.T) {
					metrics, err := diskStoreMetrics()
					require.NoError(t, err)
					disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, metrics, pread)
					require.NoError(t, err)
					defer func() { require.NoError(t, disk.close(ctx)) }()
					// The index in memory enters through the graph the one
					// on disk was reopened with.
					f.index.state.Store(withCentroids(st, disk.st.centroids))
					defer f.index.state.Store(f.built)

					for _, search := range searches {
						t.Run(search.name, func(t *testing.T) {
							uc := f.uc
							if search.prepare != nil {
								search.prepare(&uc)
							}
							require.NoError(t, f.index.UpdateUserConfig(uc, func() {}))
							require.NoError(t, disk.ix.UpdateUserConfig(uc, func() {}))
							defer func() { require.NoError(t, f.index.UpdateUserConfig(f.uc, func() {})) }()

							several, rounds := 0, 0
							for i, query := range f.queries {
								// What the search loads, to count its blocks apart.
								var loaded []uint32
								readsBefore, bytesBefore, err := diskStoreReads()
								require.NoError(t, err)
								difference, stats, err := diskDifference(ctx, f.index, disk, query, search.k)
								require.NoError(t, err)
								require.Emptyf(t, difference, "query %d", i)
								readsAfter, bytesAfter, err := diskStoreReads()
								require.NoError(t, err)

								hook := &SearchStats{loadHook: func(sc *searchScratch, _ *posting, _ float32) {
									loaded = append(loaded[:0], sc.touched...)
								}}
								_, _, err = f.index.search(ctx, query, search.k, nil, hook)
								require.NoError(t, err)
								members, edges, entries := 0, 0, 0
								for _, p := range loaded {
									members += layout[p].Members
									edges += layout[p].Edges
									entries += layout[p].Distinct
								}
								require.Equal(t, len(loaded), stats.Postings)
								require.Nil(t, stats.Loaded)
								// The read of every block was timed, and of no
								// block the format does not have.
								require.Positive(t, stats.MemberTime)
								require.Positive(t, stats.EdgeTime)
								require.Equal(t, cell.halo, stats.HaloTime > 0)
								kept := &diskSearchStats{KeepLoaded: true}
								_, _, err = disk.search(ctx, query, search.k, kept)
								require.NoError(t, err)
								require.Equal(t, loaded, kept.Loaded, "the postings loaded, in order")
								require.Equal(t, stats.Work.PostingsLoaded, stats.Postings)
								require.Equal(t, members*8*(1+meta.ScanCodeWords), stats.MemberBytes)
								require.Equal(t, members+4*edges, stats.EdgeBytes)
								blocks := 2
								if cell.halo {
									blocks = 3
									require.Equal(t, entries*(4+8*meta.RoutingCodeWords), stats.HaloBytes)
								} else {
									require.Zero(t, stats.HaloBytes)
								}
								require.Equal(t, blocks*len(loaded), stats.Blocks)
								require.Equal(t, stats.MemberBytes+stats.EdgeBytes+stats.HaloBytes, stats.BlockBytes)
								if uc.RescoreLimit > 0 {
									require.Equal(t, min(max(uc.RescoreLimit, search.k), members), stats.Rescored)
								} else {
									require.Zero(t, stats.Rescored)
								}
								if pread {
									// One read per block, of the block and
									// what the store wraps it in: 13 bytes
									// and the key.
									require.Equal(t, uint64(stats.Blocks), readsAfter-readsBefore)
									require.Equal(t, uint64(stats.BlockBytes+stats.Blocks*(13+diskKeyBytes)), bytesAfter-bytesBefore)
								} else {
									require.Equal(t, readsBefore, readsAfter, "a mapped store counts no read")
								}
								if len(loaded) > 1 {
									several++
								}
								rounds += stats.Rounds
							}
							require.Positive(t, several, "no search loaded more than its entry posting")
							require.Positive(t, rounds)
						})
					}
				})
			}
		})
	}
}

// What a written index holds is what the state it was written from holds:
// every block decodes to its posting, and the files a reader loads into
// memory give back the arrays of the index.
func TestDiskFormat(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		cosine bool
		bits   int
		halo   bool
		degree int
	}{
		{name: "the full code in memory"},
		{name: "a sketch in memory", bits: 64, cosine: true},
		{name: "a halo", bits: 64, halo: true},
		{name: "a halo over capped edges", bits: 128, halo: true, degree: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDiskFixture(t, 2000, 64, 5, 811, tt.cosine)
			st := f.cell(t, tt.bits, false, tt.degree)
			store := st.postings
			dir := t.TempDir()
			meta, err := writeDiskIndex(ctx, testLogger(), dir, st, diskWriteOptions{Halo: tt.halo}, f.provider,
				func(id uint64) []float32 { return f.raw[id] })
			require.NoError(t, err)

			require.Equal(t, diskMeta{
				Version: diskFormatVersion, Distance: f.provider.Type(), Dims: 64, Vectors: 2000,
				Postings: store.postings(), Edges: len(store.edges), DocIDs: 2000,
				ScanCodeWords: store.codeWords, RoutingBits: tt.bits, RoutingCodeWords: st.side.codeWords,
				Halo: tt.halo, HaloEntries: meta.HaloEntries,
				MemberBytes: int64(2000 * 8 * (1 + store.codeWords)), EdgeBytes: int64(2000 + 4*len(store.edges)),
				HaloBytes: int64(meta.HaloEntries * (4 + 8*st.side.codeWords)),
			}, *meta)
			require.Positive(t, len(store.edges))
			if tt.bits > 0 {
				require.Equal(t, tt.bits/64+1, meta.RoutingCodeWords)
				require.Less(t, meta.RoutingCodeWords, meta.ScanCodeWords)
			}
			_, err = os.Stat(filepath.Join(dir, diskRoutingFile))
			if tt.halo {
				require.ErrorIs(t, err, os.ErrNotExist, "a halo keeps no routing code in memory")
				require.Positive(t, meta.HaloEntries)
			} else {
				require.NoError(t, err)
				require.Zero(t, meta.HaloEntries)
			}
			raw, err := os.ReadFile(filepath.Join(dir, diskMetaFile))
			require.NoError(t, err)
			var written diskMeta
			require.NoError(t, json.Unmarshal(raw, &written))
			require.Equal(t, *meta, written)

			disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
			require.NoError(t, err)
			defer func() { require.NoError(t, disk.close(ctx)) }()

			// What stays in memory.
			require.Equal(t, store.start, disk.st.postings.start)
			require.Equal(t, st.side.postingOf, disk.st.side.postingOf)
			require.Equal(t, st.side.codeWords, disk.st.side.codeWords)
			if tt.halo {
				require.Nil(t, disk.st.side.codes)
			} else {
				require.Equal(t, st.side.codes, disk.st.side.codes)
			}
			require.Len(t, disk.slotOf, 2000)
			for slot, id := range store.docIDs {
				require.Equal(t, uint32(slot), disk.slotOf[id])
			}
			require.Equal(t, st.centroids.vectors, disk.st.centroids.vectors)
			// The codecs are those of the index: they encode alike.
			for _, id := range []int{0, 1, 999, 1999} {
				require.Equal(t, st.quantizer.Encode(f.stored[id]), disk.st.quantizer.Encode(f.stored[id]), "doc %d", id)
				scan, diskScan := st.quantizer, disk.st.quantizer
				if tt.bits > 0 {
					scan, diskScan = st.scanQuantizer, disk.st.scanQuantizer
				} else {
					require.Nil(t, disk.st.scanQuantizer)
				}
				require.Equal(t, scan.Encode(f.stored[id]), diskScan.Encode(f.stored[id]), "doc %d", id)
			}

			// The exact vectors, in posting order.
			info, err := disk.vectors.Stat()
			require.NoError(t, err)
			require.Equal(t, int64(2000*64*4), info.Size())
			container := disk.ix.tempVectors.Get(64)
			defer disk.ix.tempVectors.Put(container)
			for _, id := range []uint64{0, 7, 1000, 1999} {
				vector, err := disk.readVector(ctx, id, container, nil)
				require.NoError(t, err)
				require.Equal(t, f.raw[id], vector, "doc %d", id)
			}
			_, err = disk.readVector(ctx, 2000, container, nil)
			require.ErrorContains(t, err, "does not hold it")

			// Every posting, block by block, fetched in rounds of several.
			round := &diskRound{}
			entries := 0
			for from := 0; from < store.postings(); from += 5 {
				var picks []scored
				for p := from; p < min(from+5, store.postings()); p++ {
					picks = append(picks, scored{id: uint64(p)})
				}
				require.NoError(t, disk.fetch(round, picks))
				for i, pick := range picks {
					want := store.load(uint32(pick.id))
					got := round.postings[i]
					require.Equal(t, want.first, got.post.first)
					require.Equal(t, want.docIDs, got.post.docIDs)
					require.Equal(t, want.codes, got.post.codes)
					require.True(t, slices.Equal(want.edges, got.post.edges), "posting %d", pick.id)
					for member := range want.docIDs {
						require.Truef(t, slices.Equal(want.edgesOf(member), got.post.edgesOf(member)),
							"member %d of posting %d", member, pick.id)
						require.Equal(t, want.key(member), got.post.key(member))
					}
					require.Equal(t, len(want.docIDs)*8*(1+store.codeWords), got.bytes[diskBlockMembers])
					require.Equal(t, len(want.docIDs)+4*len(want.edges), got.bytes[diskBlockEdges])
					if !tt.halo {
						require.Zero(t, got.bytes[diskBlockHalo])
						continue
					}
					endpoints := slices.Clone(want.edges)
					slices.Sort(endpoints)
					endpoints = slices.Compact(endpoints)
					require.True(t, slices.Equal(endpoints, got.haloSlots), "posting %d", pick.id)
					for at, slot := range got.haloSlots {
						require.Equal(t, st.side.code(slot),
							got.haloCodes[at*st.side.codeWords:(at+1)*st.side.codeWords], "endpoint %d", slot)
					}
					require.Equal(t, len(endpoints)*(4+8*st.side.codeWords), got.bytes[diskBlockHalo])
					entries += len(endpoints)
				}
			}
			require.Equal(t, meta.HaloEntries, entries)
		})
	}
}

// The graph over the centroids comes back from its commit log as it was
// written: the entry of a search does not depend on when the index was
// opened.
func TestDiskCentroidGraphReopens(t *testing.T) {
	ctx := context.Background()
	f := newDiskFixture(t, 3000, 64, 8, 821, false)
	dir := t.TempDir()
	_, err := writeDiskIndex(ctx, testLogger(), dir, f.built, diskWriteOptions{}, f.provider,
		func(id uint64) []float32 { return f.raw[id] })
	require.NoError(t, err)
	require.Greater(t, f.built.postings.postings(), 20)

	var first [][]uint64
	for round := 0; round < 3; round++ {
		disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
		require.NoError(t, err)
		var entries [][]uint64
		for _, query := range f.queries {
			ids, err := disk.st.centroids.nearest(ctx, query, 5)
			require.NoError(t, err)
			require.Len(t, ids, 5)
			entries = append(entries, ids)
		}
		require.NoError(t, disk.close(ctx))
		if round == 0 {
			first = entries
			continue
		}
		require.Equal(t, first, entries, "reopened %d times", round)
	}
	// It is a graph over the centroids of the index: the nearest centroid of
	// a centroid is itself.
	disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.close(ctx)) }()
	for p, centroid := range f.built.centroids.vectors {
		ids, err := disk.st.centroids.nearest(ctx, centroid, 1)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(p)}, ids)
	}
}

func TestDiskIndexErrors(t *testing.T) {
	ctx := context.Background()
	f := newDiskFixture(t, 1500, 24, 6, 831, false)
	vectorOf := func(id uint64) []float32 { return f.raw[id] }
	written := t.TempDir()
	_, err := writeDiskIndex(ctx, testLogger(), written, f.built, diskWriteOptions{}, f.provider, vectorOf)
	require.NoError(t, err)

	t.Run("writing", func(t *testing.T) {
		_, err := writeDiskIndex(ctx, testLogger(), t.TempDir(), nil, diskWriteOptions{}, f.provider, vectorOf)
		require.ErrorIs(t, err, ErrNotBuilt)
		_, err = writeDiskIndex(ctx, testLogger(), written, f.built, diskWriteOptions{}, f.provider, vectorOf)
		require.ErrorContains(t, err, "is not empty")
		_, err = writeDiskIndex(ctx, testLogger(), t.TempDir(), f.built, diskWriteOptions{}, f.provider,
			func(uint64) []float32 { return make([]float32, 3) })
		require.ErrorContains(t, err, "has length 3, expected 24")

		// Postings with codes of their own are not the full code.
		short := newDiskFixture(t, 1500, 128, 6, 833, false)
		routing, err := short.built.quantizer.withBits(64)
		require.NoError(t, err)
		capped := withCentroids(short.built, short.built.centroids)
		capped.scanQuantizer = routing
		_, err = writeDiskIndex(ctx, testLogger(), t.TempDir(), capped, diskWriteOptions{}, short.provider, vectorOf)
		require.ErrorContains(t, err, "the format writes the full code")
	})

	t.Run("an index keyed by doc id", func(t *testing.T) {
		stream := newDiskFixture(t, 1500, 24, 6, 835, false)
		_, err := newStreamIndex(stream.index, 1500, streamConfig{threshold: 60}, func(id uint64) []float32 { return stream.raw[id] })
		require.NoError(t, err)
		_, err = writeDiskIndex(ctx, testLogger(), t.TempDir(), stream.index.state.Load(), diskWriteOptions{}, stream.provider, vectorOf)
		require.ErrorContains(t, err, "keyed by doc id")
	})

	// copyIndex gives every case a directory of its own to break.
	copyIndex := func(t *testing.T) string {
		dir := t.TempDir()
		require.NoError(t, os.CopyFS(dir, os.DirFS(written)))
		return dir
	}
	editMeta := func(t *testing.T, dir string, edit func(*diskMeta)) {
		raw, err := os.ReadFile(filepath.Join(dir, diskMetaFile))
		require.NoError(t, err)
		var meta diskMeta
		require.NoError(t, json.Unmarshal(raw, &meta))
		edit(&meta)
		raw, err = json.Marshal(meta)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, diskMetaFile), raw, 0o644))
	}
	opening := []struct {
		name     string
		provider distancer.Provider
		breakIt  func(t *testing.T, dir string)
		wantErr  string
	}{
		{name: "no meta", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(filepath.Join(dir, diskMetaFile)))
		}, wantErr: "read the meta"},
		{name: "a meta that is not one", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.WriteFile(filepath.Join(dir, diskMetaFile), []byte("{"), 0o644))
		}, wantErr: "parse the meta"},
		{name: "another format version", breakIt: func(t *testing.T, dir string) {
			editMeta(t, dir, func(m *diskMeta) { m.Version = 99 })
		}, wantErr: "format version 99"},
		{name: "another distance", provider: distancer.NewCosineDistanceProvider(), wantErr: "written for distance"},
		{name: "an empty index", breakIt: func(t *testing.T, dir string) {
			editMeta(t, dir, func(m *diskMeta) { m.Vectors = 0 })
		}, wantErr: "meta of an empty index"},
		{name: "a layout cut short", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Truncate(filepath.Join(dir, diskLayoutFile), 100))
		}, wantErr: "read the layout"},
		{name: "no routing codes", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(filepath.Join(dir, diskRoutingFile)))
		}, wantErr: "read the routing codes"},
		{name: "centroids of another size", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Truncate(filepath.Join(dir, diskCentroidFile), 96))
		}, wantErr: "read the centroids"},
		{name: "vectors cut short", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Truncate(filepath.Join(dir, diskVectorFile), 96))
		}, wantErr: "the vectors take 96 bytes"},
		{name: "no vectors", breakIt: func(t *testing.T, dir string) {
			require.NoError(t, os.Remove(filepath.Join(dir, diskVectorFile)))
		}, wantErr: "open the vectors"},
		{name: "postings that do not span the vectors", breakIt: func(t *testing.T, dir string) {
			editMeta(t, dir, func(m *diskMeta) { m.Vectors, m.DocIDs = m.Vectors-1, m.DocIDs+1 })
		}, wantErr: "the postings span the slots"},
	}
	for _, tt := range opening {
		t.Run("opening: "+tt.name, func(t *testing.T) {
			dir := copyIndex(t)
			if tt.breakIt != nil {
				tt.breakIt(t, dir)
			}
			provider := f.provider
			if tt.provider != nil {
				provider = tt.provider
			}
			_, err := openDiskIndex(ctx, testLogger(), dir, provider, f.uc, nil, false)
			require.ErrorContains(t, err, tt.wantErr)
			// Nothing is left open: the same directory opens once mended.
		})
	}

	t.Run("searching", func(t *testing.T) {
		disk, err := openDiskIndex(ctx, testLogger(), copyIndex(t), f.provider, f.uc, nil, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, disk.close(ctx)) }()
		_, _, err = disk.search(ctx, f.queries[0], 0, nil)
		require.ErrorContains(t, err, "k must be positive")
		_, _, err = disk.search(ctx, f.queries[0][:5], 10, nil)
		require.ErrorContains(t, err, "query has length 5")
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err = disk.search(canceled, f.queries[0], 10, nil)
		require.ErrorIs(t, err, context.Canceled)
		// A search without stats returns what one with stats does.
		want, wantDists, err := disk.search(ctx, f.queries[1], 10, &diskSearchStats{Work: &SearchStats{}})
		require.NoError(t, err)
		ids, dists, err := disk.search(ctx, f.queries[1], 10, nil)
		require.NoError(t, err)
		require.Equal(t, want, ids)
		require.Equal(t, wantDists, dists)
	})

	t.Run("a block that is not what the layout says", func(t *testing.T) {
		dir := copyIndex(t)
		// The layout of a posting one member short of its blocks.
		raw, err := os.ReadFile(filepath.Join(dir, diskLayoutFile))
		require.NoError(t, err)
		postings := f.built.postings.postings()
		for p := 1; p < postings; p++ {
			raw[4*p]++ // every posting but the last starts one slot later
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, diskLayoutFile), raw, 0o644))
		disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, disk.close(ctx)) }()
		err = disk.fetch(&diskRound{}, []scored{{id: 0}})
		require.ErrorContains(t, err, "of posting 0 take")
	})
}

// scoreHalo gives the estimates the sidecar gives for the same endpoints, and
// refuses an endpoint its posting does not lead to.
func TestDiskScoreHalo(t *testing.T) {
	ctx := context.Background()
	f := newDiskFixture(t, 2000, 64, 5, 841, false)
	st := f.cell(t, 64, false, 0)
	dir := t.TempDir()
	_, err := writeDiskIndex(ctx, testLogger(), dir, st, diskWriteOptions{Halo: true}, f.provider, func(id uint64) []float32 { return f.raw[id] })
	require.NoError(t, err)
	disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.close(ctx)) }()

	round := &diskRound{}
	estimated := 0
	for p := 0; p < st.postings.postings(); p++ {
		require.NoError(t, disk.fetch(round, []scored{{id: uint64(p)}}))
		dp := &round.postings[0]
		if len(dp.haloSlots) == 0 {
			continue
		}
		estimator := st.quantizer.NewEstimator(f.queries[p%len(f.queries)])
		// In another order than the halo keeps them, and some twice.
		slots := slices.Clone(dp.haloSlots)
		slices.Reverse(slots)
		slots = append(slots, dp.haloSlots[0])
		want, got := make([]float32, len(slots)), make([]float32, len(slots))
		require.NoError(t, st.side.score(estimator, slots, want))
		require.NoError(t, dp.scoreHalo(estimator, st.side.codeWords, slots, got))
		require.Equal(t, want, got, "posting %d", p)
		estimated += len(slots)

		// A member of the posting is no endpoint of its edges.
		err := dp.scoreHalo(estimator, st.side.codeWords, []uint32{dp.post.first}, got[:1])
		require.ErrorContains(t, err, "is not in the halo")
	}
	require.Positive(t, estimated)
}

// An index written next to another of the same build shares its vectors, as
// one file, and its centroid graph, as a copy: the two enter every search
// through the same postings, and each still returns what its state in memory
// returns.
func TestDiskShareFrom(t *testing.T) {
	ctx := context.Background()
	f := newDiskFixture(t, 3000, 64, 6, 851, false)
	vectorOf := func(id uint64) []float32 { return f.raw[id] }
	first := t.TempDir()
	_, err := writeDiskIndex(ctx, testLogger(), first, f.built, diskWriteOptions{}, f.provider, vectorOf)
	require.NoError(t, err)
	base, err := openDiskIndex(ctx, testLogger(), first, f.provider, f.uc, nil, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, base.close(ctx)) }()

	tests := []struct {
		name   string
		bits   int
		halo   bool
		onePer bool
	}{
		{name: "a sketch in memory", bits: 64},
		{name: "a halo over pruned edges", bits: 64, halo: true, onePer: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := f.cell(t, tt.bits, tt.onePer, 0)
			dir := t.TempDir()
			// A vectorOf that fails: the vectors are not written again.
			_, err := writeDiskIndex(ctx, testLogger(), dir, st, diskWriteOptions{Halo: tt.halo, ShareFrom: first},
				f.provider, func(uint64) []float32 { return nil })
			require.NoError(t, err)
			mine, err := os.Stat(filepath.Join(dir, diskVectorFile))
			require.NoError(t, err)
			theirs, err := os.Stat(filepath.Join(first, diskVectorFile))
			require.NoError(t, err)
			require.True(t, os.SameFile(mine, theirs), "the vectors are one file")

			disk, err := openDiskIndex(ctx, testLogger(), dir, f.provider, f.uc, nil, false)
			require.NoError(t, err)
			defer func() { require.NoError(t, disk.close(ctx)) }()
			f.index.state.Store(withCentroids(st, disk.st.centroids))
			defer f.index.state.Store(f.built)
			for i, query := range f.queries {
				want, err := base.st.centroids.nearest(ctx, query, 4)
				require.NoError(t, err)
				got, err := disk.st.centroids.nearest(ctx, query, 4)
				require.NoError(t, err)
				require.Equal(t, want, got, "the entries of query %d", i)
				difference, _, err := diskDifference(ctx, f.index, disk, query, 10)
				require.NoError(t, err)
				require.Emptyf(t, difference, "query %d", i)
			}
		})
	}

	t.Run("nothing to share from", func(t *testing.T) {
		_, err := writeDiskIndex(ctx, testLogger(), t.TempDir(), f.built, diskWriteOptions{ShareFrom: t.TempDir()},
			f.provider, vectorOf)
		require.ErrorContains(t, err, "copy the centroid graph")
	})
}

// The index of the segment of a written index is a tail of its file, a few
// dozen bytes per block.
func TestWarmSegmentIndexes(t *testing.T) {
	ctx := context.Background()
	f := newDiskFixture(t, 2000, 64, 5, 861, false)
	dir := t.TempDir()
	meta, err := writeDiskIndex(ctx, testLogger(), dir, f.built, diskWriteOptions{}, f.provider,
		func(id uint64) []float32 { return f.raw[id] })
	require.NoError(t, err)

	read, err := warmSegmentIndexes(filepath.Join(dir, diskStoreDir))
	require.NoError(t, err)
	blocks := int64(2 * meta.Postings)
	// A node of the tree is the key, where its block starts and ends, and
	// two children: 36 bytes and the key.
	require.GreaterOrEqual(t, read, blocks*(36+diskKeyBytes))
	require.Less(t, read, blocks*(36+diskKeyBytes)+4096)
	require.Less(t, read, (meta.MemberBytes+meta.EdgeBytes)/10, "the index is a small part of the segment")

	// A directory without segments has nothing to read.
	read, err = warmSegmentIndexes(t.TempDir())
	require.NoError(t, err)
	require.Zero(t, read)
	// A file that is not a segment is refused.
	broken := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(broken, "segment-1.db"), make([]byte, 64), 0o644))
	_, err = warmSegmentIndexes(broken)
	require.ErrorContains(t, err, "the index starts at 0")
	require.NoError(t, os.WriteFile(filepath.Join(broken, "segment-1.db"), make([]byte, 4), 0o644))
	_, err = warmSegmentIndexes(broken)
	require.ErrorContains(t, err, "read the header")
}

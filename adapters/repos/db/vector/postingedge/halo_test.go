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
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// The halo of v3.0 (bench-report.md, "v3.0: punto de operación del halo"). A
// search ranks the postings it has not loaded by the estimated distance to
// the endpoints of the edges that lead to them, which it reads from the
// sidecar: the code of every vector, resident in memory. With a halo there
// is no sidecar. A posting carries, next to its members and their edges, a
// short code of every distinct endpoint of those edges, and the search ranks
// the postings with what the postings it loads bring along.
//
// Nothing of it is in the index. The experiment takes an index built in
// batch and gives it a state whose sidecar holds the short codes, while the
// postings keep the full codes for the scan: the search then does what it
// would do with a halo, and what it would read is counted from the postings
// it loads.

// haloState returns the state of an index built in batch with the endpoints
// of its edges ranked by another codec over the same quantizer. The
// postings, with the codes the scan reads, the edges, the centroid graph and
// the posting map are those of the index. The sidecar is a new one, with the
// code of every vector under routing. vectorOf gives the vector of a doc id
// as the index stores it.
func haloState(ctx context.Context, logger logrus.FieldLogger, st *state, routing codec,
	vectorOf func(docID uint64) []float32,
) (*state, error) {
	switch {
	case st == nil:
		return nil, ErrNotBuilt
	case st.postings.lists != nil:
		return nil, errors.New("the index is keyed by doc id: a halo is laid over an index built in batch")
	case st.scanQuantizer != nil:
		return nil, errors.New("the postings have codes of their own already")
	}
	store := st.postings
	n := store.vectors()
	if n == 0 {
		return nil, errors.New("no vector to encode")
	}
	words := len(routing.Encode(vectorOf(store.docIDs[0])))
	codes := make([]uint64, n*words)
	err := parallelChunks(ctx, logger, n, 1024, func(_, start, end int) error {
		for slot := start; slot < end; slot++ {
			code := routing.Encode(vectorOf(store.docIDs[slot]))
			if len(code) != words {
				return errors.Errorf("code of slot %d has %d words, expected %d", slot, len(code), words)
			}
			copy(codes[slot*words:], code)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "encode the endpoints")
	}
	halo := &state{
		dims:          st.dims,
		quantizer:     routing,
		scanQuantizer: st.quantizer,
		centroids:     st.centroids,
		postings:      store,
		side:          &sidecar{codeWords: words, codes: codes, postingOf: st.side.postingOf},
		visited:       visited.NewPool(n),
		build:         st.build,
	}
	postings := store.postings()
	halo.scratch.New = func() any { return newSearchScratch(postings) }
	return halo, nil
}

// haloPosting is what one posting holds, in the units its blocks on disk are
// made of: members, edges, and the distinct endpoints of those edges, which
// are the entries of its halo.
type haloPosting struct {
	Members  int
	Edges    int
	Distinct int
}

// haloPostings counts every posting of an index built in batch.
func haloPostings(st *state) ([]haloPosting, error) {
	store := st.postings
	if store.lists != nil {
		return nil, errors.New("the index is keyed by doc id")
	}
	out := make([]haloPosting, store.postings())
	var endpoints []uint32
	for p := range out {
		from, to := store.start[p], store.start[p+1]
		endpoints = append(endpoints[:0], store.edges[store.edgeStart[from]:store.edgeStart[to]]...)
		slices.Sort(endpoints)
		out[p] = haloPosting{
			Members: int(to - from), Edges: len(endpoints), Distinct: len(slices.Compact(endpoints)),
		}
	}
	return out, nil
}

// haloRead is what one search loaded, summed over its postings: what it would
// read from disk, block by block, and the work its stats report.
type haloRead struct {
	Loads   int
	Members int
	Edges   int
	// Entries counts the halo entries of the loaded postings: an endpoint
	// that two of them share is read twice.
	Entries int
	Stats   SearchStats
}

// haloSearch runs one search and tells what it loaded. It checks the postings
// its hook saw against what the search counted: as many as it loaded, with
// the members it scanned and the edges it met.
func haloSearch(ctx context.Context, index *Index, query []float32, k int, postings []haloPosting,
) ([]uint64, haloRead, error) {
	var loaded []uint32
	read := haloRead{}
	read.Stats.loadHook = func(sc *searchScratch, _ *posting, _ float32) {
		// Every posting of a round is marked before any of it is loaded:
		// the last call sees them all.
		loaded = append(loaded[:0], sc.touched...)
	}
	ids, _, err := index.search(ctx, query, k, nil, &read.Stats)
	if err != nil {
		return nil, haloRead{}, err
	}
	read.Stats.loadHook = nil
	for _, p := range loaded {
		read.Loads++
		read.Members += postings[p].Members
		read.Edges += postings[p].Edges
		read.Entries += postings[p].Distinct
	}
	stats := &read.Stats
	if read.Loads != stats.PostingsLoaded || read.Members != stats.VectorsScanned ||
		read.Edges != stats.EdgesFollowed+stats.EdgesCut {
		return nil, haloRead{}, errors.Errorf("the hook saw %d postings with %d members and %d edges, the search counted %d, %d and %d",
			read.Loads, read.Members, read.Edges, stats.PostingsLoaded, stats.VectorsScanned, stats.EdgesFollowed+stats.EdgesCut)
	}
	return ids, read, nil
}

// A state laid over the index with the codec of the index itself searches as
// the index does: the same results, distances and work. With a shorter code
// for the endpoints the scan is the same and the ranking of the postings is
// not.
func TestHaloState(t *testing.T) {
	const n = 3000
	ctx := context.Background()
	all := clusteredVectors(n, 64, 6, 701)
	queries := clusteredVectors(40, 64, 6, 702)
	provider := distancer.NewL2SquaredProvider()

	tests := []struct {
		name string
		// bits caps the code of the endpoints, 0 for the codec of the index.
		bits int
		// ef and rescore are the search.
		ef, rescore int
	}{
		{name: "the codec of the index", ef: 32, rescore: 100},
		{name: "the codec of the index, a wide search", ef: 400, rescore: 400},
		{name: "a sketch of 64 bits", bits: 64, ef: 32, rescore: 100},
		{name: "a sketch of 64 bits, a wide search", bits: 64, ef: 400, rescore: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.Distance, uc.EF, uc.RescoreLimit = "l2-squared", tt.ef, tt.rescore
			index, err := New(testConfig(t, all, 0, provider), uc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all))
			require.NoError(t, index.Build(ctx))
			built := index.state.Load()
			postings, err := haloPostings(built)
			require.NoError(t, err)

			type answer struct {
				ids   []uint64
				dists []float32
				read  haloRead
			}
			run := func() []answer {
				out := make([]answer, len(queries))
				for i, q := range queries {
					ids, read, err := haloSearch(ctx, index, q, 10, postings)
					require.NoError(t, err)
					_, dists, err := index.SearchByVector(ctx, q, 10, nil)
					require.NoError(t, err)
					read.Stats = SearchStats{
						Rounds: read.Stats.Rounds, PostingsLoaded: read.Stats.PostingsLoaded,
						VectorsScanned: read.Stats.VectorsScanned, EndpointsScored: read.Stats.EndpointsScored,
						EdgesFollowed: read.Stats.EdgesFollowed, EdgesCut: read.Stats.EdgesCut, Rescored: read.Stats.Rescored,
					}
					out[i] = answer{ids: ids, dists: dists, read: read}
				}
				return out
			}
			want := run()

			routing := built.quantizer
			if tt.bits > 0 {
				routing, err = built.quantizer.withBits(tt.bits)
				require.NoError(t, err)
			}
			halo, err := haloState(ctx, testLogger(), built, routing, func(id uint64) []float32 { return all[id] })
			require.NoError(t, err)

			// Everything but the sidecar is the index's own.
			require.Same(t, built.postings, halo.postings)
			require.Same(t, built.centroids, halo.centroids)
			require.Equal(t, built.side.postingOf, halo.side.postingOf)
			require.Equal(t, built.quantizer, halo.scanQuantizer)
			for _, slot := range []uint32{0, 1, n / 2, n - 1} {
				require.Equal(t, routing.Encode(all[built.postings.docIDs[slot]]), halo.side.code(slot), "slot %d", slot)
			}
			if tt.bits > 0 {
				require.Equal(t, tt.bits/64+1, halo.side.codeWords, "the sign bits and a header word")
				require.Less(t, halo.side.codeWords, built.side.codeWords)
			} else {
				require.Equal(t, built.side.codes, halo.side.codes)
			}

			index.state.Store(halo)
			got := run()
			index.state.Store(built)
			if tt.bits == 0 {
				require.Equal(t, want, got)
				return
			}
			// The first load is the entry posting, which the centroid graph
			// picks: the scan of it does not depend on the sketch.
			differ := false
			hits, total := 0, 0
			for i := range got {
				require.GreaterOrEqual(t, got[i].read.Loads, 1)
				if !slices.Equal(want[i].ids, got[i].ids) || want[i].read.Loads != got[i].read.Loads ||
					want[i].read.Stats.EndpointsScored != got[i].read.Stats.EndpointsScored {
					differ = true
				}
				for _, id := range want[i].ids {
					total++
					if slices.Contains(got[i].ids, id) {
						hits++
					}
				}
			}
			require.True(t, differ, "a sketch of %d bits ranks the postings as the full code does", tt.bits)
			// It is a noisier ranking, not a broken one.
			require.Greater(t, float64(hits)/float64(total), 0.7)
			if tt.ef >= 400 {
				require.Greater(t, float64(hits)/float64(total), 0.95)
			}
		})
	}
}

func TestHaloStateErrors(t *testing.T) {
	const n = 1200
	ctx := context.Background()
	all := clusteredVectors(n, 24, 6, 709)
	vectorOf := func(id uint64) []float32 { return all[id] }

	_, err := haloState(ctx, testLogger(), nil, nil, vectorOf)
	require.ErrorIs(t, err, ErrNotBuilt)

	tests := []struct {
		name    string
		prepare func(uc *UserConfig)
		stream  bool
		wantErr string
	}{
		{name: "postings with codes of their own", prepare: func(uc *UserConfig) { uc.ScanCodeBits = 64 }, wantErr: "codes of their own"},
		{name: "an index keyed by doc id", stream: true, wantErr: "keyed by doc id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.Distance = "l2-squared"
			if tt.prepare != nil {
				tt.prepare(&uc)
			}
			index, err := New(testConfig(t, all, 0, distancer.NewL2SquaredProvider()), uc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all))
			require.NoError(t, index.Build(ctx))
			if tt.stream {
				_, err = newStreamIndex(index, n, streamConfig{threshold: 60}, vectorOf)
				require.NoError(t, err)
			}
			st := index.state.Load()
			_, err = haloState(ctx, testLogger(), st, st.quantizer, vectorOf)
			require.ErrorContains(t, err, tt.wantErr)
			if tt.stream {
				_, err = haloPostings(st)
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

// The blocks of a posting are counted from its members and their edges, and
// the entries of its halo are the endpoints of those edges, each once.
func TestHaloPostings(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		n        int
		clusters int
	}{
		{name: "neighbors in other postings", n: 3000, clusters: 6},
		{name: "one cluster", n: 1200, clusters: 1},
		{name: "as many clusters as postings", n: 1200, clusters: 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			all := clusteredVectors(tt.n, 24, tt.clusters, 719)
			uc := smallConfig()
			uc.Distance = "l2-squared"
			index, err := New(testConfig(t, all, 0, distancer.NewL2SquaredProvider()), uc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(tt.n, 0), all))
			require.NoError(t, index.Build(ctx))
			st := index.state.Load()
			store := st.postings

			got, err := haloPostings(st)
			require.NoError(t, err)
			require.Len(t, got, store.postings())
			var members, edges int
			for p, counted := range got {
				// Counted apart, slot by slot.
				seen := map[uint32]bool{}
				ownEdges := 0
				for slot := store.start[p]; slot < store.start[p+1]; slot++ {
					for _, endpoint := range store.edgesOf(slot) {
						require.NotEqual(t, uint32(p), st.side.postingOf[endpoint], "an edge inside posting %d", p)
						seen[endpoint] = true
						ownEdges++
					}
				}
				require.Equal(t, haloPosting{Members: int(store.start[p+1] - store.start[p]), Edges: ownEdges, Distinct: len(seen)}, counted, "posting %d", p)
				require.LessOrEqual(t, counted.Distinct, counted.Edges)
				members += counted.Members
				edges += counted.Edges
			}
			require.Equal(t, tt.n, members)
			require.Equal(t, len(store.edges), edges)
			// The edges are left as they were.
			again, err := haloPostings(st)
			require.NoError(t, err)
			require.Equal(t, got, again)
		})
	}
}

// What a search would read is the sum of the blocks of the postings it
// loads, which its own stats confirm.
func TestHaloSearch(t *testing.T) {
	const n = 3000
	ctx := context.Background()
	all := clusteredVectors(n, 64, 6, 727)
	queries := clusteredVectors(30, 64, 6, 728)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index, err := New(testConfig(t, all, 0, distancer.NewL2SquaredProvider()), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all))
	require.NoError(t, index.Build(ctx))
	st := index.state.Load()
	postings, err := haloPostings(st)
	require.NoError(t, err)

	tests := []struct {
		name string
		ef   int
		// everything: the search loads every posting.
		everything bool
	}{
		{name: "a narrow search", ef: 10},
		{name: "the default search", ef: 20},
		{name: "a search that loads all it can reach", ef: n, everything: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc.EF = tt.ef
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			for _, q := range queries {
				want, _, err := index.SearchByVector(ctx, q, 10, nil)
				require.NoError(t, err)
				ids, read, err := haloSearch(ctx, index, q, 10, postings)
				require.NoError(t, err)
				require.Equal(t, want, ids)
				require.Equal(t, read.Stats.PostingsLoaded, read.Loads)
				require.Equal(t, read.Stats.VectorsScanned, read.Members)
				require.GreaterOrEqual(t, read.Loads, 1)
				require.LessOrEqual(t, read.Entries, read.Edges)
				// A search scores an endpoint once, and not at all in a
				// posting it has loaded: no more than the entries it read.
				require.LessOrEqual(t, read.Stats.EndpointsScored, read.Entries)
				if tt.everything && read.Loads == len(postings) {
					members, edges, entries := 0, 0, 0
					for _, p := range postings {
						members += p.Members
						edges += p.Edges
						entries += p.Distinct
					}
					require.Equal(t, haloRead{Loads: len(postings), Members: members, Edges: edges, Entries: entries, Stats: read.Stats}, read)
				}
			}
		})
	}

	// A table that is not the index's own does not pass.
	wrong := slices.Clone(postings)
	for p := range wrong {
		wrong[p].Members++
	}
	_, _, err = haloSearch(ctx, index, queries[0], 10, wrong)
	require.ErrorContains(t, err, "the search counted")
}

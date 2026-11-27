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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

// Pruning the halo, v3.1 (bench-report.md, "v3.1: podar el halo"). The
// entries of the halo of a posting are the distinct endpoints of the edges of
// its members, so a posting with fewer edges carries a shorter halo. The
// edges of a build are pruned with the two filters that were measured before
// on the layout with the resident sidecar: one edge per target posting
// (onePerTarget, v1.6) and a cap on the degree (capDegree, v1.5). Neither is
// a parameter of the index.

// pruneEdges applies to the edges of a build the filter of one edge per
// target posting, if asked, and then a cap on the degree, if one is given.
// With both, a vector keeps its nearest edge into each of the first degree
// postings its edges reach. Without either the arrays come back as given.
func pruneEdges(edgeStart, edges, postingOf []uint32, onePer bool, degree int) (start, kept []uint32) {
	start, kept = edgeStart, edges
	if onePer {
		start, kept = onePerTarget(start, kept, postingOf)
	}
	return capDegree(start, kept, degree)
}

// checkPruned tells whether the pruned edges are what the two filters leave
// of the full ones. Every row is built again from the full row, edge by
// edge, without the code of the filters.
func checkPruned(fullStart, full, start, kept, postingOf []uint32, onePer bool, degree int) error {
	if len(start) != len(fullStart) {
		return errors.Errorf("%d edge offsets, the full edges have %d", len(start), len(fullStart))
	}
	if len(start) == 0 {
		return nil
	}
	n := len(start) - 1
	if start[0] != 0 || int(start[n]) != len(kept) {
		return errors.Errorf("edge offsets span [%d, %d), the edges [0, %d)", start[0], start[n], len(kept))
	}
	var want []uint32
	for slot := 0; slot < n; slot++ {
		want = want[:0]
		for _, endpoint := range full[fullStart[slot]:fullStart[slot+1]] {
			if degree > 0 && len(want) == degree {
				break
			}
			if onePer && slices.ContainsFunc(want, func(nearer uint32) bool {
				return postingOf[nearer] == postingOf[endpoint]
			}) {
				continue
			}
			want = append(want, endpoint)
		}
		if got := kept[start[slot]:start[slot+1]]; !slices.Equal(got, want) {
			return errors.Errorf("slot %d keeps the edges %v, expected %v", slot, got, want)
		}
	}
	return nil
}

// A state laid over the index with the codec of the index itself searches as
// the index does: the same results, distances and work. With a shorter code
// for the endpoints the scan is the same and the ranking of the postings is
// not.
func TestHaloState(t *testing.T) {
	t.Parallel()
	const n = 300
	ctx := context.Background()
	all := clusteredVectors(n, 64, 2, 701)
	queries := clusteredVectors(10, 64, 2, 702)
	provider := distancer.NewL2SquaredProvider()

	// The ef and the rescore limit are search parameters: one build serves
	// every case, which sets them and puts the state of the build back.
	base := smallConfig()
	base.Distance = "l2-squared"
	// Postings of 10: in fewer postings the searches load the same ones
	// whatever ranks them, and no sketch is told apart.
	base.TargetPostingSize = 10
	index, err := New(testConfig(t, all, 0, provider), base)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all))
	require.NoError(t, index.Build(ctx))

	tests := []struct {
		name string
		// bits caps the code of the endpoints, 0 for the codec of the index.
		bits int
		// ef and rescore are the search.
		ef, rescore int
	}{
		{name: "the codec of the index", ef: 32, rescore: 100},
		{name: "a sketch of 64 bits", bits: 64, ef: 32, rescore: 100},
		{name: "a sketch of 64 bits, a wide search", bits: 64, ef: 120, rescore: 120},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := base
			uc.EF, uc.RescoreLimit = tt.ef, tt.rescore
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
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
			if tt.ef >= 120 {
				require.Greater(t, float64(hits)/float64(total), 0.95)
			}
		})
	}
}

func TestHaloStateErrors(t *testing.T) {
	t.Parallel()
	const n = 100
	ctx := context.Background()
	all := clusteredVectors(n, 24, 2, 709)
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
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name     string
		n        int
		clusters int
	}{
		{name: "neighbors in other postings", n: 160, clusters: 2},
		{name: "one cluster", n: 80, clusters: 1},
		{name: "as many clusters as postings", n: 80, clusters: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
	const n = 200
	ctx := context.Background()
	all := clusteredVectors(n, 64, 3, 727)
	queries := clusteredVectors(6, 64, 3, 728)
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

func TestPruneEdges(t *testing.T) {
	t.Parallel()
	// Ten slots in four postings.
	postingOf := []uint32{0, 0, 1, 1, 1, 2, 2, 3, 3, 3}
	rows := [][]uint32{
		{2, 3, 5, 4, 6, 7},
		{7, 8, 9},
		{},
		{9, 0, 5, 1, 6, 8},
		{2},
	}

	tests := []struct {
		name   string
		onePer bool
		degree int
		want   [][]uint32
		// given: the arrays come back as they were given, not as a copy.
		given bool
	}{
		{name: "neither filter", want: rows, given: true},
		{
			name: "one edge per target posting", onePer: true,
			want: [][]uint32{{2, 5, 7}, {7}, {}, {9, 0, 5}, {2}},
		},
		{
			name: "a cap of 2", degree: 2,
			want: [][]uint32{{2, 3}, {7, 8}, {}, {9, 0}, {2}},
		},
		{
			name: "a cap no row reaches", degree: 8,
			want: rows,
		},
		{
			name: "both: the cap counts target postings", onePer: true, degree: 2,
			want: [][]uint32{{2, 5}, {7}, {}, {9, 0}, {2}},
		},
		{
			name: "both, with a cap of 1", onePer: true, degree: 1,
			want: [][]uint32{{2}, {7}, {}, {9}, {2}},
		},
		{
			name: "both, with a cap no filtered row reaches", onePer: true, degree: 3,
			want: [][]uint32{{2, 5, 7}, {7}, {}, {9, 0, 5}, {2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edgeStart, edges := csrOf(rows)
			startBefore, edgesBefore := slices.Clone(edgeStart), slices.Clone(edges)

			start, kept := pruneEdges(edgeStart, edges, postingOf, tt.onePer, tt.degree)
			require.Len(t, start, len(rows)+1)
			require.Equal(t, uint32(len(kept)), start[len(rows)])
			for slot, want := range tt.want {
				got := kept[start[slot]:start[slot+1]]
				require.Truef(t, slices.Equal(want, got), "row %d: %v, expected %v", slot, got, want)
				if tt.degree > 0 {
					require.LessOrEqual(t, len(got), tt.degree)
				}
				if tt.onePer {
					targets := map[uint32]bool{}
					for _, endpoint := range got {
						require.Falsef(t, targets[postingOf[endpoint]], "row %d has two edges into posting %d", slot, postingOf[endpoint])
						targets[postingOf[endpoint]] = true
					}
				}
			}
			require.Equal(t, startBefore, edgeStart, "the given offsets are left as they are")
			require.Equal(t, edgesBefore, edges, "the given edges are left as they are")
			if tt.given {
				require.Same(t, &edges[0], &kept[0])
			} else if len(kept) > 0 {
				require.NotSame(t, &edges[0], &kept[0])
			}

			require.NoError(t, checkPruned(edgeStart, edges, start, kept, postingOf, tt.onePer, tt.degree))
		})
	}

	// With both filters a row is a prefix of the row the first one leaves.
	edgeStart, edges := csrOf(rows)
	oneStart, one := pruneEdges(edgeStart, edges, postingOf, true, 0)
	bothStart, both := pruneEdges(edgeStart, edges, postingOf, true, 2)
	for slot := range rows {
		filtered, capped := one[oneStart[slot]:oneStart[slot+1]], both[bothStart[slot]:bothStart[slot+1]]
		require.Equal(t, filtered[:min(2, len(filtered))], capped, "row %d", slot)
	}
}

func TestCheckPruned(t *testing.T) {
	t.Parallel()
	postingOf := []uint32{0, 0, 1, 1, 1, 2, 2, 3, 3, 3}
	fullStart, full := csrOf([][]uint32{{2, 3, 5, 4, 6, 7}, {7, 8, 9}, {}})

	tests := []struct {
		name    string
		rows    [][]uint32
		onePer  bool
		degree  int
		wantErr string
	}{
		{name: "the full edges, unpruned", rows: [][]uint32{{2, 3, 5, 4, 6, 7}, {7, 8, 9}, {}}},
		{name: "one per target", rows: [][]uint32{{2, 5, 7}, {7}, {}}, onePer: true},
		{name: "a cap", rows: [][]uint32{{2, 3, 5}, {7, 8, 9}, {}}, degree: 3},
		{name: "both", rows: [][]uint32{{2, 5}, {7}, {}}, onePer: true, degree: 2},
		{
			name: "a target posting is lost", rows: [][]uint32{{2, 5}, {7}, {}}, onePer: true,
			wantErr: "slot 0 keeps the edges [2 5], expected [2 5 7]",
		},
		{
			name: "an edge into a posting twice", rows: [][]uint32{{2, 3, 5, 7}, {7}, {}}, onePer: true,
			wantErr: "slot 0 keeps the edges [2 3 5 7]",
		},
		{
			name: "not the nearest edge into the posting", rows: [][]uint32{{3, 5, 7}, {7}, {}}, onePer: true,
			wantErr: "slot 0 keeps the edges [3 5 7], expected [2 5 7]",
		},
		{
			name: "a row above the cap", rows: [][]uint32{{2, 3, 5}, {7, 8, 9}, {}}, degree: 2,
			wantErr: "slot 0 keeps the edges [2 3 5], expected [2 3]",
		},
		{
			name: "a row cut short of the cap", rows: [][]uint32{{2, 3}, {7, 8}, {}}, degree: 3,
			wantErr: "slot 0 keeps the edges [2 3], expected [2 3 5]",
		},
		{
			name: "the cap taken before the filter", rows: [][]uint32{{2}, {7}, {}}, onePer: true, degree: 2,
			wantErr: "slot 0 keeps the edges [2], expected [2 5]",
		},
		{name: "a row too few", rows: [][]uint32{{2, 3, 5, 4, 6, 7}, {7, 8, 9}}, wantErr: "3 edge offsets, the full edges have 4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, kept := csrOf(tt.rows)
			err := checkPruned(fullStart, full, start, kept, postingOf, tt.onePer, tt.degree)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("offsets that do not span the edges", func(t *testing.T) {
		start, kept := csrOf([][]uint32{{2, 3, 5, 4, 6, 7}, {7, 8, 9}, {}})
		require.ErrorContains(t, checkPruned(fullStart, full, start, kept[:len(kept)-1], postingOf, false, 0), "edge offsets span")
	})
	t.Run("no vectors", func(t *testing.T) {
		require.NoError(t, checkPruned(nil, nil, nil, nil, postingOf, true, 8))
	})
}

// Over a state with a sketch, pruned edges leave every posting the halo of
// the edges that remain, and every search still adds up with its own counts,
// whatever the source margin cuts.
func TestHaloPruned(t *testing.T) {
	t.Parallel()
	const n = 200
	ctx := context.Background()
	all := clusteredVectors(n, 64, 3, 733)
	queries := clusteredVectors(6, 64, 3, 734)
	uc := smallConfig()
	uc.Distance, uc.EF = "l2-squared", 10
	index, err := New(testConfig(t, all, 0, distancer.NewL2SquaredProvider()), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(n, 0), all))
	require.NoError(t, index.Build(ctx))
	built := index.state.Load()
	routing, err := built.quantizer.withBits(64)
	require.NoError(t, err)
	halo, err := haloState(ctx, testLogger(), built, routing, func(id uint64) []float32 { return all[id] })
	require.NoError(t, err)
	fullStart, full := built.postings.edgeStart, built.postings.edges
	postingOf := built.side.postingOf
	fullPostings, err := haloPostings(built)
	require.NoError(t, err)
	fullTargets := edgeRedundancyOf(fullStart, full, postingOf)
	require.Positive(t, fullTargets.Shared, "the fixture must have vectors with several edges into one posting")

	tests := []struct {
		name   string
		onePer bool
		degree int
	}{
		{name: "every edge"},
		{name: "one edge per target posting", onePer: true},
		{name: "a cap of 4", degree: 4},
		{name: "both", onePer: true, degree: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, kept := pruneEdges(fullStart, full, postingOf, tt.onePer, tt.degree)
			require.NoError(t, checkPruned(fullStart, full, start, kept, postingOf, tt.onePer, tt.degree))
			pruned := tt.onePer || tt.degree > 0
			if pruned {
				require.Less(t, len(kept), len(full), "the filter must take edges out of the fixture")
			}
			got := edgeRedundancyOf(start, kept, postingOf)
			if tt.onePer {
				require.Zero(t, got.Shared)
			}
			if tt.onePer && tt.degree == 0 {
				require.Equal(t, fullTargets.Targets, got.Targets, "no vector loses a target posting")
			}

			index.state.Store(halo)
			t.Cleanup(func() { index.state.Store(built) })
			if pruned {
				require.NoError(t, index.replaceEdges(start, kept))
			}
			st := index.state.Load()
			require.Same(t, halo.side, st.side, "the sketches stay")
			require.Equal(t, halo.scanQuantizer, st.scanQuantizer)

			postings, err := haloPostings(st)
			require.NoError(t, err)
			edges, shorter := 0, false
			for p, counted := range postings {
				seen := map[uint32]bool{}
				ownEdges := 0
				for slot := st.postings.start[p]; slot < st.postings.start[p+1]; slot++ {
					row := kept[start[slot]:start[slot+1]]
					ownEdges += len(row)
					for _, endpoint := range row {
						seen[endpoint] = true
					}
				}
				require.Equal(t, haloPosting{Members: fullPostings[p].Members, Edges: ownEdges, Distinct: len(seen)}, counted, "posting %d", p)
				require.LessOrEqual(t, counted.Distinct, fullPostings[p].Distinct, "posting %d", p)
				shorter = shorter || counted.Distinct < fullPostings[p].Distinct
				edges += counted.Edges
			}
			require.Equal(t, len(kept), edges)
			require.Equal(t, pruned, shorter, "a halo is shorter only with fewer edges")

			// No margin, one that cuts nearly every source once the search
			// has ef candidates, and one that cuts none.
			for _, margin := range []float32{0, 1e-6, 1e6} {
				marginUC := uc
				marginUC.SourceMargin = margin
				require.NoError(t, index.UpdateUserConfig(marginUC, func() {}))
				cut, followed := 0, 0
				for _, q := range queries {
					// haloSearch fails unless what it read adds up.
					ids, read, err := haloSearch(ctx, index, q, 10, postings)
					require.NoError(t, err)
					require.Len(t, ids, 10)
					require.Equal(t, read.Edges, read.Stats.EdgesFollowed+read.Stats.EdgesCut)
					require.LessOrEqual(t, read.Stats.EndpointsScored, read.Entries)
					cut += read.Stats.EdgesCut
					followed += read.Stats.EdgesFollowed
				}
				require.Positive(t, followed)
				if margin == 1e-6 {
					require.Positive(t, cut, "the margin must cut sources")
				} else {
					require.Zero(t, cut)
				}
			}
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))

			// The full layout is not that of pruned edges.
			if pruned {
				_, _, err = haloSearch(ctx, index, queries[0], 10, fullPostings)
				require.ErrorContains(t, err, "the search counted")
			}
		})
	}
}

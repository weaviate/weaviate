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
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// What the endpoint scoring of a search costs with the endpoint side of the
// index keyed by doc id instead of by slot: edges that store doc ids, a
// sidecar of codes indexed by doc id, and the posting of each doc id in an
// array of 4 bytes per vector (the vector→posting map of disk-design.md,
// "Open for phase 2"). Keyed by slot, the endpoints of a source that land in
// one posting are neighbours in memory; keyed by doc id, they are wherever
// the order of insertion put them.
//
// Nothing of this is wired into the index. A search is recorded through
// SearchStats.loadHook, load by load, and its endpoint scoring is replayed
// over each keying: the same endpoints, deduplicated, scored and folded into
// the ranks of their postings as postingSearch.load does, over arrays that
// differ only in their order.

// recordedLoad is one loaded posting as the endpoint scoring of a search sees
// it.
type recordedLoad struct {
	// marked is how many postings were marked loaded by then. It counts the
	// postings picked for the same round, which are marked before any of
	// them is loaded.
	marked int
	// followed lists the endpoints of the edges of the sources within the
	// cut, source by source and edge by edge, before any is skipped.
	followed []uint32
}

// recordedSearch is what one search gave its endpoint scoring.
type recordedSearch struct {
	query []float32
	// touched lists the postings in the order the search marked them loaded.
	touched []uint32
	loads   []recordedLoad
	// scored is the search's own count of the endpoints it scored.
	scored int
}

// recordSearch runs one search and records what its endpoint scoring was
// given. It returns the ids the search found as well.
func recordSearch(ctx context.Context, index *Index, query []float32, k int) (recordedSearch, []uint64, error) {
	rec := recordedSearch{query: query}
	stats := SearchStats{loadHook: func(sc *searchScratch, post *posting, cut float32) {
		rec.touched = append(rec.touched, sc.touched[len(rec.touched):]...)
		load := recordedLoad{marked: len(sc.touched)}
		for i, dist := range sc.memberDist {
			if dist <= cut {
				load.followed = append(load.followed, post.edgesOf(i)...)
			}
		}
		rec.loads = append(rec.loads, load)
	}}
	ids, _, err := index.search(ctx, query, k, nil, &stats)
	if err != nil {
		return rec, nil, err
	}
	rec.scored = stats.EndpointsScored
	return rec, ids, nil
}

// endpointKeys is one keying of the endpoint side of a built index: the
// sidecar codes and the posting of every vector, indexed by key, and the
// recorded searches with their endpoints turned into keys.
type endpointKeys struct {
	name     string
	side     *sidecar
	searches []recordedSearch
}

// keyedSidecar lays the sidecar of st out by the keys of keyOf, which gives
// the key of every slot and must be a permutation of the slots. The arrays
// are fresh copies whatever the keying, the slots one included, so that all
// of them come from the same allocator at about the same time.
func keyedSidecar(st *state, keyOf []uint32) *sidecar {
	words := st.side.codeWords
	side := &sidecar{
		codeWords: words,
		codes:     make([]uint64, len(st.side.codes)),
		postingOf: make([]uint32, len(st.side.postingOf)),
	}
	for slot, key := range keyOf {
		copy(side.codes[int(key)*words:int(key+1)*words], st.side.codes[slot*words:(slot+1)*words])
		side.postingOf[key] = st.side.postingOf[slot]
	}
	return side
}

// keyedSearches turns the endpoints of the recorded searches into the keys
// of keyOf, into copies.
func keyedSearches(searches []recordedSearch, keyOf []uint32) []recordedSearch {
	keyed := make([]recordedSearch, len(searches))
	for i, search := range searches {
		keyed[i] = search
		keyed[i].loads = make([]recordedLoad, len(search.loads))
		for j, load := range search.loads {
			followed := make([]uint32, len(load.followed))
			for e, slot := range load.followed {
				followed[e] = keyOf[slot]
			}
			keyed[i].loads[j] = recordedLoad{marked: load.marked, followed: followed}
		}
	}
	return keyed
}

// keyedBy is the keying of keyOf, sidecar and searches.
func keyedBy(name string, st *state, keyOf []uint32, searches []recordedSearch) *endpointKeys {
	return &endpointKeys{name: name, side: keyedSidecar(st, keyOf), searches: keyedSearches(searches, keyOf)}
}

// slotKeys, docIDKeys and shuffledKeys give the key of every slot of st: the
// slot itself, the doc id the slot holds, and that doc id through a random
// permutation of the doc ids. The doc ids must be dense, 0 to n-1.
func slotKeys(st *state) []uint32 {
	keys := make([]uint32, len(st.postings.docIDs))
	for slot := range keys {
		keys[slot] = uint32(slot)
	}
	return keys
}

func docIDKeys(st *state) ([]uint32, error) {
	n := len(st.postings.docIDs)
	keys := make([]uint32, n)
	seen := make([]bool, n)
	for slot, docID := range st.postings.docIDs {
		if docID >= uint64(n) || seen[docID] {
			return nil, fmt.Errorf("doc ids are not dense: %d at slot %d of %d", docID, slot, n)
		}
		seen[docID] = true
		keys[slot] = uint32(docID)
	}
	return keys, nil
}

func shuffledKeys(st *state, seed uint64) ([]uint32, error) {
	keys, err := docIDKeys(st)
	if err != nil {
		return nil, err
	}
	perm := rand.New(rand.NewPCG(seed, seed+1)).Perm(len(keys))
	for slot, docID := range keys {
		keys[slot] = uint32(perm[docID])
	}
	return keys, nil
}

// endpointReplay holds what a replay needs besides the keys: a scratch as
// big as the index and the estimator of every recorded query.
type endpointReplay struct {
	sc         *searchScratch
	estimators []codeEstimator
	dists      []float32
}

func newEndpointReplay(st *state, searches []recordedSearch) *endpointReplay {
	r := &endpointReplay{sc: newSearchScratch(len(st.postings.start) - 1)}
	r.sc.endpoints = visited.NewSparseSet(len(st.postings.docIDs), 8)
	for _, search := range searches {
		r.estimators = append(r.estimators, st.quantizer.NewEstimator(search.query))
	}
	return r
}

// run replays the endpoint scoring of every recorded search over the keys,
// as postingSearch.load does it: the postings marked loaded when the search
// marked them, the endpoints of a load collected, scored as one batch and
// folded into the ranks of their postings. It returns how many endpoints it
// scored and the sum of their estimates, which no keying may change.
func (r *endpointReplay) run(keys *endpointKeys) (scored int, sum float64, err error) {
	sc, side := r.sc, keys.side
	for i, search := range keys.searches {
		marked := 0
		for _, load := range search.loads {
			for ; marked < load.marked; marked++ {
				sc.markLoaded(search.touched[marked])
			}
			sc.unscored = sc.unscored[:0]
			for _, key := range load.followed {
				if sc.loaded[side.postingOf[key]] || sc.endpoints.CheckAndVisit(uint64(key)) {
					continue
				}
				sc.unscored = append(sc.unscored, key)
			}
			if cap(r.dists) < len(sc.unscored) {
				r.dists = make([]float32, len(sc.unscored))
			}
			dists := r.dists[:len(sc.unscored)]
			if err := side.score(r.estimators[i], sc.unscored, dists); err != nil {
				return 0, 0, err
			}
			sc.rankPostings(side.postingOf, dists)
			scored += len(dists)
			for _, d := range dists {
				sum += float64(d)
			}
		}
		r.reset()
	}
	return scored, sum, nil
}

// reset clears the scratch as returnScratch does between searches.
func (r *endpointReplay) reset() {
	sc := r.sc
	for _, p := range sc.frontier {
		sc.rank[p] = inf32
	}
	for _, p := range sc.touched {
		sc.rank[p] = inf32
		sc.loaded[p] = false
	}
	sc.frontier = sc.frontier[:0]
	sc.touched = sc.touched[:0]
	sc.endpoints.Reset()
}

// The replay scores exactly the endpoints the searches scored, whatever the
// keying, with the same estimates, and leaves its scratch as it found it.
func TestEndpointKeysReplay(t *testing.T) {
	t.Parallel()
	const k = 10
	all := clusteredVectors(208, 24, 4, 515)
	vectors, queries := all[:200], all[200:]
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)
	st := index.state.Load()
	ctx := context.Background()

	slots := slotKeys(st)
	docIDs, err := docIDKeys(st)
	require.NoError(t, err)
	shuffled, err := shuffledKeys(st, 7)
	require.NoError(t, err)
	require.NotEqual(t, slots, docIDs, "a build lays its slots out by posting, not by doc id")

	for _, ef := range []int{10, 40, 160} {
		t.Run(fmt.Sprintf("ef %d", ef), func(t *testing.T) {
			uc.EF = ef
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			var searches []recordedSearch
			want := 0
			for _, query := range queries {
				ids, dists, err := index.SearchByVector(ctx, query, k, nil)
				require.NoError(t, err)
				rec, recordedIDs, err := recordSearch(ctx, index, query, k)
				require.NoError(t, err)
				require.Equal(t, ids, recordedIDs)
				// The hook changes nothing the search does.
				recIDs, recDists, err := index.search(ctx, query, k, nil, &SearchStats{loadHook: func(*searchScratch, *posting, float32) {}})
				require.NoError(t, err)
				require.Equal(t, ids, recIDs)
				require.Equal(t, dists, recDists)
				searches = append(searches, rec)
				want += rec.scored
			}
			require.Positive(t, want)

			replay := newEndpointReplay(st, searches)
			var sums []float64
			for _, keys := range []*endpointKeys{
				keyedBy("slots", st, slots, searches),
				keyedBy("doc ids", st, docIDs, searches),
				keyedBy("doc ids, shuffled", st, shuffled, searches),
			} {
				scored, sum, err := replay.run(keys)
				require.NoError(t, err)
				assert.Equal(t, want, scored, keys.name)
				sums = append(sums, sum)

				sc := replay.sc
				assert.Empty(t, sc.touched, keys.name)
				assert.Empty(t, sc.frontier, keys.name)
				for p := range sc.rank {
					require.Equal(t, inf32, sc.rank[p], keys.name)
					require.False(t, sc.loaded[p], keys.name)
				}
			}
			assert.Equal(t, sums[0], sums[1])
			assert.Equal(t, sums[0], sums[2])
		})
	}
}

// keyedBy moves every code and posting to the key of its slot, and nothing
// else.
func TestKeyedBy(t *testing.T) {
	t.Parallel()
	st := &state{
		postings: &postingStore{docIDs: []uint64{2, 0, 1}, start: []uint32{0, 2, 3}},
		side: &sidecar{
			codeWords: 2,
			codes:     []uint64{10, 11, 20, 21, 30, 31},
			postingOf: []uint32{0, 0, 1},
		},
	}
	searches := []recordedSearch{{loads: []recordedLoad{{marked: 1, followed: []uint32{2, 0}}}}}

	docIDs, err := docIDKeys(st)
	require.NoError(t, err)
	keys := keyedBy("doc ids", st, docIDs, searches)
	assert.Equal(t, []uint64{20, 21, 30, 31, 10, 11}, keys.side.codes)
	assert.Equal(t, []uint32{0, 1, 0}, keys.side.postingOf)
	assert.Equal(t, []uint32{1, 2}, keys.searches[0].loads[0].followed)
	assert.Equal(t, []uint32{2, 0}, searches[0].loads[0].followed, "the recording is left as it was")

	st.postings.docIDs[0] = 7
	_, err = docIDKeys(st)
	assert.ErrorContains(t, err, "not dense")
	st.postings.docIDs[0] = 0
	_, err = docIDKeys(st)
	assert.ErrorContains(t, err, "not dense")
}

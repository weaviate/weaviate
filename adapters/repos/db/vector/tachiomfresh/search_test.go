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

package tachiomfresh

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The synthetic corpus of the tests that call newRandomCorpusIndex: 200 docs of 8
// tokens in 96 dims, a split threshold small enough that the 1,600 tokens
// force many splits, and search budgets large enough that every posting is
// probed and every document is scored, so search is exact.
const (
	testDocs           = 200
	testTokens         = 8
	testDim            = 96
	testMaxPostingSize = 64
	// above any centroid count 1,600 tokens can produce, so stage 1
	// probes every posting
	testKCentroids   = 1000
	testKDocsToScore = testDocs
)

func newRandomCorpusIndex(t *testing.T) (testIndex, [][][]float32) {
	t.Helper()
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))

	rng := rand.New(rand.NewSource(7))
	docs := make([][][]float32, testDocs)
	for i := 0; i < testDocs; i++ {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
		addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
	}
	// splits run on the task queue
	tf.Index.waitForMaintenance(t)
	return tf, docs
}

// TestSearchByMultiVectorSelfRecall is the port of hfresh's test of
// the same name: querying with a document's own multi-vector
// must return that document as top-1. Every document is scored, so the
// exact MaxSim stage sees it and self-recall@1 must be perfect.
func TestSearchByMultiVectorSelfRecall(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)

	sampleRng := rand.New(rand.NewSource(13))
	const samples = 50
	for q := 0; q < samples; q++ {
		id := uint64(sampleRng.Intn(testDocs))
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 1, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		require.Equal(t, id, ids[0],
			"self-query for doc %d returned doc %d: exact MaxSim must rank the identical multi-vector first", id, ids[0])
	}
}

// TestSearchMatchesBruteForceWhenEverythingIsProbed compares the full top-10
// of random queries with an in-test brute-force dot MaxSim over all
// documents: with every posting probed and every document scored, ids must
// be identical and distances within 1e-5.
func TestSearchMatchesBruteForceWhenEverythingIsProbed(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)

	dot := distancer.NewDotProductProvider()
	bruteForce := func(query [][]float32) ([]uint64, []float32) {
		type result struct {
			id   uint64
			dist float32
		}
		results := make([]result, testDocs)
		for d, doc := range docs {
			var score float32
			for _, q := range query {
				minDist := float32(math.MaxFloat32)
				for _, tok := range doc {
					dist, err := dot.SingleDist(q, tok)
					require.NoError(t, err)
					minDist = min(minDist, dist)
				}
				score += minDist
			}
			results[d] = result{id: uint64(d), dist: score}
		}
		sort.Slice(results, func(i, j int) bool {
			if results[i].dist != results[j].dist {
				return results[i].dist < results[j].dist
			}
			return results[i].id < results[j].id
		})
		ids := make([]uint64, 10)
		dists := make([]float32, 10)
		for i := range 10 {
			ids[i], dists[i] = results[i].id, results[i].dist
		}
		return ids, dists
	}

	rng := rand.New(rand.NewSource(21))
	for q := 0; q < 20; q++ {
		query := randomMultiVector(rng, 5, testDim)
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
		require.NoError(t, err)

		wantIDs, wantDists := bruteForce(query)
		require.Equal(t, wantIDs, ids, "query %d", q)
		for i := range wantDists {
			require.InDelta(t, wantDists[i], dists[i], 1e-5, "query %d rank %d", q, i)
		}
	}
}

// TestSplitKeepsEveryToken checks the posting table after the inserts: every
// token is in exactly one posting, postings with entries have a centroid
// and split-away postings have no entries.
func TestSplitKeepsEveryToken(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)

	live := make(map[uint64][][]float32, len(docs))
	for i, doc := range docs {
		live[uint64(i)] = doc
	}
	checkInvariants(t, &tf, live, nil, false)

	var centroids int
	for id := range tf.Index.postings.lists {
		if tf.Index.Centroids.Exists(uint64(id)) {
			centroids++
		} else {
			require.Empty(t, tf.posting(uint64(id)), "posting %d has no centroid but still has entries", id)
		}
	}
	require.Greater(t, centroids, 1, "no split happened")
}

// TestConcurrentInsertAndSearch inserts the synthetic corpus from several
// goroutines while others search and delete, with splits,
// merges and reassigns running in the background. Then it checks the
// invariants, that search over the surviving documents is still exact, and
// that no deleted document is returned. The inserts run in parallel as
// Weaviate's index queue runs them (workers = GOMAXPROCS-1, docs
// partitioned by id % workers, queue/scheduler.go). Run it with -race.
func TestConcurrentInsertAndSearch(t *testing.T) {
	const workers = 8
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))

	rng := rand.New(rand.NewSource(7))
	docs := make([][][]float32, testDocs)
	live := make(map[uint64][][]float32, testDocs)
	for i := range docs {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
		live[uint64(i)] = docs[i]
		// stored up front: a split may read any document back
		tf.mvStore.storeMultiVector(uint64(i), docs[i])
	}
	// every fifth document is deleted by its inserter's neighbour goroutine
	// as soon as it is in
	deleted := make(map[uint64]bool)
	for i := 0; i < testDocs; i += 5 {
		deleted[uint64(i)] = true
	}

	var wg sync.WaitGroup
	errs := make(chan error, 3*workers)
	inserted := make([]chan uint64, workers)
	for w := range workers {
		inserted[w] = make(chan uint64, testDocs)
		wg.Add(3)
		go func() {
			defer wg.Done()
			defer close(inserted[w])
			for i := w; i < testDocs; i += workers {
				if err := tf.addMulti(t.Context(), uint64(i), docs[i]); err != nil {
					errs <- err
					return
				}
				inserted[w] <- uint64(i)
			}
		}()
		go func() {
			defer wg.Done()
			for id := range inserted[w] {
				if !deleted[id] {
					continue
				}
				// the index tombstones the document; its record stays
				// readable here, as an in-flight split may hold its entry
				if err := tf.Index.DeleteMulti(id); err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for q := range 20 {
				if _, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[(w*20+q)%testDocs], 10, nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	tf.Index.waitForMaintenance(t)

	checkInvariants(t, &tf, live, deleted, false)

	// everything is probed and scored, so self-queries are exact: a live
	// document finds itself, a deleted one is never returned
	for id := range 50 {
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 10, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		if !deleted[uint64(id)] {
			require.Equal(t, uint64(id), ids[0])
		}
		for _, got := range ids {
			require.False(t, deleted[got], "deleted doc %d returned", got)
		}
	}
}

// searchScratch gives what two maps and a full sort give:
// per document the sum over query tokens of the best similarity, and the top
// n by that sum, ties by doc id ascending.
func TestSearchScratchMatchesMapsAndFullSort(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		docs, tokens, entries int
		n                     int
	}{
		{"fewer documents than n", 20, 4, 50, 100},
		{"as many as n", 100, 4, 400, 100},
		{"table grows several times", 20000, 8, 30000, 50},
		{"many ties", 300, 3, 2000, 25},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(11))
			acc := make(map[uint32]float32)
			s := getSearchScratch()
			defer putSearchScratch(s)
			for token := range tt.tokens {
				best := make(map[uint32]float32)
				for range tt.entries {
					// sparse ids, and few distinct similarities so that
					// sums tie
					id := uint32(rng.Intn(tt.docs)) * 7919
					sim := float32(rng.Intn(5)) / 4
					if prev, ok := best[id]; !ok || sim > prev {
						best[id] = sim
					}

					doc, _ := s.slot(id)
					if doc.token != uint32(token+1) {
						doc.acc += doc.best
						doc.best = sim
						doc.token = uint32(token + 1)
					} else if sim > doc.best {
						doc.best = sim
					}
				}
				for id, sim := range best {
					acc[id] += sim
				}
			}

			want := make([]candidate, 0, len(acc))
			for id, score := range acc {
				want = append(want, candidate{id: id, score: score})
			}
			sort.Slice(want, func(i, j int) bool {
				if want[i].score != want[j].score {
					return want[i].score > want[j].score
				}
				return want[i].id < want[j].id
			})
			if len(want) > tt.n {
				want = want[:tt.n]
			}
			require.Equal(t, want, s.top(tt.n))
		})
	}

	t.Run("deleted documents are left out", func(t *testing.T) {
		s := getSearchScratch()
		defer putSearchScratch(s)
		for id := range uint32(10) {
			doc, isNew := s.slot(id)
			require.True(t, isNew)
			// skip is what top reads: a deleted or disallowed document
			doc.deleted = id%2 == 0
			doc.skip = doc.deleted
			doc.best, doc.token = float32(id), 1
		}
		_, isNew := s.slot(3)
		require.False(t, isNew)
		require.Equal(t, []candidate{{9, 9}, {7, 7}, {5, 5}}, s.top(3))
	})
}

// reset clears the cells a scan used, whatever the probing did: ids that
// collide, a table that grew during the scan, and a scratch that scans
// little after it had grown large.
func TestScratchResetFreesEveryCell(t *testing.T) {
	ids := func(n int, id func(i int) uint32) []uint32 {
		out := make([]uint32, n)
		for i := range out {
			out[i] = id(i)
		}
		return out
	}
	tests := []struct {
		name string
		ids  []uint32
	}{
		{name: "nothing"},
		{name: "consecutive ids", ids: ids(100, func(i int) uint32 { return uint32(i) })},
		{name: "ids of one cell", ids: ids(100, func(i int) uint32 { return uint32(i) << 25 })},
		{name: "enough to grow the table", ids: ids(1<<18, func(i int) uint32 { return uint32(i) * 3 })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := searchScratchPool.New().(*searchScratch)
			for range 2 {
				for _, id := range tt.ids {
					_, isNew := s.slot(id)
					require.True(t, isNew, "a document left from the scan before: %d", id)
				}
				s.reset()
				require.Empty(t, s.docs)
				for i, cell := range s.table {
					require.Zero(t, cell, "cell %d", i)
				}
			}
		})
	}
	t.Run("a small scan after the table grew", func(t *testing.T) {
		s := searchScratchPool.New().(*searchScratch)
		for _, id := range ids(1<<18, func(i int) uint32 { return uint32(i) * 3 }) {
			s.slot(id)
		}
		s.reset()
		for _, id := range []uint32{7, 1 << 20, 3} {
			_, isNew := s.slot(id)
			require.True(t, isNew, "a document left from the scan before: %d", id)
		}
		s.reset()
		require.Empty(t, s.docs)
		for i, cell := range s.table {
			require.Zero(t, cell, "cell %d", i)
		}
	})
}

// A scratch that has served a query gives the next query what a new scratch
// gives it.
func TestReusedScratchScoresLikeANewOne(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)
	// the scans below enqueue merges, which must not run between them
	tf.Index.pauseMaintenance(t)
	scan := func(s *searchScratch, query [][]float32) []candidate {
		_, err := tf.Index.scanPostings(t.Context(), query, testKCentroids, nil, s)
		require.NoError(t, err)
		return s.top(testDocs)
	}

	// reset, what putSearchScratch does before it pools the scratch: the
	// scratch is used again below, so it must not go back to the pool
	reused := searchScratchPool.New().(*searchScratch)
	scan(reused, docs[1])
	reused.reset()
	for i, cell := range reused.table {
		require.Zero(t, cell, "cell %d", i)
	}

	require.Equal(t,
		scan(searchScratchPool.New().(*searchScratch), docs[2]),
		scan(reused, docs[2]))
}

// TestScanIgnoresDocumentsReachedOnlyByStaleEntries scans a posting that
// holds the entries of a document re-inserted since (stale: older than the
// document's version) and the entries of a live document. The stale one
// must neither count as reached nor become a stage-2 candidate.
func TestScanIgnoresDocumentsReachedOnlyByStaleEntries(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)
	tf.Index.opts.disableReassign = true

	vectors := createTestVectors(4, 2)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 1) // docs 1 and 2
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))
	// doc 1 is inserted again: its entry in the posting is stale
	_, err := tf.Index.VersionMap.Reinsert(t.Context(), 1)
	require.NoError(t, err)

	scratch := getSearchScratch()
	defer putSearchScratch(scratch)
	_, err = tf.Index.scanPostings(t.Context(), [][]float32{vectors[0]}, 1, nil, scratch)
	require.NoError(t, err)

	require.Equal(t, 1, scratch.live, "documents reached by a live entry")
	var ids []uint32
	for _, c := range scratch.top(10) {
		ids = append(ids, c.id)
	}
	require.Equal(t, []uint32{2}, ids, "stage-2 candidates")
}

// TestSearchByMultiVectorDistanceWidens searches with a distance every
// document is within. The first search asks for
// common.DefaultSearchByDistInitialLimit documents, fewer than the index
// holds; the search must widen until it has them all.
func TestSearchByMultiVectorDistanceWidens(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)

	rng := rand.New(rand.NewSource(5))
	for i := range uint64(testDocs) {
		addMultiVectorToIndex(t, &tf, i, randomMultiVector(rng, testTokens, testDim))
	}
	tf.Index.waitForMaintenance(t)
	require.Greater(t, testDocs, common.DefaultSearchByDistInitialLimit)

	query := randomMultiVector(rng, testTokens, testDim)
	ids, _, err := tf.Index.SearchByMultiVectorDistance(t.Context(), query, math.MaxFloat32, 10*testDocs, nil)
	require.NoError(t, err)
	require.Len(t, ids, testDocs)
}

// TestLateInteractionStopsOnCancel: the exact scoring of the candidates
// returns the context's error once the query is cancelled, rather than
// reading and scoring every candidate.
func TestLateInteractionStopsOnCancel(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)
	for id := range uint64(3) {
		tf.mvStore.storeMultiVector(id, [][]float32{{1, 0}, {0, 1}})
	}
	var reads atomic.Int64
	read := tf.Index.multiVectorForID
	tf.Index.multiVectorForID = func(ctx context.Context, id uint64) ([][]float32, error) {
		reads.Add(1)
		return read(ctx, id)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := tf.Index.computeLateInteraction(ctx, [][]float32{{1, 0}}, 2, []uint64{0, 1, 2}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, reads.Load(), "candidates read after the cancel")
}

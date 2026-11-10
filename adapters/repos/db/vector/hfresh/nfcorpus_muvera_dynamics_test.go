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

//go:build nfcorpus

package hfresh

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestNFCorpusMuveraDynamics is the HFresh + MUVERA side of the dynamics
// measurement, the baseline for TestNFCorpusDynamics in
// tachiomfresh/nfcorpus_test.go: the same protocol, seed and therefore the
// same documents. Build, then
// three rounds of deleting a random 20 % of the live documents and
// reinserting them under new doc ids, as a Weaviate update does. After the
// build, each delete and each reinsert it prints the posting statistics and
// recall@10 and QPS on a few cells.
//
// Ground truth as on the TachiomFresh side: the first 10 live documents of
// the cache's exact MaxSim top 100 (that test verifies this shortcut by
// brute force on the same live sets).
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -count=1 -tags=nfcorpus -run 'TestNFCorpusMuveraDynamics$' -v -timeout 900m ./adapters/repos/db/vector/hfresh/...
func TestNFCorpusMuveraDynamics(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	corpus, err := loadMVC1(path)
	require.NoError(t, err)
	workers := buildWorkers()
	// same constants as TestNFCorpusDynamics
	const (
		rounds   = 3
		fraction = 0.2
		seed     = 1
	)
	n := corpus.numDocs()
	perRound := int(fraction * float64(n))
	t.Logf("corpus: %d docs, %d queries; rounds=%d fraction=%.2f (%d docs per round) seed=%d; build: workers=%d; search: GOMAXPROCS=1\n",
		n, len(corpus.queries), rounds, fraction, perRound, seed, workers)

	// docOf[indexDocID] is the corpus document behind an index doc id, -1
	// once its record is dropped; current[corpusDoc] is its live index doc id
	docOf := make([]atomic.Int32, n+rounds*perRound)
	current := make([]uint64, n)
	live := make([]bool, n) // by corpus doc
	for i := range n {
		docOf[i].Store(int32(i))
		current[i] = uint64(i)
		live[i] = true
	}

	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{192}) {
		uc := muveraUserConfig()
		tf := newTestIndex(t, uc, nil,
			withDistanceProvider(distancer.NewDotProductProvider()),
			func(cfg *Config) {
				cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
					doc := docOf[id].Load()
					if doc < 0 {
						return nil, fmt.Errorf("doc %d was deleted", id)
					}
					return corpus.doc(int(doc)), nil
				}
			})
		index := tf.Index

		// exact top 10 of query q over the live documents, as index doc ids
		groundTruth := func(q int) map[uint64]struct{} {
			want := make(map[uint64]struct{}, 10)
			for _, doc := range corpus.groundTruth(q) {
				if live[doc] {
					want[current[doc]] = struct{}{}
					if len(want) == 10 {
						break
					}
				}
			}
			require.Len(t, want, 10, "query %d has fewer than 10 live documents in its cached top %d", q, corpus.gtDepth)
			return want
		}

		measure := func(round int, phase string) {
			index.waitForMaintenance(t)
			t.Logf("dynamics index=hfresh-muvera maxPostingSize=%d round=%d phase=%s centroids=%d heap_bytes=%d\n",
				maxPostingSize, round, phase, index.PostingSizes.Count(), heapLive())

			prev := runtime.GOMAXPROCS(1)
			for _, searchProbe := range envList(t, "TACHIOMFRESH_PROBE", []uint32{64, 512}) {
				for _, rescoreLimit := range envList(t, "TACHIOMFRESH_RESCORE", []int{500, 1000, 2000}) {
					uc.SearchProbe = searchProbe
					uc.RQ.RescoreLimit = rescoreLimit
					require.NoError(t, index.UpdateUserConfig(uc, func() {}))

					var recall10 float64
					var took time.Duration
					for q, query := range corpus.queries {
						before := time.Now()
						ids, _, err := index.SearchByMultiVector(t.Context(), query, 10, nil)
						took += time.Since(before)
						require.NoError(t, err)
						want := groundTruth(q)
						hits := 0
						for _, id := range ids {
							require.GreaterOrEqual(t, docOf[id].Load(), int32(0), "deleted doc %d returned", id)
							if _, ok := want[id]; ok {
								hits++
							}
						}
						recall10 += float64(hits) / 10
					}
					nq := float64(len(corpus.queries))
					t.Logf("cell index=hfresh-muvera maxPostingSize=%d round=%d phase=%s searchProbe=%d rescoreLimit=%d recall@10=%.4f qps=%.2f mean_ms=%.3f\n",
						maxPostingSize, round, phase, searchProbe, rescoreLimit, recall10/nq, nq/took.Seconds(), took.Seconds()*1000/nq)
				}
			}
			runtime.GOMAXPROCS(prev)
			// the searches enqueued merges
			index.waitForMaintenance(t)
			t.Logf("dynamics index=hfresh-muvera maxPostingSize=%d round=%d phase=%s-after-search centroids=%d\n",
				maxPostingSize, round, phase, index.PostingSizes.Count())
		}

		start := time.Now()
		require.NoError(t, index.AddMulti(t.Context(), 0, corpus.doc(0)))
		// as TestNFCorpusMuvera: the posting sizes set directly after the
		// first insert
		index.maxPostingSize = maxPostingSize
		index.minPostingSize = max(maxPostingSize/3, minPostingSizeFloor)
		require.NoError(t, parallelInsert(n-1, workers, func(i int) error {
			return index.AddMulti(t.Context(), uint64(i+1), corpus.doc(i+1))
		}))
		index.waitForMaintenance(t)
		t.Logf("dynamics index=hfresh-muvera maxPostingSize=%d build_s=%.3f\n", maxPostingSize, time.Since(start).Seconds())
		measure(0, "built")

		rng := rand.New(rand.NewSource(seed))
		nextID := uint64(n)
		for round := 1; round <= rounds; round++ {
			victims := rng.Perm(n)[:perRound]

			start = time.Now()
			for _, doc := range victims {
				id := current[doc]
				require.NoError(t, index.DeleteMulti(id))
				docOf[id].Store(-1) // the doc record is dropped
				live[doc] = false
			}
			t.Logf("dynamics index=hfresh-muvera maxPostingSize=%d round=%d delete_s=%.3f\n", maxPostingSize, round, time.Since(start).Seconds())
			measure(round, "deleted")

			start = time.Now()
			ids := make([]uint64, len(victims))
			for i, doc := range victims {
				ids[i] = nextID
				docOf[nextID].Store(int32(doc))
				nextID++
			}
			require.NoError(t, parallelInsert(len(victims), workers, func(i int) error {
				return index.AddMulti(t.Context(), ids[i], corpus.doc(victims[i]))
			}))
			index.waitForMaintenance(t)
			for i, doc := range victims {
				current[doc] = ids[i]
				live[doc] = true
			}
			t.Logf("dynamics index=hfresh-muvera maxPostingSize=%d round=%d reinsert_s=%.3f\n", maxPostingSize, round, time.Since(start).Seconds())
			measure(round, "reinserted")
		}
		require.NoError(t, index.Shutdown(t.Context()))
	}
}

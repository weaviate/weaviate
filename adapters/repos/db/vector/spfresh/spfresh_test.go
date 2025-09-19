//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestSPFreshRecall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RNGFactor = 5.0
	l := logrus.New()
	// l.SetLevel(logrus.DebugLevel)
	cfg.Logger = l

	logger, _ := test.NewNullLogger()

	vectors_size := 10_000
	queries_size := 100
	dimensions := 64
	k := 100

	before := time.Now()
	vectors, queries := testinghelpers.RandomVecsFixedSeed(vectors_size, queries_size, dimensions)

	var mu sync.Mutex

	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		res, _ := testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(cfg.Distancer))
		mu.Lock()
		truths[i] = res
		mu.Unlock()
	})

	fmt.Printf("generating data took %s\n", time.Since(before))

	index, err := New(cfg, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	defer index.Shutdown(t.Context())

	before = time.Now()
	var count atomic.Uint32
	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
		cur := count.Add(1)
		if cur%1000 == 0 {
			fmt.Printf("indexing vectors %d/%d\n", cur, vectors_size)
			fmt.Println("background tasks: split", index.splitCh.Len(), "reassign", index.reassignCh.Len(), "merge", index.mergeCh.Len())
		}
		err := index.Add(t.Context(), id, vectors[id])
		require.NoError(t, err)
	})

	fmt.Printf("indexing done, took: %s, waiting for background tasks...\n", time.Since(before))

	for index.splitCh.Len() > 0 || index.reassignCh.Len() > 0 || index.mergeCh.Len() > 0 {
		fmt.Println("background tasks: split", index.splitCh.Len(), "reassign", index.reassignCh.Len(), "merge", index.mergeCh.Len())

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("all background tasks done,took: %s\n", time.Since(before))

	recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(recall, latency)
	require.Greater(t, recall, float32(0.7))
}

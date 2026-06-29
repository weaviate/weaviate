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

package hnsw

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestKnnSearchByVectorMaxDist_RacesInsertPromotion detects a data race where
// KnnSearchByVectorMaxDist reads h.entryPointID and h.currentMaximumLayer
// without RLock, while insert.go writes them under h.Lock() during promotion.
//
// Run with: go test -race -run TestKnnSearchByVectorMaxDist_RacesInsertPromotion
func TestKnnSearchByVectorMaxDist_RacesInsertPromotion(t *testing.T) {
	const (
		numInserts  = 500
		numSearches = 500
		dims        = 32
	)

	// Pre-generate random vectors for inserts
	rng := rand.New(rand.NewSource(42))
	vectors := make([][]float32, numInserts)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	// Vector lookup function
	vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
		if int(id) < len(vectors) {
			return vectors[id], nil
		}
		return make([]float32, dims), nil
	}

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "race-test-max-dist",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk:      vectorForID,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })

	// Seed one element so searches have something to find
	require.NoError(t, index.Add(context.Background(), 0, vectors[0]))

	ctx := context.Background()
	var wg sync.WaitGroup

	// Writer goroutine: inserts that can promote entrypoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i < numInserts; i++ {
			_ = index.Add(ctx, uint64(i), vectors[i])
		}
	}()

	// Reader goroutine: concurrent KnnSearchByVectorMaxDist calls
	wg.Add(1)
	go func() {
		defer wg.Done()
		searchVec := make([]float32, dims)
		for i := 0; i < numSearches; i++ {
			// Use large maxDist to ensure search proceeds
			_, _ = index.KnnSearchByVectorMaxDist(ctx, searchVec, 100.0, 10, nil)
		}
	}()

	wg.Wait()
}

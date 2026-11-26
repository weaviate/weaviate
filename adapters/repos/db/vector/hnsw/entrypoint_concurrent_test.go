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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Concurrent inserts into a graph whose objects are gone: each one finds only
// the others, all under maintenance. None may fail.
func TestEntrypoint_ConcurrentInsertsIntoGoneGraph(t *testing.T) {
	type testCase struct {
		inserts     int
		compressed  bool
		upperLevels bool // every node but the first one on level 1
	}
	var tests []testCase
	for _, inserts := range []int{2, 8} {
		for _, compressed := range []bool{false, true} {
			for _, upperLevels := range []bool{false, true} {
				tests = append(tests, testCase{inserts, compressed, upperLevels})
			}
		}
	}

	for _, tc := range tests {
		inserts := tc.inserts
		name := fmt.Sprintf("%d inserts/compressed=%v/upperLevels=%v", tc.inserts, tc.compressed, tc.upperLevels)
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const existing = 3
			vectors, _ := testinghelpers.RandomVecsFixedSeed(existing+inserts, 1, 16)
			store := testinghelpers.NewDummyStore(t)
			t.Cleanup(func() { store.Shutdown(ctx) })

			var gone sync.Map
			// all inserts reach the gone entrypoint before any moves on
			var arrived atomic.Int32
			barrier := make(chan struct{})
			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "entrypoint-concurrent",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewL2SquaredProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					if _, ok := gone.Load(id); ok {
						if arrived.Add(1) == int32(inserts) {
							close(barrier)
						}
						<-barrier
						return nil, storobj.NewErrNotFoundf(id, "deleted from object store")
					}
					return vectors[id], nil
				},
				GetViewThunk:                 GetViewThunk,
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				AllocChecker:                 memwatch.NewDummyMonitor(),
				MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
			}, concurrentInsertConfig(tc.compressed), cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			t.Cleanup(func() { index.Drop(ctx) })
			index.PostStartup(ctx)
			index.randFunc = func() float64 { return 1 }
			if tc.upperLevels {
				index.randFunc = func() float64 { return 0.01 }
			}

			for i := range uint64(existing) {
				require.NoError(t, index.Add(ctx, i, vectors[i]))
			}
			require.Equal(t, tc.compressed, index.Compressed())
			if tc.upperLevels {
				require.Positive(t, index.currentMaximumLayer)
			}
			for i := range uint64(existing) {
				gone.Store(i, struct{}{})
				if tc.compressed {
					index.compressor.Delete(ctx, i)
				} else {
					index.cache.Delete(ctx, i)
				}
			}

			errs := make(chan error, inserts)
			ids := make([]uint64, 0, inserts)
			for i := range inserts {
				id := uint64(existing + i)
				ids = append(ids, id)
				go func() { errs <- index.Add(ctx, id, vectors[id]) }()
			}
			for range inserts {
				select {
				case err := <-errs:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("insert did not finish")
				}
			}

			for _, id := range ids {
				found, _, err := index.SearchByVector(ctx, vectors[id], len(vectors), nil)
				require.NoError(t, err)
				require.Subset(t, found, ids, "search from %d", id)
			}
		})
	}
}

func concurrentInsertConfig(compressed bool) ent.UserConfig {
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 16
	uc.EFConstruction = 64
	uc.VectorCacheMaxObjects = 100000
	if compressed {
		uc.RQ = ent.RQConfig{Enabled: true, Bits: 8}
	} else {
		uc.RQ.Enabled = false
		uc.SkipDefaultQuantization = true
	}
	return uc
}

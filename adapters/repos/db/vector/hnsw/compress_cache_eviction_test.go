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
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// evictOnSnapshotCache makes the wrapped cache count as full the moment its
// snapshot is taken, then gives the deletion watcher time to clear it.
type evictOnSnapshotCache struct {
	cache.Cache[float32]
}

func (c *evictOnSnapshotCache) All() [][]float32 {
	c.UpdateMaxSize(1)
	deadline := time.Now().Add(50 * time.Millisecond)
	for c.CountVectors() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	return c.Cache.All()
}

// A full cache is cleared in place by its deletion watcher. Compression reads
// the cache's snapshot, so the watcher must stay out until every cached vector
// is encoded.
func TestCompress_CacheEvictionDuringPreload(t *testing.T) {
	bq := func(uc *ent.UserConfig) { uc.BQ = ent.BQConfig{Enabled: true} }
	rq := func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 8, RescoreLimit: 10} }

	tests := []struct {
		name        string
		multivector bool
		compress    func(*ent.UserConfig)
	}{
		{name: "BQ", compress: bq},
		{name: "RQ", compress: rq},
		{name: "BQ, multivector", multivector: true, compress: bq},
		{name: "RQ, multivector", multivector: true, compress: rq},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const docs = 200
			ctx := context.Background()
			logger, _ := test.NewNullLogger()
			vectors, _ := testinghelpers.RandomVecs(docs, 0, 32)
			doc := func(id uint64) [][]float32 {
				return testMultiVectors[id%uint64(len(testMultiVectors))]
			}

			uc := ent.UserConfig{MaxConnections: 16, EFConstruction: 32, EF: 32, VectorCacheMaxObjects: 1_000_000}
			uc.SetDefaults()
			uc.Multivector.Enabled = tt.multivector
			cfg := Config{
				RootPath:              t.TempDir(),
				ID:                    "compress-cache-eviction",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewL2SquaredProvider(),
				AllocChecker:          memwatch.NewDummyMonitor(),
				MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[id], nil
				},
				GetViewThunk:                 GetViewThunk,
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
			}
			if tt.multivector {
				cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
					return nil, errors.New("can not use VectorForIDThunk with multivector")
				}
				cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
					return doc(id), nil
				}
				cfg.GetViewThunk = func() common.BucketView { return &noopBucketView{} }
				cfg.TempVectorForIDWithViewThunk = nil
			}
			index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			t.Cleanup(func() { index.Shutdown(context.Background()) })

			// same cache as New builds, with a watcher that ticks every millisecond
			index.cache.Drop()
			var inner cache.Cache[float32]
			if tt.multivector {
				inner = cache.NewShardedMultiFloat32LockCache(cfg.MultiVectorForIDThunk, uc.VectorCacheMaxObjects,
					logger, false, time.Millisecond, cfg.AllocChecker)
			} else {
				inner = cache.NewShardedFloat32LockCache(cfg.VectorForIDThunk, cfg.MultiVectorForIDThunk,
					uc.VectorCacheMaxObjects, 1, logger, false, time.Millisecond, cfg.AllocChecker)
			}
			index.cache = &evictOnSnapshotCache{Cache: inner}
			index.vectorForID = index.cache.Get
			index.multiVectorForID = index.cache.MultiGet

			type keys struct{ docID, relativeID uint64 }
			wantKeys := map[keys]struct{}{}
			for id := uint64(0); id < docs; id++ {
				if !tt.multivector {
					require.NoError(t, index.Add(ctx, id, vectors[id]))
					continue
				}
				require.NoError(t, index.AddMulti(ctx, id, doc(id)))
				for relativeID := range doc(id) {
					wantKeys[keys{id, uint64(relativeID)}] = struct{}{}
				}
			}
			cached := index.cache.CountVectors()
			require.Positive(t, cached)
			index.cachePrefilled.Store(true)

			compressed := uc
			tt.compress(&compressed)
			done := make(chan struct{})
			require.NoError(t, index.UpdateUserConfig(compressed, func() { close(done) }))
			<-done

			require.True(t, index.Compressed())
			require.Equal(t, cached, index.compressor.CountVectors(),
				"every cached vector must be encoded")
			if tt.multivector {
				gotKeys := map[keys]struct{}{}
				for nodeID := uint64(0); nodeID < uint64(cached); nodeID++ {
					docID, relativeID := index.compressor.GetKeys(nodeID)
					gotKeys[keys{docID, relativeID}] = struct{}{}
				}
				require.Equal(t, wantKeys, gotKeys, "every vector must keep its document keys")
			}
		})
	}
}

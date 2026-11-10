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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func newCompressRetryIndex(t *testing.T, vectors [][]float32, store *lsmkv.Store,
	async bool, configure func(*ent.UserConfig),
) *hnsw {
	t.Helper()
	uc := ent.UserConfig{MaxConnections: 16, EFConstruction: 32, EF: 32, VectorCacheMaxObjects: 1_000_000}
	uc.SetDefaults()
	configure(&uc)
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "compress-retry",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:  async,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })
	return index
}

// A failed attempt to set up RQ must not be the last one.
func TestRQCompressionRetriedAfterFailure(t *testing.T) {
	ctx := context.Background()
	vectors, _ := testinghelpers.RandomVecs(20, 0, 32)
	store := testinghelpers.NewDummyStore(t)
	index := newCompressRetryIndex(t, vectors, store, false, func(uc *ent.UserConfig) {
		uc.RQ.Enabled = true
		uc.RQ.Bits = 8
	})
	index.PostStartup(ctx)

	// a file in place of the compressed bucket's directory fails its creation
	blocker := filepath.Join(store.GetDir(), helpers.GetCompressedBucketName(""))
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	require.Error(t, index.Add(ctx, 0, vectors[0]))
	require.False(t, index.Compressed())

	require.NoError(t, os.Remove(blocker))
	for i := 1; i < len(vectors); i++ {
		require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, index.Compressed(), "a later insert must set up RQ once the bucket can be created")
}

func TestUpdateUserConfigAppliesCacheSizeWithAsyncIndexing(t *testing.T) {
	for _, async := range []bool{false, true} {
		vectors, _ := testinghelpers.RandomVecs(20, 0, 32)
		index := newCompressRetryIndex(t, vectors, testinghelpers.NewDummyStore(t), async,
			func(uc *ent.UserConfig) { uc.BQ.Enabled = true })
		require.True(t, index.Compressed())

		uc := ent.UserConfig{VectorCacheMaxObjects: 123}
		uc.BQ.Enabled = true
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		require.Equalf(t, int64(123), index.compressor.GetCacheMaxSize(), "async indexing: %v", async)
	}
}

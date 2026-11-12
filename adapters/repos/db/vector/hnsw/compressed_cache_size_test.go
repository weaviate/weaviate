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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestSetCompressedVectorCacheMaxObjects(t *testing.T) {
	tests := []struct {
		name       string
		bq         bool
		compressed bool
	}{
		{name: "compressed", bq: true, compressed: true},
		{name: "uncompressed is a no-op", bq: false, compressed: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors, _ := testinghelpers.RandomVecs(10, 0, 32)
			uc := ent.UserConfig{MaxConnections: 16, EFConstruction: 32, EF: 32, VectorCacheMaxObjects: 1_000_000}
			uc.SetDefaults()
			uc.BQ.Enabled = tt.bq
			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "compressed-cache-size",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewL2SquaredProvider(),
				AllocChecker:          memwatch.NewDummyMonitor(),
				MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[id], nil
				},
				GetViewThunk:                 GetViewThunk,
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
			}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			t.Cleanup(func() { index.Shutdown(context.Background()) })
			require.Equal(t, tt.compressed, index.Compressed())

			index.SetCompressedVectorCacheMaxObjects(123)
			if tt.compressed {
				require.Equal(t, int64(123), index.compressor.GetCacheMaxSize())
			}
		})
	}
}

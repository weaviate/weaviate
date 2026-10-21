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

package flat

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func TestFlat_AddBatch_WrongDimensions(t *testing.T) {
	ctx := context.Background()
	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	config := flatent.UserConfig{}
	config.SetDefaults()

	index, err := New(Config{
		ID:                "flat-test-addbatch-dims",
		RootPath:          t.TempDir(),
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, config, store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	// First batch establishes dimension = 3
	batch1IDs := []uint64{1, 2}
	batch1Vectors := [][]float32{
		{0.1, 0.2, 0.3},
		{0.4, 0.5, 0.6},
	}
	err = index.AddBatch(ctx, batch1IDs, batch1Vectors)
	require.NoError(t, err)

	// Second batch with mismatched dimension = 4 must return common.ErrWrongDimensions
	batch2IDs := []uint64{3, 4}
	batch2Vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7, 0.8},
	}
	err = index.AddBatch(ctx, batch2IDs, batch2Vectors)
	require.Error(t, err)
	assert.True(t, errors.Is(err, common.ErrWrongDimensions))
}

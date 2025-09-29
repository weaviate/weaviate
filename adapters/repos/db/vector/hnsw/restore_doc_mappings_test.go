//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRestoreDocMappingsWithMissingBucket(t *testing.T) {
	rootPath := t.TempDir()
	store := testinghelpers.NewDummyStore(t)

	uc := ent.UserConfig{}
	uc.Multivector.Enabled = true

	index, err := New(Config{
		RootPath:              rootPath,
		ID:                    "doc-mappings",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			return nil, nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)

	assert.Nil(t, err)

	err = index.AddMulti(context.Background(), 1, [][]float32{{1, 2, 3}})
	assert.Nil(t, err)

	newStore := testinghelpers.NewDummyStore(t)
	index.store = newStore
	assert.Nil(t, err)
	err = index.restoreDocMappings()
	assert.ErrorContains(t, err, "multivector mappings bucket not found")
}

func TestRestoreDocMappingsWithNilData(t *testing.T) {
	rootPath := t.TempDir()
	store := testinghelpers.NewDummyStore(t)

	uc := ent.UserConfig{}
	uc.Multivector.Enabled = true

	index, err := New(Config{
		RootPath:              rootPath,
		ID:                    "doc-mappings",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			return nil, nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)

	assert.Nil(t, err)

	err = index.AddMulti(context.Background(), 1, [][]float32{{1, 2, 3}})
	assert.Nil(t, err)
	err = index.AddMulti(context.Background(), 2, [][]float32{{4, 5, 6}, {7, 8, 9}})
	assert.Nil(t, err)
	nodeIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nodeIDBytes, 2)
	err = index.store.Bucket(index.id + "_mv_mappings").Delete(nodeIDBytes)
	assert.Nil(t, err)
	err = index.store.Bucket(index.id+"_mv_mappings").Put(nodeIDBytes, []byte{5})
	require.Nil(t, err)
	err = index.restoreDocMappings()
	require.Nil(t, err)
	assert.Nil(t, index.nodes[2])
}

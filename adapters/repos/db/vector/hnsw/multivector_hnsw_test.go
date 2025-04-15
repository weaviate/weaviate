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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var multiVectors = [][][]float32{
	// Document ID: 0
	{
		{0.3546, 0.3751, 0.8565}, // Relative ID: 0
		{0.7441, 0.6594, 0.1069}, // Relative ID: 1
		{0.3224, 0.9466, 0.0006}, // Relative ID: 2
	},

	// Document ID: 1
	{
		{0.9017, 0.3555, 0.2460}, // Relative ID: 0
		{0.5278, 0.1360, 0.8384}, // Relative ID: 1
	},

	// Document ID: 2
	{
		{0.0817, 0.9565, 0.2802}, // Relative ID: 0
	},
}

var multiQueries = [][][]float32{
	// Query 0
	{
		{0.9054, 0.4201, 0.0613},
	},

	// Query 1
	{
		{0.3491, 0.8591, 0.3742},
		{0.0613, 0.4201, 0.9054},
	},
}

// Expected results for each query
var expectedResults = [][]uint64{
	{1, 0, 2},
	{0, 2, 1},
}

func TestMultiVectorHnsw(t *testing.T) {
	var vectorIndex *hnsw
	ctx := context.Background()
	maxConnections := 8
	efConstruction := 64
	ef := 64
	k := 10

	t.Run("importing into hnsw", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
			},
			MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
				return multiVectors[id], nil
			},
		}, ent.UserConfig{
			VectorCacheMaxObjects: 1e12,
			MaxConnections:        maxConnections,
			EFConstruction:        efConstruction,
			EF:                    ef,
			Multivector:           ent.MultivectorConfig{Enabled: true},
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range multiVectors {
			err := vectorIndex.AddMulti(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("inspect a query", func(t *testing.T) {
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, expectedResults[i], ids)
		}
	})

	t.Run("delete some nodes", func(t *testing.T) {
		// Delete the second node and then add back
		newExpectedResults := [][]uint64{
			{0, 2},
			{0, 2},
		}
		err := vectorIndex.DeleteMulti(1)
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, newExpectedResults[i], ids)
		}
		err = vectorIndex.AddMulti(ctx, 1, multiVectors[1])
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, expectedResults[i], ids)
		}

		// Delete the third node and then add back
		newExpectedResults = [][]uint64{
			{1, 0},
			{0, 1},
		}
		err = vectorIndex.DeleteMulti(2)
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, newExpectedResults[i], ids)
		}
		err = vectorIndex.AddMulti(ctx, 2, multiVectors[2])
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, expectedResults[i], ids)
		}
	})
}

func TestMultiVectorCompressHnsw(t *testing.T) {
	var vectorIndex *hnsw
	maxConnections := 8
	efConstruction := 64
	ef := 64

	userConfigTest := []ent.UserConfig{
		{
			MaxConnections:        maxConnections,
			EFConstruction:        efConstruction,
			EF:                    ef,
			VectorCacheMaxObjects: 1e12,
			Multivector:           ent.MultivectorConfig{Enabled: true},
			PQ: ent.PQConfig{
				Enabled: true,
				Encoder: ent.PQEncoder{
					Type:         ent.PQEncoderTypeKMeans,
					Distribution: ent.PQEncoderDistributionLogNormal,
				},
				Centroids:     256,
				TrainingLimit: 100_000,
			},
		},
		{
			MaxConnections:        maxConnections,
			EFConstruction:        efConstruction,
			EF:                    ef,
			VectorCacheMaxObjects: 1e12,
			Multivector:           ent.MultivectorConfig{Enabled: true},
			SQ:                    ent.SQConfig{Enabled: true},
		},
	}

	t.Run("creating hnsw with compression", func(t *testing.T) {
		for _, userConfig := range userConfigTest {
			_, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewDotProductProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
				},
				MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
					return multiVectors[id], nil
				},
				TempMultiVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
					return multiVectors[id], nil
				},
			}, userConfig, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.Nil(t, err)
		}
	})

	t.Run("compressing hnsw after creation", func(t *testing.T) {
		for _, userConfig := range userConfigTest {
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewDotProductProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
				},
				MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
					return multiVectors[id], nil
				},
				TempMultiVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
					return multiVectors[id], nil
				},
			}, ent.UserConfig{
				VectorCacheMaxObjects: 1e12,
				MaxConnections:        maxConnections,
				EFConstruction:        efConstruction,
				EF:                    ef,
				Multivector:           ent.MultivectorConfig{Enabled: true},
			},
				cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.Nil(t, err)
			vectorIndex = index
			err = vectorIndex.AddMulti(context.Background(), 0, multiVectors[0])
			require.Nil(t, err)
			err = vectorIndex.compress(userConfig)
			require.Nil(t, err)
		}
	})
}

func TestMultiVectorBQHnsw(t *testing.T) {
	var vectorIndex *hnsw
	ctx := context.Background()
	maxConnections := 8
	efConstruction := 64
	ef := 64
	k := 10

	t.Run("importing into hnsw", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				docID, relativeID := vectorIndex.cache.GetKeys(id)
				return multiVectors[docID][relativeID], nil
			},
			TempMultiVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
				return multiVectors[id], nil
			},
		}, ent.UserConfig{
			VectorCacheMaxObjects: 1e12,
			MaxConnections:        maxConnections,
			EFConstruction:        efConstruction,
			EF:                    ef,
			Multivector:           ent.MultivectorConfig{Enabled: true},
			// BQ:             ent.BQConfig{Enabled: true},
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range multiVectors {
			err := vectorIndex.AddMulti(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
		uc := ent.UserConfig{
			VectorCacheMaxObjects: 1e12,
			MaxConnections:        maxConnections,
			EFConstruction:        efConstruction,
			EF:                    ef,
			Multivector: ent.MultivectorConfig{
				Enabled: true,
			},
			BQ: ent.BQConfig{
				Enabled: true,
			},
		}
		err = vectorIndex.compress(uc)
		require.Nil(t, err)
	})

	t.Run("inspect a query", func(t *testing.T) {
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, expectedResults[i], ids)
		}
	})

	t.Run("delete some nodes", func(t *testing.T) {
		// Delete the first node and then add back
		newExpectedResults := [][]uint64{
			{1, 2},
			{2, 1},
		}
		err := vectorIndex.DeleteMulti(0)
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, newExpectedResults[i], ids)
		}
		err = vectorIndex.AddMulti(ctx, 0, multiVectors[0])
		require.Nil(t, err)
		for i, query := range multiQueries {
			ids, _, err := vectorIndex.SearchByMultiVector(ctx, query, k, nil)
			require.Nil(t, err)
			require.Equal(t, expectedResults[i], ids)
		}
	})
}

func TestMultivectorPersistence(t *testing.T) {
	dirName := t.TempDir()
	ctx := context.Background()
	indexID := "integrationtest"
	maxConnections := 8
	efConstruction := 64
	ef := 64
	k := 10

	logger, _ := test.NewNullLogger()
	cl, clErr := NewCommitLogger(dirName, indexID, logger,
		cyclemanager.NewCallbackGroupNoop())
	makeCL := func() (CommitLogger, error) {
		return cl, clErr
	}
	store := testinghelpers.NewDummyStore(t)

	index, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return multiVectors[id], nil
		},
	}, ent.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        maxConnections,
		EFConstruction:        efConstruction,
		EF:                    ef,
		Multivector: ent.MultivectorConfig{
			Enabled: true,
		},
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)

	t.Run("adding nodes", func(t *testing.T) {
		for i, vec := range multiVectors {
			err := index.AddMulti(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
	})

	for i, query := range multiQueries {
		ids, _, err := index.SearchByMultiVector(ctx, query, k, nil)
		require.Nil(t, err)
		require.Equal(t, expectedResults[i], ids)
	}

	require.Nil(t, index.Flush())

	// destroy the index
	index = nil

	fmt.Println("building the second index")
	// build a new index from the (uncondensed) commit log
	secondIndex, err := New(Config{
		RootPath:              dirName,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0}, errors.New("can not use VectorForIDThunk with multivector")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return multiVectors[id], nil
		},
	}, ent.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        maxConnections,
		EFConstruction:        efConstruction,
		EF:                    ef,
		Multivector: ent.MultivectorConfig{
			Enabled: true,
		},
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)

	for i, query := range multiQueries {
		ids, _, err := secondIndex.SearchByMultiVector(ctx, query, k, nil)
		require.Nil(t, err)
		require.Equal(t, expectedResults[i], ids)
	}
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package flat_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := provider.SingleDist(x, y)
		return dist
	}
}

func run(dirName string, logger *logrus.Logger, compression string,
	vectors [][]float32, queries [][]float32, k int, truths [][]uint64,
	extraVectorsForDelete [][]float32, allowIds []uint64,
	distancer distancer.Provider,
) (float32, float32, error) {
	vectors_size := len(vectors)
	queries_size := len(queries)
	runId := uuid.New().String()
	store, err := lsmkv.New(runId, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return 0, 0, err
	}
	err = store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM)
	if err != nil {
		return 0, 0, err
	}
	for k, v := range vectors {
		object := storobj.New(uint64(k))
		id := strfmt.UUID("bf706904-8618-463f-899c-4a2aafd48d56")
		object.SetID(id)
		object.Vector = v
		data, _ := object.MarshalBinary()
		Id := make([]byte, 8)
		binary.LittleEndian.PutUint64(Id, uint64(k))
		store.Bucket(helpers.ObjectsBucketLSM).Put(Id, data)
	}

	index, err := flat.New(hnsw.Config{
		RootPath:              dirName,
		ID:                    runId,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}, flatent.UserConfig{
		Compression: compression,
		EF:          100 * k,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), func() *lsmkv.CursorReplace {
		return store.Bucket(helpers.ObjectsBucketLSM).Cursor()
	})
	if err != nil {
		return 0, 0, err
	}

	ssdhelpers.Concurrently(uint64(vectors_size), func(id uint64) {
		index.Add(id, vectors[id])
	})

	for i := range extraVectorsForDelete {
		index.Add(uint64(vectors_size+i), extraVectorsForDelete[i])
	}

	for i := range extraVectorsForDelete {
		Id := make([]byte, 16)
		binary.BigEndian.PutUint64(Id[8:], uint64(vectors_size+i))
		store.Bucket(helpers.ObjectsBucketLSM).Delete(Id)
		err := index.Delete(uint64(vectors_size + i))
		if err != nil {
			return 0, 0, err
		}
	}

	var relevant uint64
	var retrieved int
	var querying time.Duration = 0

	var allowList helpers.AllowList = nil
	if allowIds != nil {
		allowList = helpers.NewAllowList(allowIds...)
	}
	ssdhelpers.Concurrently(uint64(len(queries)), func(i uint64) {
		before := time.Now()
		results, _, _ := index.SearchByVector(queries[i], k, allowList)
		querying += time.Since(before)
		retrieved += k
		relevant += testinghelpers.MatchesInLists(truths[i], results)
	})

	return float32(relevant) / float32(retrieved), float32(querying.Microseconds()) / float32(queries_size), nil
}

func Test_NoRaceFlatIndex(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	dimensions := 256
	vectors_size := 12000
	queries_size := 100
	k := 10
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	distancer := distancer.NewCosineDistanceProvider()

	truths := make([][]uint64, queries_size)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(vectors, queries[i], k, distanceWrapper(distancer))
	}

	t.Run("recall on no compression", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionNone, vectors, queries, k, truths, nil, nil, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.99)
		assert.True(t, latency < 1_000_000)
	})

	t.Run("recall on compression", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionBQ, vectors, queries, k, truths, nil, nil, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.9)
		assert.True(t, latency < 1_000_000)
	})

	extraVectorsForDelete, _ := testinghelpers.RandomVecs(5_000, 0, dimensions)
	t.Run("recall on no compression with deletes", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionNone, vectors, queries, k, truths, extraVectorsForDelete, nil, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.99)
		assert.True(t, latency < 1_000_000)
	})

	t.Run("recall on compression with deletes", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionBQ, vectors, queries, k, truths, extraVectorsForDelete, nil, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.8)
		assert.True(t, latency < 1_000_000)
	})

	from := 0
	to := 3_000
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(vectors[from:to], queries[i], k, distanceWrapper(distancer))
	}

	allowIds := make([]uint64, 0, to-from)
	for i := uint64(from); i < uint64(to); i++ {
		allowIds = append(allowIds, i)
	}

	t.Run("recall on filtered no compression", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionNone, vectors, queries, k, truths, nil, allowIds, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.99)
		assert.True(t, latency < 1_000_000)
	})

	t.Run("recall on filtered compression", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionBQ, vectors, queries, k, truths, nil, allowIds, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.9)
		assert.True(t, latency < 1_000_000)
	})

	t.Run("recall on filtered no compression with deletes", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionNone, vectors, queries, k, truths, extraVectorsForDelete, allowIds, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.99)
		assert.True(t, latency < 1_000_000)
	})

	t.Run("recall on filtered compression with deletes", func(t *testing.T) {
		recall, latency, err := run(dirName, logger, flatent.CompressionBQ, vectors, queries, k, truths, extraVectorsForDelete, allowIds, distancer)
		require.Nil(t, err)

		fmt.Println(recall, latency)
		assert.True(t, recall > 0.8)
		assert.True(t, latency < 1_000_000)
	})

	err := os.RemoveAll(dirName)
	if err != nil {
		fmt.Println(err)
	}
}

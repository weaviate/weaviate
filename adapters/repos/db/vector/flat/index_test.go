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

//go:build !race

package flat

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func createTestStore(t *testing.T) (*lsmkv.Store, string) {
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store := testStore(t, dirName, logger)
	t.Cleanup(func() {
		store.Shutdown(context.Background())
	})
	return store, dirName
}

func testStore(t *testing.T, dirName string, logger *logrus.Logger) *lsmkv.Store {
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	return store
}

func loadTestStore(t *testing.T, dirName string) *lsmkv.Store {
	logger, _ := test.NewNullLogger()
	return testStore(t, dirName, logger)
}

func run(ctx context.Context, dirName string, logger *logrus.Logger, compression CompressionType, vectorCache bool,
	vectors [][]float32, queries [][]float32, k int, truths [][]uint64,
	extraVectorsForDelete [][]float32, allowIds []uint64,
	distancer distancer.Provider, concurrentCacheReads int,
) (float32, float32, error) {
	vectorsSize := len(vectors)
	queries_size := len(queries)
	runId := uuid.New().String()

	store := testStore(&testing.T{}, dirName, logger)

	defer store.Shutdown(context.Background())

	bq := flatent.CompressionUserConfig{
		Enabled: false,
	}
	rq := flatent.RQUserConfig{
		Enabled: false,
	}
	switch compression {
	case CompressionNone:
		// no action
	case CompressionBQ:
		bq.Enabled = true
		bq.RescoreLimit = 100 * k
		bq.Cache = vectorCache
	case CompressionRQ1:
		rq.Enabled = true
		rq.RescoreLimit = 100 * k
		rq.Cache = vectorCache
	case CompressionRQ8:
		rq.Enabled = true
		rq.RescoreLimit = 100 * k
		rq.Bits = 8
		rq.Cache = vectorCache
	}
	index, err := New(Config{
		ID:               runId,
		RootPath:         dirName,
		DistanceProvider: distancer,
	}, flatent.UserConfig{
		BQ: bq,
		RQ: rq,
	}, store)
	if err != nil {
		return 0, 0, err
	}
	defer index.Shutdown(context.Background())

	if concurrentCacheReads != 0 {
		index.concurrentCacheReads = concurrentCacheReads
	}

	compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	})

	for i := range extraVectorsForDelete {
		index.Add(ctx, uint64(vectorsSize+i), extraVectorsForDelete[i])
	}

	for i := range extraVectorsForDelete {
		Id := make([]byte, 16)
		binary.BigEndian.PutUint64(Id[8:], uint64(vectorsSize+i))
		err := index.Delete(uint64(vectorsSize + i))
		if err != nil {
			return 0, 0, err
		}
	}

	buckets := store.GetBucketsByName()
	for _, bucket := range buckets {
		bucket.FlushMemtable()
	}

	var relevant uint64
	var retrieved int
	var querying time.Duration = 0
	mutex := new(sync.Mutex)

	var allowList helpers.AllowList = nil
	if allowIds != nil {
		allowList = helpers.NewAllowList(allowIds...)
	}
	err = nil
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		before := time.Now()
		results, _, _ := index.SearchByVector(ctx, queries[i], k, allowList)

		since := time.Since(before)
		len := len(results)
		matches := testinghelpers.MatchesInLists(truths[i], results)

		if hasDuplicates(results) {
			err = errors.New("results have duplicates")
		}

		mutex.Lock()
		querying += since
		retrieved += len
		relevant += matches
		mutex.Unlock()
	})

	return float32(relevant) / float32(retrieved), float32(querying.Microseconds()) / float32(queries_size), err
}

func hasDuplicates(results []uint64) bool {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i] == results[j] {
				return true
			}
		}
	}
	return false
}

func Test_NoRaceFlatIndex(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	dimensions := 256
	vectorsSize := 12000
	queries_size := 100
	k := 10
	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	distancer := distancer.NewCosineDistanceProvider()

	truths := make([][]uint64, queries_size)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer))
	}

	extraVectorsForDelete, _ := testinghelpers.RandomVecs(5_000, 0, dimensions)
	for _, compression := range []CompressionType{CompressionNone, CompressionBQ, CompressionRQ1, CompressionRQ8} {
		t.Run("compression: "+compression.String(), func(t *testing.T) {
			compressionDirName := t.TempDir()
			for _, cache := range []bool{false, true} {
				t.Run("cache: "+strconv.FormatBool(cache), func(t *testing.T) {
					if compression == CompressionNone && cache == true {
						return
					}
					targetRecall := float32(0.99)
					if compression == CompressionBQ {
						targetRecall = 0.8
					}
					if compression == CompressionRQ1 {
						targetRecall = 0.8
					}
					if compression == CompressionRQ8 {
						targetRecall = 0.9
					}
					t.Run("recall", func(t *testing.T) {
						recall, latency, err := run(ctx, compressionDirName, logger, compression, cache, vectors, queries, k, truths, nil, nil, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})

					t.Run("recall with deletes", func(t *testing.T) {
						recall, latency, err := run(ctx, compressionDirName, logger, compression, cache, vectors, queries, k, truths, extraVectorsForDelete, nil, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})
				})
			}
		})
	}
	for _, compression := range []CompressionType{CompressionNone, CompressionBQ, CompressionRQ1, CompressionRQ8} {
		t.Run("compression: "+compression.String(), func(t *testing.T) {
			compressionDirName := t.TempDir()
			for _, cache := range []bool{false, true} {
				t.Run("cache: "+strconv.FormatBool(cache), func(t *testing.T) {
					from := 0
					to := 3_000
					for i := range queries {
						truths[i], _ = testinghelpers.BruteForce(logger, vectors[from:to], queries[i], k, distanceWrapper(distancer))
					}

					allowIds := make([]uint64, 0, to-from)
					for i := uint64(from); i < uint64(to); i++ {
						allowIds = append(allowIds, i)
					}
					targetRecall := float32(0.99)
					if compression == CompressionBQ {
						targetRecall = 0.8
					}
					if compression == CompressionRQ1 {
						targetRecall = 0.8
					}
					if compression == CompressionRQ8 {
						targetRecall = 0.9
					}

					t.Run("recall on filtered", func(t *testing.T) {
						recall, latency, err := run(ctx, compressionDirName, logger, compression, cache, vectors, queries, k, truths, nil, allowIds, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})

					t.Run("recall on filtered with deletes", func(t *testing.T) {
						recall, latency, err := run(ctx, compressionDirName, logger, compression, cache, vectors, queries, k, truths, extraVectorsForDelete, allowIds, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})
				})
			}
		})
	}

	err := os.RemoveAll(dirName)
	if err != nil {
		fmt.Println(err)
	}
}

func TestFlat_QueryVectorDistancer(t *testing.T) {
	cases := []struct {
		pq    bool
		cache bool
		bq    bool
		rq    bool
	}{
		{pq: false, cache: false, bq: false, rq: false},
		{pq: true, cache: false, bq: false, rq: false},
		{pq: true, cache: true, bq: false, rq: false},
		{pq: false, cache: false, bq: true, rq: false},
		{pq: false, cache: true, bq: true, rq: false},
		{pq: false, cache: false, bq: false, rq: true},
		{pq: false, cache: true, bq: false, rq: true},
	}
	for _, tt := range cases {
		t.Run("tt.name", func(t *testing.T) {
			dirName := t.TempDir()

			pq := flatent.CompressionUserConfig{
				Enabled: tt.pq, Cache: tt.cache,
			}
			bq := flatent.CompressionUserConfig{
				Enabled: tt.bq, Cache: tt.cache, RescoreLimit: 10,
			}
			rq := flatent.RQUserConfig{
				Enabled: tt.rq, Cache: tt.cache, RescoreLimit: 10,
			}
			store := loadTestStore(t, dirName)

			distancr := distancer.NewCosineDistanceProvider()

			index, err := New(Config{
				ID:               "id",
				RootPath:         t.TempDir(),
				DistanceProvider: distancr,
			}, flatent.UserConfig{
				PQ: pq,
				BQ: bq,
				RQ: rq,
			}, store)
			require.Nil(t, err)

			index.Add(context.TODO(), uint64(0), []float32{-2, 0})

			dist := index.QueryVectorDistancer([]float32{0, 0})
			require.NotNil(t, dist)
			distance, err := dist.DistanceToNode(0)
			require.Nil(t, err)
			require.Equal(t, distance, float32(1.))

			// get distance for non-existing node above default cache size
			_, err = dist.DistanceToNode(1001)
			require.NotNil(t, err)
		})
	}
}

func TestFlat_Preload(t *testing.T) {
	cases := []struct {
		name        string
		distance    string
		compression CompressionType
		cache       bool
	}{
		{name: "no_quantization", distance: "cosine-dot", compression: CompressionNone, cache: false},
		{name: "euclidean_bq", distance: "l2-squared", compression: CompressionBQ, cache: true},
		{name: "cosine_bq_cache", distance: "cosine-dot", compression: CompressionBQ, cache: true},
		{name: "cosine_rq1_cache", distance: "cosine-dot", compression: CompressionRQ1, cache: true},
		{name: "cosine_rq8_cache", distance: "cosine-dot", compression: CompressionRQ8, cache: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()

			var distanceProvider distancer.Provider
			if tt.distance == "cosine-dot" {
				distanceProvider = distancer.NewCosineDistanceProvider()
			} else {
				distanceProvider = distancer.NewL2SquaredProvider()
			}

			var config flatent.UserConfig
			switch tt.compression {
			case CompressionBQ:
				config = flatent.UserConfig{
					BQ: flatent.CompressionUserConfig{
						Enabled: true, Cache: tt.cache, RescoreLimit: 10,
					},
				}
			case CompressionRQ1:
				config = flatent.UserConfig{
					RQ: flatent.RQUserConfig{
						Enabled: true, Cache: tt.cache, RescoreLimit: 10,
					},
				}
			case CompressionRQ8:
				config = flatent.UserConfig{
					RQ: flatent.RQUserConfig{
						Enabled: true, Cache: tt.cache, RescoreLimit: 10, Bits: 8,
					},
				}
			default:
				config = flatent.UserConfig{}
			}

			store := loadTestStore(t, dirName)
			defer store.Shutdown(context.Background())

			index, err := New(Config{
				ID:               "test-preload",
				RootPath:         dirName,
				DistanceProvider: distanceProvider,
			}, config, store)
			require.Nil(t, err)
			defer index.Shutdown(context.Background())

			rawVector1 := []float32{4, -1, 4}
			id1 := uint64(1)

			ctx := context.Background()
			err = index.Add(ctx, id1, rawVector1)
			require.Nil(t, err)

			var quantizedUint64 []uint64
			var quantizedByte []byte

			// Test that vector gets cached after adding
			if index.Cached() {
				if index.quantizer.Type() == Uint64Quantizer {
					quantizedUint64, err = index.cache.GetUint64(ctx, id1)
					require.NoError(t, err)
					require.NotNil(t, quantizedUint64)
				} else if index.quantizer.Type() == ByteQuantizer {
					quantizedByte, err = index.cache.GetBytes(ctx, id1)
					require.NoError(t, err)
					require.NotNil(t, quantizedByte)
				}
			}

			// Ensure deleting removes the cached vector
			index.Delete(id1)
			if index.Cached() {
				var vec interface{}
				if index.quantizer.Type() == Uint64Quantizer {
					vec, err = index.cache.GetUint64(ctx, id1)
				} else if index.quantizer.Type() == ByteQuantizer {
					vec, err = index.cache.GetBytes(ctx, id1)
				}
				require.NoError(t, err)
				require.Nil(t, vec)
			}

			index.Preload(id1, rawVector1)
			if index.Cached() {
				if index.quantizer.Type() == Uint64Quantizer {
					requantizedUint64, err := index.cache.GetUint64(ctx, id1)
					require.NoError(t, err)
					dist, err := index.quantizer.DistanceBetweenUint64Vectors(quantizedUint64, requantizedUint64)
					require.LessOrEqual(t, dist, float32(0.00))
					require.NoError(t, err)
				} else if index.quantizer.Type() == ByteQuantizer {
					requantizedByte, err := index.cache.GetBytes(ctx, id1)
					require.NoError(t, err)
					dist, err := index.quantizer.DistanceBetweenByteVectors(quantizedByte, requantizedByte)
					require.LessOrEqual(t, dist, float32(0.01))
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestConcurrentReads(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	dimensions := 256
	vectorsSize := 12000
	queries_size := 100
	k := 10
	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	distancer := distancer.NewCosineDistanceProvider()

	truths := make([][]uint64, queries_size)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer))
	}

	cores := runtime.GOMAXPROCS(0) * 2

	concurrentReads := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, cores - 1, cores, cores + 1}
	for i := range concurrentReads {
		t.Run("concurrent reads: "+strconv.Itoa(concurrentReads[i]), func(t *testing.T) {
			targetRecall := float32(0.8)
			recall, latency, err := run(ctx, dirName, logger, CompressionRQ1, true, vectors, queries, k, truths, nil, nil, distancer, concurrentReads[i])
			require.Nil(t, err)

			fmt.Println(recall, latency)
			assert.Greater(t, recall, targetRecall)
			assert.Less(t, latency, float32(1_000_000))
		})
	}
}

func TestFlat_Validation(t *testing.T) {
	ctx := t.Context()

	dirName := t.TempDir()

	store := loadTestStore(t, dirName)

	distancr := distancer.NewCosineDistanceProvider()

	index, err := New(Config{
		ID:               "id",
		RootPath:         t.TempDir(),
		DistanceProvider: distancr,
	}, flatent.UserConfig{}, store)
	require.Nil(t, err)

	// call ValidateBeforeInsert before inserting anything
	err = index.ValidateBeforeInsert([]float32{-2, 0})
	require.Nil(t, err)

	// add a vector with 2 dims
	err = index.Add(ctx, uint64(0), []float32{-2, 0})
	require.Nil(t, err)

	// validate before inserting a vector with 2 dim
	err = index.ValidateBeforeInsert([]float32{-1, 0})
	require.NoError(t, err)

	// add again
	err = index.Add(ctx, uint64(0), []float32{-1, 0})
	require.Nil(t, err)

	// validate before inserting a vector with 1 dim
	err = index.ValidateBeforeInsert([]float32{-2})
	require.Error(t, err)

	// add a vector with 1 dim
	err = index.Add(ctx, uint64(0), []float32{-2})
	require.Error(t, err)
}

func TestFlat_ValidateCount(t *testing.T) {
	ctx := t.Context()

	store := testinghelpers.NewDummyStore(t)
	rootPath := t.TempDir()
	defer store.Shutdown(context.Background())

	indexID := "id"
	distancer := distancer.NewCosineDistanceProvider()
	config := flatent.UserConfig{}
	config.SetDefaults()

	index, err := New(Config{
		ID:               indexID,
		RootPath:         rootPath,
		DistanceProvider: distancer,
	}, config, store)
	require.Nil(t, err)
	vectors := [][]float32{{-2, 0}, {-2, 1}}

	for i := range vectors {
		err = index.Add(ctx, uint64(i), vectors[i])
		require.Nil(t, err)
	}

	count := index.AlreadyIndexed()
	require.Equal(t, count, uint64(len(vectors)))
	err = index.store.Bucket(index.getBucketName()).FlushAndSwitch()
	require.Nil(t, err)

	err = index.Shutdown(ctx)
	require.Nil(t, err)
	index = nil

	index, err = New(Config{
		ID:               indexID,
		RootPath:         rootPath,
		DistanceProvider: distancer,
	}, config, store)
	require.Nil(t, err)
	newCount := index.AlreadyIndexed()
	require.Equal(t, count, newCount)
}

func TestFlat_RQPersistence(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	store := loadTestStore(t, dirName)
	defer store.Shutdown(context.Background())

	distancer := distancer.NewCosineDistanceProvider()

	// Create index with RQ enabled
	rq := flatent.RQUserConfig{
		Enabled:      true,
		Cache:        false,
		RescoreLimit: 10,
	}
	index, err := New(Config{
		ID:               "test-rq",
		RootPath:         dirName,
		DistanceProvider: distancer,
	}, flatent.UserConfig{
		RQ: rq,
	}, store)
	require.Nil(t, err)
	defer index.Shutdown(context.Background())

	// Add a vector to trigger RQ initialization
	testVector := []float32{1.0, 2.0, 3.0, 4.0}
	err = index.Add(ctx, 1, testVector)
	require.Nil(t, err)

	// Verify RQ quantizer was created
	require.NotNil(t, index.quantizer)

	// Test encoding
	var encoded interface{}
	if index.quantizer.Type() == Uint64Quantizer {
		encoded = index.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded.([]uint64)), 0)
	} else if index.quantizer.Type() == ByteQuantizer {
		encoded = index.quantizer.EncodeBytes(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded.([]byte)), 0)
	}

	// Test distance calculation - RQ distances can be negative
	var distance float32
	var distanceErr error
	if index.quantizer.Type() == Uint64Quantizer {
		encodedVec := encoded.([]uint64)
		distance, distanceErr = index.quantizer.DistanceBetweenUint64Vectors(encodedVec, encodedVec)
	} else if index.quantizer.Type() == ByteQuantizer {
		encodedVec := encoded.([]byte)
		distance, distanceErr = index.quantizer.DistanceBetweenByteVectors(encodedVec, encodedVec)
	}
	require.Nil(t, distanceErr)
	require.IsType(t, float32(0), distance) // Just verify it returns a float32

	// Store the original distance for comparison after restart
	originalDistance := distance

	// Shutdown and recreate index to test persistence
	index.Shutdown(context.Background())
	store.Shutdown(context.Background())

	// Recreate store and index
	store2 := loadTestStore(t, dirName)
	defer store2.Shutdown(context.Background())

	index2, err := New(Config{
		ID:               "test-rq",
		RootPath:         dirName,
		DistanceProvider: distancer,
	}, flatent.UserConfig{
		RQ: rq,
	}, store2)
	require.Nil(t, err)
	defer index2.Shutdown(context.Background())

	// Verify RQ quantizer was restored
	require.NotNil(t, index2.quantizer)

	// Test that the restored quantizer works
	var encoded2 interface{}
	if index2.quantizer.Type() == Uint64Quantizer {
		encoded2 = index2.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded2)
	} else if index2.quantizer.Type() == ByteQuantizer {
		encoded2 = index2.quantizer.EncodeBytes(testVector)
		require.NotNil(t, encoded2)
	}

	// The encoded vectors should be identical (same seed)
	if index.quantizer.Type() == Uint64Quantizer {
		encodedVec := encoded.([]uint64)
		encodedVec2 := encoded2.([]uint64)
		require.Equal(t, len(encodedVec), len(encodedVec2))
		for i := range encodedVec {
			require.Equal(t, encodedVec[i], encodedVec2[i])
		}
	} else if index.quantizer.Type() == ByteQuantizer {
		encodedVec := encoded.([]byte)
		encodedVec2 := encoded2.([]byte)
		require.Equal(t, len(encodedVec), len(encodedVec2))
		for i := range encodedVec {
			require.Equal(t, encodedVec[i], encodedVec2[i])
		}
	}

	// Test distance calculation with restored quantizer
	var distance2 float32
	var distance2Err error
	if index2.quantizer.Type() == Uint64Quantizer {
		encodedVec2 := encoded2.([]uint64)
		distance2, distance2Err = index2.quantizer.DistanceBetweenUint64Vectors(encodedVec2, encodedVec2)
	} else if index2.quantizer.Type() == ByteQuantizer {
		encodedVec2 := encoded2.([]byte)
		distance2, distance2Err = index2.quantizer.DistanceBetweenByteVectors(encodedVec2, encodedVec2)
	}
	require.Nil(t, distance2Err)
	require.IsType(t, float32(0), distance2) // Just verify it returns a float32

	// Verify that the distance calculation is consistent after restart
	require.Equal(t, originalDistance, distance2, "Distance calculation should be consistent after restart")
}

func TestQuantizerInterfaces(t *testing.T) {
	ctx := context.Background()
	distancer := distancer.NewCosineDistanceProvider()
	testVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0}

	t.Run("BinaryQuantizerWrapper", func(t *testing.T) {
		store, dirName := createTestStore(t)
		// Test BQ quantizer
		bq := flatent.CompressionUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-bq",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			BQ: bq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Add vector to initialize quantizer
		err = index.Add(ctx, 1, testVector)
		require.Nil(t, err)

		require.NotNil(t, index.quantizer)
		require.Equal(t, Uint64Quantizer, index.quantizer.Type())

		// Test encoding
		encoded := index.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 0)

		// Test byte encoding should return nil for BQ
		encodedBytes := index.quantizer.EncodeBytes(testVector)
		require.Nil(t, encodedBytes)

		// Test distance calculation
		distance, err := index.quantizer.DistanceBetweenUint64Vectors(encoded, encoded)
		require.Nil(t, err)
		require.IsType(t, float32(0), distance)

		// Test byte distance should return error
		_, err = index.quantizer.DistanceBetweenByteVectors([]byte{}, []byte{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "binary quantizer does not support byte vectors")

		// Test FromCompressedBytes methods with proper data
		// Create proper compressed data by encoding a test vector
		testData := []float32{1.0, 2.0, 3.0, 4.0}
		compressedData := index.quantizer.EncodeUint64(testData)

		// Convert to bytes for testing
		compressedBytes := make([]byte, len(compressedData)*8)
		for i, val := range compressedData {
			binary.BigEndian.PutUint64(compressedBytes[i*8:], val)
		}

		var buffer []uint64
		result := index.quantizer.FromCompressedBytesToUint64(compressedBytes, &buffer)
		require.NotNil(t, result)

		var byteBuffer []byte
		resultBytes := index.quantizer.FromCompressedBytesToBytes(compressedBytes, &byteBuffer)
		require.Nil(t, resultBytes)
	})

	t.Run("BinaryRotationalQuantizerWrapper", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Test RQ1 quantizer
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-rq1",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())
		testData := []float32{1.0, 2.0, 3.0, 4.0}

		index.Add(context.Background(), 0, testData)
		require.NotNil(t, index.quantizer)
		require.Equal(t, Uint64Quantizer, index.quantizer.Type())

		// Test encoding
		encoded := index.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 0)

		// Test byte encoding should return nil for RQ1
		encodedBytes := index.quantizer.EncodeBytes(testVector)
		require.Nil(t, encodedBytes)

		// Test distance calculation
		distance, err := index.quantizer.DistanceBetweenUint64Vectors(encoded, encoded)
		require.Nil(t, err)
		require.IsType(t, float32(0), distance)

		// Test byte distance should return error
		_, err = index.quantizer.DistanceBetweenByteVectors([]byte{}, []byte{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "rotational quantizer does not support byte vectors")

		// Test FromCompressedBytes methods with proper data

		compressedData := index.quantizer.EncodeUint64(testData)

		// Convert to bytes for testing
		compressedBytes := make([]byte, len(compressedData)*8)
		for i, val := range compressedData {
			binary.BigEndian.PutUint64(compressedBytes[i*8:], val)
		}

		var buffer []uint64
		result := index.quantizer.FromCompressedBytesToUint64(compressedBytes, &buffer)
		require.NotNil(t, result)

		var byteBuffer []byte
		resultBytes := index.quantizer.FromCompressedBytesToBytes(compressedBytes, &byteBuffer)
		require.Nil(t, resultBytes)
	})

	t.Run("RotationalQuantizerWrapper", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Test RQ8 quantizer
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
			Bits:         8,
		}
		index, err := New(Config{
			ID:               "test-rq8",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())
		testData := []float32{1.0, 2.0, 3.0, 4.0}

		index.Add(context.Background(), 0, testData)
		require.NotNil(t, index.quantizer)
		require.Equal(t, ByteQuantizer, index.quantizer.Type())

		// Test encoding
		encoded := index.quantizer.EncodeBytes(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 0)

		// Test uint64 encoding should return nil for RQ8
		encodedUint64 := index.quantizer.EncodeUint64(testVector)
		require.Nil(t, encodedUint64)

		// Test distance calculation
		distance, err := index.quantizer.DistanceBetweenByteVectors(encoded, encoded)
		require.Nil(t, err)
		require.IsType(t, float32(0), distance)

		// Test uint64 distance should return error
		_, err = index.quantizer.DistanceBetweenUint64Vectors([]uint64{}, []uint64{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "byte quantizer does not support uint64 vectors")

		// Test FromCompressedBytes methods with proper data
		compressedData := index.quantizer.EncodeBytes(testData)

		var buffer []byte
		result := index.quantizer.FromCompressedBytesToBytes(compressedData, &buffer)
		require.NotNil(t, result)

		var uint64Buffer []uint64
		resultUint64 := index.quantizer.FromCompressedBytesToUint64(compressedData, &uint64Buffer)
		require.Nil(t, resultUint64)
	})
}

func TestCompressionTypes(t *testing.T) {
	t.Run("String method", func(t *testing.T) {
		require.Equal(t, "none", CompressionNone.String())
		require.Equal(t, "bq", CompressionBQ.String())
		require.Equal(t, "rq-1", CompressionRQ1.String())
		require.Equal(t, "rq-8", CompressionRQ8.String())
	})

	t.Run("IsQuantized method", func(t *testing.T) {
		require.False(t, CompressionNone.IsQuantized())
		require.True(t, CompressionBQ.IsQuantized())
		require.True(t, CompressionRQ1.IsQuantized())
		require.True(t, CompressionRQ8.IsQuantized())
	})
}

func TestQuantizerBuilder(t *testing.T) {
	distancer := distancer.NewCosineDistanceProvider()
	builder := NewQuantizerBuilder(distancer)

	type testCase struct {
		name              string
		compressionType   CompressionType
		expectedQuantizer *QuantizerType
		shouldBeNil       bool
		testEncoding      func(t *testing.T, quantizer Quantizer)
	}

	testVector := []float32{1.0, 2.0, 3.0, 4.0}
	uint64Quantizer := Uint64Quantizer
	byteQuantizer := ByteQuantizer

	testCases := []testCase{
		{
			name:              "CreateQuantizer for BQ",
			compressionType:   CompressionBQ,
			expectedQuantizer: &uint64Quantizer,
			shouldBeNil:       false,
			testEncoding: func(t *testing.T, quantizer Quantizer) {
				encoded := quantizer.EncodeUint64(testVector)
				require.NotNil(t, encoded)
				require.Greater(t, len(encoded), 0)
			},
		},
		{
			name:              "CreateQuantizer for RQ1",
			compressionType:   CompressionRQ1,
			expectedQuantizer: &uint64Quantizer,
			shouldBeNil:       false,
			testEncoding: func(t *testing.T, quantizer Quantizer) {
				encoded := quantizer.EncodeUint64(testVector)
				require.NotNil(t, encoded)
				require.Greater(t, len(encoded), 0)
			},
		},
		{
			name:              "CreateQuantizer for RQ8",
			compressionType:   CompressionRQ8,
			expectedQuantizer: &byteQuantizer,
			shouldBeNil:       false,
			testEncoding: func(t *testing.T, quantizer Quantizer) {
				encoded := quantizer.EncodeBytes(testVector)
				require.NotNil(t, encoded)
				require.Greater(t, len(encoded), 0)
			},
		},
		{
			name:              "CreateQuantizer for unsupported CompressionNone",
			compressionType:   CompressionNone,
			expectedQuantizer: nil,
			shouldBeNil:       true,
			testEncoding:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			quantizer := builder.CreateQuantizer(tc.compressionType, 128)

			if tc.shouldBeNil {
				require.Nil(t, quantizer)
			} else {
				require.NotNil(t, quantizer)
				require.NotNil(t, tc.expectedQuantizer)
				require.Equal(t, *tc.expectedQuantizer, quantizer.Type())

				if tc.testEncoding != nil {
					tc.testEncoding(t, quantizer)
				}
			}
		})
	}
}

func TestCacheFunctionality(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	t.Run("Uint64Cache", func(t *testing.T) {
		getUint64Vector := func(ctx context.Context, id uint64) ([]uint64, error) {
			return []uint64{1, 2, 3, 4}, nil
		}
		getByteVector := func(ctx context.Context, id uint64) ([]byte, error) {
			return []byte{1, 2, 3, 4}, nil
		}

		cache := NewCache(getUint64Vector, getByteVector, 100, logger, nil, Uint64Quantizer)
		require.NotNil(t, cache)

		// Test Grow
		cache.Grow(10)

		// Test PreloadUint64
		cache.PreloadUint64(1, []uint64{1, 2, 3, 4})

		// Test PreloadBytes should be no-op for Uint64Quantizer
		cache.PreloadBytes(1, []byte{1, 2, 3, 4})

		// Test Delete
		cache.Delete(ctx, 1, 2, 3)

		// Test Len
		length := cache.Len()
		require.GreaterOrEqual(t, length, int32(0))

		// Test PageSize
		pageSize := cache.PageSize()
		require.Greater(t, pageSize, uint64(0))

		// Test GetUint64
		vec, err := cache.GetUint64(ctx, 1)
		require.Nil(t, err)
		require.NotNil(t, vec)

		// Test GetBytes should return error for Uint64Quantizer
		_, err = cache.GetBytes(ctx, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "byte cache not available")

		// Test LockAll/UnlockAll
		cache.LockAll()
		cache.UnlockAll()

		// Test SetSizeAndGrowNoLockUint64
		cache.SetSizeAndGrowNoLockUint64(100)

		// Test PreloadNoLockUint64
		cache.PreloadNoLockUint64(1, []uint64{1, 2, 3, 4})

		// Test PreloadNoLockBytes should be no-op
		cache.PreloadNoLockBytes(1, []byte{1, 2, 3, 4})
	})

	t.Run("ByteCache", func(t *testing.T) {
		getUint64Vector := func(ctx context.Context, id uint64) ([]uint64, error) {
			return []uint64{1, 2, 3, 4}, nil
		}
		getByteVector := func(ctx context.Context, id uint64) ([]byte, error) {
			return []byte{1, 2, 3, 4}, nil
		}

		cache := NewCache(getUint64Vector, getByteVector, 100, logger, nil, ByteQuantizer)
		require.NotNil(t, cache)

		// Test Grow
		cache.Grow(10)

		// Test PreloadBytes
		cache.PreloadBytes(1, []byte{1, 2, 3, 4})

		// Test PreloadUint64 should be no-op for ByteQuantizer
		cache.PreloadUint64(1, []uint64{1, 2, 3, 4})

		// Test Delete
		cache.Delete(ctx, 1, 2, 3)

		// Test Len
		length := cache.Len()
		require.GreaterOrEqual(t, length, int32(0))

		// Test PageSize
		pageSize := cache.PageSize()
		require.Greater(t, pageSize, uint64(0))

		// Test GetBytes
		vec, err := cache.GetBytes(ctx, 1)
		require.Nil(t, err)
		require.NotNil(t, vec)

		// Test GetUint64 should return error for ByteQuantizer
		_, err = cache.GetUint64(ctx, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uint64 cache not available")

		// Test LockAll/UnlockAll
		cache.LockAll()
		cache.UnlockAll()

		// Test SetSizeAndGrowNoLockBytes
		cache.SetSizeAndGrowNoLockBytes(100)

		// Test PreloadNoLockBytes
		cache.PreloadNoLockBytes(1, []byte{1, 2, 3, 4})

		// Test PreloadNoLockUint64 should be no-op
		cache.PreloadNoLockUint64(1, []uint64{1, 2, 3, 4})
	})
}

func TestQuantizedSearchFunctionality(t *testing.T) {
	ctx := context.Background()
	distancer := distancer.NewCosineDistanceProvider()
	testVectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
		{0.0, 0.0, 0.0, 1.0},
	}
	queryVector := []float32{1.0, 0.0, 0.0, 0.0}

	type testCase struct {
		name            string
		createConfig    func() flatent.UserConfig
		validateResults func(t *testing.T, results []uint64, distances []float32)
	}

	testCases := []testCase{
		{
			name: "BQ Search",
			createConfig: func() flatent.UserConfig {
				bq := flatent.CompressionUserConfig{
					Enabled:      true,
					Cache:        false,
					RescoreLimit: 10,
				}
				return flatent.UserConfig{
					BQ: bq,
				}
			},
			validateResults: func(t *testing.T, results []uint64, distances []float32) {
				// First result should be the exact match (ID 0)
				require.Equal(t, uint64(0), results[0])
				require.Equal(t, float32(0), distances[0])
			},
		},
		{
			name: "RQ1 Search",
			createConfig: func() flatent.UserConfig {
				rq := flatent.RQUserConfig{
					Enabled:      true,
					Cache:        false,
					RescoreLimit: 10,
				}
				return flatent.UserConfig{
					RQ: rq,
				}
			},
			validateResults: func(t *testing.T, results []uint64, distances []float32) {
				// Results should be reasonable (exact match might not be first due to quantization)
				require.Contains(t, results, uint64(0))
			},
		},
		{
			name: "RQ8 Search",
			createConfig: func() flatent.UserConfig {
				rq := flatent.RQUserConfig{
					Enabled:      true,
					Cache:        false,
					RescoreLimit: 10,
					Bits:         8,
				}
				return flatent.UserConfig{
					RQ: rq,
				}
			},
			validateResults: func(t *testing.T, results []uint64, distances []float32) {
				// Results should be reasonable
				require.Contains(t, results, uint64(0))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store, dirName := createTestStore(t)
			config := tc.createConfig()

			index, err := New(Config{
				ID:               "test-" + strings.ToLower(strings.ReplaceAll(tc.name, " ", "-")),
				RootPath:         dirName,
				DistanceProvider: distancer,
			}, config, store)
			require.Nil(t, err)
			defer index.Shutdown(context.Background())

			// Add vectors
			for i, vec := range testVectors {
				err = index.Add(ctx, uint64(i), vec)
				require.Nil(t, err)
			}

			// Search
			results, distances, err := index.SearchByVector(ctx, queryVector, 2, nil)
			require.Nil(t, err)
			require.Len(t, results, 2)
			require.Len(t, distances, 2)

			// Validate results using the test case specific validation
			tc.validateResults(t, results, distances)
		})
	}
}

func TestEdgeCases(t *testing.T) {
	ctx := context.Background()
	distancer := distancer.NewCosineDistanceProvider()

	t.Run("Empty vector handling", func(t *testing.T) {
		store, dirName := createTestStore(t)

		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-empty-vector",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Try to add empty vector - should fail validation
		err = index.ValidateBeforeInsert([]float32{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot insert vector of dimension 0")

		// Adding empty vector should also fail
		err = index.Add(ctx, 1, []float32{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot insert vector of dimension 0")
	})

	t.Run("Single dimension vector", func(t *testing.T) {
		store, dirName := createTestStore(t)

		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-single-dim",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Add single dimension vector
		err = index.Add(ctx, 1, []float32{1.0})
		require.Nil(t, err)

		// Search should work
		results, distances, err := index.SearchByVector(ctx, []float32{1.0}, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
	})

	t.Run("Large dimension vector", func(t *testing.T) {
		store, dirName := createTestStore(t)

		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-large-dim",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Create large dimension vector
		largeVector := make([]float32, 1000)
		for i := range largeVector {
			largeVector[i] = float32(i) / 1000.0
		}

		// Add large dimension vector
		err = index.Add(ctx, 1, largeVector)
		require.Nil(t, err)

		// Search should work
		results, distances, err := index.SearchByVector(ctx, largeVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
	})

	t.Run("Zero vector", func(t *testing.T) {
		store, dirName := createTestStore(t)

		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-zero-vector",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Add zero vector
		zeroVector := make([]float32, 4)
		err = index.Add(ctx, 1, zeroVector)
		require.Nil(t, err)

		// Search should work
		results, distances, err := index.SearchByVector(ctx, zeroVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
	})

	t.Run("Negative values", func(t *testing.T) {
		store, dirName := createTestStore(t)

		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-negative-values",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)
		defer index.Shutdown(context.Background())

		// Add vector with negative values
		negativeVector := []float32{-1.0, -2.0, 3.0, -4.0}
		err = index.Add(ctx, 1, negativeVector)
		require.Nil(t, err)

		// Search should work
		results, distances, err := index.SearchByVector(ctx, negativeVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
	})
}

func TestEmptyIndexRestoration(t *testing.T) {
	ctx := context.Background()
	distancer := distancer.NewCosineDistanceProvider()

	t.Run("Empty index restoration does not panic", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Create index with RQ enabled but don't add any vectors
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-empty-index",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)

		// Shutdown without adding any vectors
		index.Shutdown(context.Background())
		store.Shutdown(context.Background())

		// Recreate store and index - this should not panic
		store2 := loadTestStore(t, dirName)
		defer store2.Shutdown(context.Background())

		index2, err := New(Config{
			ID:               "test-empty-index",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store2)
		require.Nil(t, err)
		defer index2.Shutdown(context.Background())

		// Verify we can still add vectors to the restored empty index
		testVector := []float32{1.0, 2.0, 3.0, 4.0}
		err = index2.Add(ctx, 1, testVector)
		require.Nil(t, err)

		// Search should work
		results, distances, err := index2.SearchByVector(ctx, testVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
		require.Equal(t, uint64(1), results[0])
	})
}

func TestRQ1PersistenceAndRestoration(t *testing.T) {
	ctx := context.Background()
	distancer := distancer.NewCosineDistanceProvider()
	testVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0}

	t.Run("RQ1 Persistence and Restoration", func(t *testing.T) {
		store, dirName := createTestStore(t)
		// Create index with RQ1 enabled
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
			Bits:         1,
		}
		index, err := New(Config{
			ID:               "test-rq1-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)

		// Add vector to trigger RQ initialization and persistence
		err = index.Add(ctx, 1, testVector)
		require.Nil(t, err)

		// Verify RQ quantizer was created
		require.NotNil(t, index.quantizer)
		require.Equal(t, Uint64Quantizer, index.quantizer.Type())

		// Test encoding and distance calculation
		encoded := index.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 0)

		distance, err := index.quantizer.DistanceBetweenUint64Vectors(encoded, encoded)
		require.Nil(t, err)
		require.IsType(t, float32(0), distance)

		// Store original values for comparison
		originalEncoded := make([]uint64, len(encoded))
		copy(originalEncoded, encoded)
		originalDistance := distance

		// Shutdown and recreate index to test persistence
		index.Shutdown(context.Background())
		store.Shutdown(context.Background())

		// Recreate store and index
		store2 := loadTestStore(t, dirName)
		defer store2.Shutdown(context.Background())

		index2, err := New(Config{
			ID:               "test-rq1-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store2)
		require.Nil(t, err)
		defer index2.Shutdown(context.Background())

		// Verify RQ quantizer was restored
		require.NotNil(t, index2.quantizer)
		require.Equal(t, Uint64Quantizer, index2.quantizer.Type())

		// Test that the restored quantizer produces identical results
		encoded2 := index2.quantizer.EncodeUint64(testVector)
		require.NotNil(t, encoded2)
		require.Equal(t, len(originalEncoded), len(encoded2))

		// Verify encoded vectors are identical before and after shutdown
		for i := range originalEncoded {
			require.Equal(t, originalEncoded[i], encoded2[i], "Encoding should produce identical results before and after shutdown")
		}

		// Test distance calculation with restored quantizer
		distance2, err := index2.quantizer.DistanceBetweenUint64Vectors(encoded2, encoded2)
		require.Nil(t, err)
		require.Equal(t, originalDistance, distance2, "Distance calculation should be consistent after restart")

		// Test that search still works with restored quantizer
		results, distances, err := index2.SearchByVector(ctx, testVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
		require.Equal(t, uint64(1), results[0])
	})

	t.Run("RQ8 Persistence and Restoration", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Create index with RQ8 enabled
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
			Bits:         8,
		}
		index, err := New(Config{
			ID:               "test-rq8-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)

		// Add vector to trigger RQ initialization and persistence
		err = index.Add(ctx, 1, testVector)
		require.Nil(t, err)

		// Verify RQ quantizer was created
		require.NotNil(t, index.quantizer)
		require.Equal(t, ByteQuantizer, index.quantizer.Type())

		// Test encoding and distance calculation
		encoded := index.quantizer.EncodeBytes(testVector)
		require.NotNil(t, encoded)
		require.Greater(t, len(encoded), 0)

		distance, err := index.quantizer.DistanceBetweenByteVectors(encoded, encoded)
		require.Nil(t, err)
		require.IsType(t, float32(0), distance)

		// Store original values for comparison
		originalEncoded := make([]byte, len(encoded))
		copy(originalEncoded, encoded)
		originalDistance := distance

		// Shutdown and recreate index to test persistence
		index.Shutdown(context.Background())
		store.Shutdown(context.Background())

		// Recreate store and index
		store2 := loadTestStore(t, dirName)
		defer store2.Shutdown(context.Background())

		index2, err := New(Config{
			ID:               "test-rq8-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store2)
		require.Nil(t, err)
		defer index2.Shutdown(context.Background())

		// Verify RQ quantizer was restored
		require.NotNil(t, index2.quantizer)
		require.Equal(t, ByteQuantizer, index2.quantizer.Type())

		// Test that the restored quantizer produces identical results
		encoded2 := index2.quantizer.EncodeBytes(testVector)
		require.NotNil(t, encoded2)
		require.Equal(t, len(originalEncoded), len(encoded2))

		// Verify encoded vectors are identical before and after shutdown
		for i := range originalEncoded {
			require.Equal(t, originalEncoded[i], encoded2[i], "Encoding should produce identical results before and after shutdown")
		}

		// Test distance calculation with restored quantizer
		distance2, err := index2.quantizer.DistanceBetweenByteVectors(encoded2, encoded2)
		require.Nil(t, err)
		require.Equal(t, originalDistance, distance2, "Distance calculation should be consistent after restart")

		// Test that search still works with restored quantizer
		results, distances, err := index2.SearchByVector(ctx, testVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)
		require.Equal(t, uint64(1), results[0])
	})

	t.Run("RQ Persistence with Multiple Vectors", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Create index with RQ1 enabled
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        false,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-rq-multi-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)

		// Add multiple vectors
		testVectors := [][]float32{
			{1.0, 2.0, 3.0, 4.0},
			{2.0, 3.0, 4.0, 5.0},
			{3.0, 4.0, 5.0, 6.0},
		}

		for i, vec := range testVectors {
			err = index.Add(ctx, uint64(i+1), vec)
			require.Nil(t, err)
		}

		// Verify quantizer was created
		require.NotNil(t, index.quantizer)

		// Test search before persistence
		results, distances, err := index.SearchByVector(ctx, testVectors[0], 2, nil)
		require.Nil(t, err)
		require.Len(t, results, 2)
		require.Len(t, distances, 2)

		// Store original results for comparison
		originalResults := make([]uint64, len(results))
		copy(originalResults, results)
		originalDistances := make([]float32, len(distances))
		copy(originalDistances, distances)

		// Shutdown and recreate index
		index.Shutdown(context.Background())
		store.Shutdown(context.Background())

		// Recreate store and index
		store2 := loadTestStore(t, dirName)
		defer store2.Shutdown(context.Background())

		index2, err := New(Config{
			ID:               "test-rq-multi-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store2)
		require.Nil(t, err)
		defer index2.Shutdown(context.Background())

		// Verify quantizer was restored
		require.NotNil(t, index2.quantizer)

		// Test search after restoration
		results2, distances2, err := index2.SearchByVector(ctx, testVectors[0], 2, nil)
		require.Nil(t, err)
		require.Len(t, results2, 2)
		require.Len(t, distances2, 2)

		// Results should be identical
		require.Equal(t, originalResults, results2)
		require.Equal(t, originalDistances, distances2)
	})

	t.Run("RQ Persistence with Cache Enabled", func(t *testing.T) {
		store, dirName := createTestStore(t)

		// Create index with RQ1 enabled and cache
		rq := flatent.RQUserConfig{
			Enabled:      true,
			Cache:        true,
			RescoreLimit: 10,
		}
		index, err := New(Config{
			ID:               "test-rq-cache-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store)
		require.Nil(t, err)

		// Add vector to trigger RQ initialization
		err = index.Add(ctx, 1, testVector)
		require.Nil(t, err)

		// Verify quantizer and cache were created
		require.NotNil(t, index.quantizer)
		require.NotNil(t, index.cache)
		require.True(t, index.Cached())

		// Test search with cache
		results, distances, err := index.SearchByVector(ctx, testVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results, 1)
		require.Len(t, distances, 1)

		// Shutdown and recreate index
		index.Shutdown(context.Background())
		store.Shutdown(context.Background())

		// Recreate store and index
		store2 := loadTestStore(t, dirName)
		defer store2.Shutdown(context.Background())

		index2, err := New(Config{
			ID:               "test-rq-cache-persistence",
			RootPath:         dirName,
			DistanceProvider: distancer,
		}, flatent.UserConfig{
			RQ: rq,
		}, store2)
		require.Nil(t, err)
		defer index2.Shutdown(context.Background())

		// Verify quantizer was restored
		require.NotNil(t, index2.quantizer)

		// Cache should be recreated but empty initially
		require.NotNil(t, index2.cache)

		// Test search after restoration
		results2, distances2, err := index2.SearchByVector(ctx, testVector, 1, nil)
		require.Nil(t, err)
		require.Len(t, results2, 1)
		require.Len(t, distances2, 1)
		require.Equal(t, uint64(1), results2[0])
	})
}

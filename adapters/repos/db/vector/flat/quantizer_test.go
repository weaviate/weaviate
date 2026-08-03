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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// Helpers are self-contained rather than shared with index_test.go: that file
// is excluded from race builds (//go:build !race), and these tests must run
// under the race detector.
func newRQTestIndex(t *testing.T, bits int, cache bool) *flat {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	index, err := New(Config{
		ID:                "quantizer-init-test",
		RootPath:          dirName,
		DistanceProvider:  distancer.NewL2SquaredProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, flatent.UserConfig{
		RQ: flatent.RQUserConfig{
			Enabled:      true,
			Bits:         bits,
			RescoreLimit: 10,
			Cache:        cache,
		},
	}, store)
	require.NoError(t, err)
	t.Cleanup(func() { index.Shutdown(context.Background()) })
	return index
}

// simulateInterruptedInit puts the index into the state a concurrent searcher
// can observe while initializeDimensionsAndRQ is still running (or after it
// failed partway): dims already published, quantizer not yet assigned. The
// given vectors exist in the uncompressed bucket only.
func simulateInterruptedInit(index *flat, vectors [][]float32) {
	atomic.StoreInt32(&index.dims, int32(len(vectors[0])))
	for i, vector := range vectors {
		vector = index.normalized(vector)
		slice := make([]byte, len(vector)*4)
		index.storeVector(uint64(i), byteSliceFromFloat32Slice(vector, slice))
	}
}

func Test_FlatRQSearchBeforeQuantizerInit(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{1, 2, 3, 4},
		{4, 3, 2, 1},
		{0, 1, 0, 1},
	}

	cases := []struct {
		name string
		bits int
	}{
		{name: "rq1", bits: 1},
		{name: "rq8", bits: 8},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			index := newRQTestIndex(t, tc.bits, false)
			simulateInterruptedInit(index, vectors)

			ids, dists, err := index.SearchByVector(ctx, []float32{1, 2, 3, 4}, 3, nil)
			require.NoError(t, err,
				"search must fall back to the uncompressed bucket while the quantizer is not initialized")
			require.Len(t, ids, 3)
			require.Len(t, dists, 3)
			assert.Equal(t, uint64(0), ids[0], "nearest neighbor should be the identical vector")
		})

		t.Run(tc.name+" empty index", func(t *testing.T) {
			index := newRQTestIndex(t, tc.bits, false)

			ids, dists, err := index.SearchByVector(ctx, []float32{1, 2, 3, 4}, 3, nil)
			require.NoError(t, err)
			assert.Empty(t, ids)
			assert.Empty(t, dists)
		})
	}
}

func Test_FlatRQContainsDocBeforeQuantizerInit(t *testing.T) {
	index := newRQTestIndex(t, 8, false)
	simulateInterruptedInit(index, [][]float32{{1, 2, 3, 4}})

	assert.True(t, index.ContainsDoc(0),
		"ContainsDoc must consult the uncompressed bucket while the quantizer is not initialized")

	var got []uint64
	index.Iterate(func(docID uint64) bool {
		got = append(got, docID)
		return true
	})
	assert.Equal(t, []uint64{0}, got,
		"Iterate must consult the uncompressed bucket while the quantizer is not initialized")
}

func Test_FlatRQQueryVectorDistancerBeforeQuantizerInit(t *testing.T) {
	cases := []struct {
		name string
		bits int
	}{
		{name: "rq1", bits: 1},
		{name: "rq8", bits: 8},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// cache enabled: the quantized distancer path dereferences the
			// quantizer, which must not happen while it is nil
			index := newRQTestIndex(t, tc.bits, true)
			simulateInterruptedInit(index, [][]float32{{1, 2, 3, 4}})

			distancer := index.QueryVectorDistancer([]float32{1, 2, 3, 4})
			require.NotNil(t, distancer.DistanceFunc)

			dist, err := distancer.DistanceFunc(0)
			require.NoError(t, err)
			assert.InDelta(t, 0.0, dist, 1e-5)
		})
	}
}

// Test_FlatRQConcurrentAddSearch exercises the real init path: the first Add
// initializes dims and the quantizer while searches run concurrently. Searches
// must never fail, and under the race detector the quantizer publication must
// be properly synchronized.
func Test_FlatRQConcurrentAddSearch(t *testing.T) {
	ctx := context.Background()
	index := newRQTestIndex(t, 8, false)

	const (
		numVectors = 200
		dims       = 16
		numReaders = 2
	)

	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	done := make(chan struct{})
	searchErrs := make(chan error, numReaders)

	var wg sync.WaitGroup
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, _, err := index.SearchByVector(ctx, query, 10, nil); err != nil {
					searchErrs <- err
					return
				}
			}
		}()
	}

	for i := 0; i < numVectors; i++ {
		vector := make([]float32, dims)
		for j := range vector {
			vector[j] = float32(i + j)
		}
		require.NoError(t, index.Add(ctx, uint64(i), vector))
	}
	close(done)
	wg.Wait()
	close(searchErrs)

	for err := range searchErrs {
		t.Errorf("concurrent search failed: %v", err)
	}

	ids, _, err := index.SearchByVector(ctx, query, 10, nil)
	require.NoError(t, err)
	assert.Len(t, ids, 10)
}

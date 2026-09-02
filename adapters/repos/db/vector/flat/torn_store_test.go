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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func shutdownStore(t *testing.T, store *lsmkv.Store) {
	require.Nil(t, store.Shutdown(context.Background()))
}

// A shard teardown deregisters every bucket before the query paths are drained,
// so Store.Bucket returns nil to a search that is still running. The search must
// fail with an error instead of dereferencing the nil bucket.
func TestFlatSearchOnTornDownStore(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name     string
		uc       flatent.UserConfig
		teardown func(t *testing.T, store *lsmkv.Store)
	}{
		{
			name:     "uncompressed",
			uc:       flatent.UserConfig{VectorCacheMaxObjects: 1_000},
			teardown: shutdownStore,
		},
		{
			name: "bq without cache",
			uc: flatent.UserConfig{
				VectorCacheMaxObjects: 1_000,
				BQ:                    flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 100},
			},
			teardown: shutdownStore,
		},
		{
			name: "bq with cache",
			uc: flatent.UserConfig{
				VectorCacheMaxObjects: 1_000,
				BQ:                    flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 100, Cache: true},
			},
			teardown: shutdownStore,
		},
		{
			// what DropVectorIndex and the dynamic flat->hnsw upgrade do: the
			// shard lives on, only this vector's raw bucket is deregistered.
			// The quantized phase is served from the cache, so the search
			// reaches the rescoring step and reads the raw vectors.
			name: "raw bucket removed under a cached bq search",
			uc: flatent.UserConfig{
				VectorCacheMaxObjects: 1_000,
				BQ:                    flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 100, Cache: true},
			},
			teardown: func(t *testing.T, store *lsmkv.Store) {
				require.Nil(t, store.ShutdownBucket(context.Background(), helpers.VectorsBucketLSM))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := lsmkv.New(dir, dir, logger, nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.Nil(t, err)

			index, err := New(Config{
				ID:                "torn-store",
				RootPath:          dir,
				Logger:            logger,
				DistanceProvider:  distancer.NewL2SquaredProvider(),
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, test.uc, store)
			require.Nil(t, err)

			// mirrors production: an empty index is prefilled at startup, so the
			// cache stays complete through the inserts below
			index.PostStartup(ctx)

			ids := make([]uint64, 100)
			vectors := make([][]float32, 100)
			for i := range vectors {
				ids[i] = uint64(i)
				vectors[i] = []float32{float32(i), float32(i) + 1, float32(i) + 2, float32(i) + 3}
			}
			require.Nil(t, index.AddBatch(ctx, ids, vectors))

			test.teardown(t, store)

			var searchErr error
			require.NotPanics(t, func() {
				_, _, searchErr = index.SearchByVector(ctx, []float32{1, 2, 3, 4}, 10, nil)
			})
			require.Error(t, searchErr)
			require.False(t, strings.Contains(searchErr.Error(), "panic"),
				"search must fail with an error, got a recovered panic: %v", searchErr)
		})
	}
}

// The write and iteration paths share the search path's exposure: they resolve
// the same buckets by name, so a teardown reaches them the same way.
func TestFlatOperationsOnTornDownStore(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	index, err := New(Config{
		ID:                "torn-store",
		RootPath:          dir,
		Logger:            logger,
		DistanceProvider:  distancer.NewL2SquaredProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, flatent.UserConfig{
		VectorCacheMaxObjects: 1_000,
		BQ:                    flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 100, Cache: true},
	}, store)
	require.Nil(t, err)

	index.PostStartup(ctx)
	require.Nil(t, index.AddBatch(ctx, []uint64{0}, [][]float32{{1, 2, 3, 4}}))
	require.Nil(t, store.Shutdown(ctx))

	failing := map[string]func() error{
		"add": func() error {
			return index.Add(ctx, 1, []float32{4, 3, 2, 1})
		},
		"add batch": func() error {
			return index.AddBatch(ctx, []uint64{2}, [][]float32{{4, 3, 2, 1}})
		},
		"delete": func() error {
			return index.Delete(0)
		},
	}
	for name, op := range failing {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, op(), lsmkv.ErrBucketNotFound)
		})
	}

	// these report through their return value or a log line, so all that is
	// owed here is that they do not crash
	silent := map[string]func(){
		"preload":      func() { index.Preload(3, []float32{1, 1, 1, 1}) },
		"contains doc": func() { require.False(t, index.ContainsDoc(0)) },
		"iterate":      func() { index.Iterate(func(uint64) bool { return true }) },
		"post startup": func() { index.PostStartup(ctx) },
	}
	for name, op := range silent {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, op)
		})
	}
}

// TestFlatIterationHoldsBucketAgainstTeardown covers the other half of the
// race. The cases above start after teardown has finished, so they only prove
// the nil-bucket guards. This one resolves the bucket first and then lets the
// teardown in: the lifetime pin the operation holds must make Bucket.Shutdown
// wait, because a teardown that ran anyway would unmap the segments the scan
// is still walking.
func TestFlatIterationHoldsBucketAgainstTeardown(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	index, err := New(Config{
		ID:                "pinned-store",
		RootPath:          dir,
		Logger:            logger,
		DistanceProvider:  distancer.NewL2SquaredProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, flatent.UserConfig{VectorCacheMaxObjects: 1_000}, store)
	require.Nil(t, err)

	ids := make([]uint64, 100)
	vectors := make([][]float32, 100)
	for i := range vectors {
		ids[i] = uint64(i)
		vectors[i] = []float32{float32(i), float32(i) + 1, float32(i) + 2, float32(i) + 3}
	}
	require.Nil(t, index.AddBatch(ctx, ids, vectors))

	scanning := make(chan struct{})
	resume := make(chan struct{})
	scanned := make(chan struct{})

	go func() {
		defer close(scanned)
		var once sync.Once
		index.Iterate(func(uint64) bool {
			once.Do(func() {
				close(scanning)
				<-resume
			})
			return true
		})
	}()

	select {
	case <-scanning:
	case <-scanned:
		t.Fatal("iteration finished without visiting a vector")
	}

	torn := make(chan error, 1)
	go func() { torn <- store.Shutdown(context.Background()) }()

	select {
	case err := <-torn:
		t.Fatalf("teardown completed while an iteration still held the bucket: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	close(resume)
	<-scanned

	select {
	case err := <-torn:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("teardown never completed after the iteration released its pin")
	}
}

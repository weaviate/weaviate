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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func newLiveCountIndex(t *testing.T, uc flatent.UserConfig) (*flat, *lsmkv.Store) {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	index, err := New(Config{
		ID:                "live-count",
		RootPath:          t.TempDir(),
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeRegularBucketOptions,
	}, uc, store)
	require.NoError(t, err)
	return index, store
}

func defaultLiveCountConfig() flatent.UserConfig {
	config := flatent.UserConfig{}
	config.SetDefaults()
	return config
}

// racing releases every function at the same moment so their critical
// sections overlap as much as the scheduler allows.
func racing(fns ...func() error) error {
	var group errgroup.Group
	start := make(chan struct{})
	for _, fn := range fns {
		group.Go(func() error {
			<-start
			return fn()
		})
	}
	close(start)
	return group.Wait()
}

func repeated(n int, fn func() error) []func() error {
	fns := make([]func() error, n)
	for i := range fns {
		fns[i] = fn
	}
	return fns
}

func storedInPrimaryBucket(t *testing.T, index *flat, store *lsmkv.Store, id uint64) bool {
	t.Helper()

	bucket := store.Bucket(index.getBucketName())
	require.NotNil(t, bucket)
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	exists, err := bucket.Exists(idBytes)
	require.NoError(t, err)
	return exists
}

func TestFlatAlreadyIndexedTracksLiveVectors(t *testing.T) {
	ctx := t.Context()
	index, _ := newLiveCountIndex(t, defaultLiveCountConfig())

	require.NoError(t, index.Add(ctx, 7, []float32{1, 0}))
	require.NoError(t, index.Add(ctx, 7, []float32{2, 0}))
	require.NoError(t, index.Add(ctx, 7, []float32{3, 0}))
	require.Equal(t, uint64(1), index.AlreadyIndexed())

	require.NoError(t, index.Delete(7))
	require.Equal(t, uint64(0), index.AlreadyIndexed())

	require.NoError(t, index.Add(ctx, 7, []float32{4, 0}))
	require.Equal(t, uint64(1), index.AlreadyIndexed())
}

func TestFlatAlreadyIndexedUnderConcurrentMutations(t *testing.T) {
	const (
		ids     = 200
		workers = 8
	)
	ctx := t.Context()
	vector := []float32{1, 0}

	tests := []struct {
		name string
		run  func(t *testing.T, index *flat, store *lsmkv.Store)
	}{
		{
			name: "one id added by several workers is counted once",
			run: func(t *testing.T, index *flat, store *lsmkv.Store) {
				for id := uint64(0); id < ids; id++ {
					require.NoError(t, racing(repeated(workers, func() error {
						return index.Add(ctx, id, vector)
					})...))
				}
				require.Equal(t, uint64(ids), index.AlreadyIndexed())
			},
		},
		{
			name: "one id deleted by several workers is uncounted once",
			run: func(t *testing.T, index *flat, store *lsmkv.Store) {
				for id := uint64(0); id < ids; id++ {
					require.NoError(t, index.Add(ctx, id, vector))
				}
				for id := uint64(0); id < ids/2; id++ {
					require.NoError(t, racing(repeated(workers, func() error {
						return index.Delete(id)
					})...))
				}
				require.Equal(t, uint64(ids/2), index.AlreadyIndexed())
			},
		},
		{
			name: "an add racing a delete leaves the count equal to the stored ids",
			run: func(t *testing.T, index *flat, store *lsmkv.Store) {
				for id := uint64(0); id < ids; id++ {
					require.NoError(t, index.Add(ctx, id, vector))
				}
				var stored uint64
				for id := uint64(0); id < ids; id++ {
					require.NoError(t, racing(
						func() error { return index.Add(ctx, id, vector) },
						func() error { return index.Delete(id) },
					))
					if storedInPrimaryBucket(t, index, store, id) {
						stored++
					}
				}
				require.Equal(t, stored, index.AlreadyIndexed())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, store := newLiveCountIndex(t, defaultLiveCountConfig())
			test.run(t, index, store)
		})
	}
}

// The count follows the primary vectors bucket: a failure in the compressed
// bucket after the primary write must not leave the two out of step, since the
// retry of the same call would then see the id as already handled.
func TestFlatAlreadyIndexedWhenCompressedBucketFails(t *testing.T) {
	ctx := t.Context()
	vector := []float32{1, 0}

	tests := []struct {
		name    string
		prepare func(t *testing.T, index *flat)
		act     func(index *flat) error
	}{
		{
			name: "add",
			act:  func(index *flat) error { return index.Add(ctx, 1, vector) },
		},
		{
			name: "delete",
			prepare: func(t *testing.T, index *flat) {
				require.NoError(t, index.Add(ctx, 1, vector))
			},
			act: func(index *flat) error { return index.Delete(1) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, store := newLiveCountIndex(t, flatent.UserConfig{
				VectorCacheMaxObjects: 1_000,
				BQ:                    flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 100},
			})
			if test.prepare != nil {
				test.prepare(t, index)
			}
			require.NoError(t, store.ShutdownBucket(ctx, index.getCompressedBucketName()))

			require.Error(t, test.act(index))

			want := uint64(0)
			if storedInPrimaryBucket(t, index, store, 1) {
				want = 1
			}
			require.Equal(t, want, index.AlreadyIndexed())
		})
	}
}

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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func newDeleteCountTestIndex(t *testing.T) *flat {
	t.Helper()

	logger, _ := test.NewNullLogger()
	root := t.TempDir()
	store, err := lsmkv.New(root, root, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	cfg := flatent.UserConfig{}
	cfg.SetDefaults()

	idx, err := New(Config{
		ID:                "delete-count",
		RootPath:          root,
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeRegularBucketOptions,
	}, cfg, store)
	require.NoError(t, err)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })

	return idx
}

// Regression for #12958: Delete must keep AlreadyIndexed() aligned with the
// live vector set (ContainsDoc), not leave a stale post-delete count.
func TestFlatDeleteUpdatesAlreadyIndexedCount(t *testing.T) {
	idx := newDeleteCountTestIndex(t)
	ctx := context.Background()

	require.NoError(t, idx.Add(ctx, 7, []float32{1, 0}))
	require.NoError(t, idx.Add(ctx, 8, []float32{0, 1}))

	before := idx.AlreadyIndexed()
	require.Equal(t, uint64(2), before)

	require.NoError(t, idx.Delete(7))

	after := idx.AlreadyIndexed()
	require.Equal(t, uint64(1), after)
	require.False(t, idx.ContainsDoc(7))
	require.True(t, idx.ContainsDoc(8))

	// Deleting a missing id must not underflow the live count.
	require.NoError(t, idx.Delete(7))
	require.Equal(t, uint64(1), idx.AlreadyIndexed())
}

// Overwriting an existing id keeps a single live vector, so the count must stay
// flat instead of growing on every Add.
func TestFlatAddOverwriteKeepsAlreadyIndexedCount(t *testing.T) {
	idx := newDeleteCountTestIndex(t)
	ctx := context.Background()

	require.NoError(t, idx.Add(ctx, 7, []float32{1, 0}))
	require.NoError(t, idx.Add(ctx, 7, []float32{0, 1}))
	require.NoError(t, idx.Add(ctx, 7, []float32{1, 1}))

	require.Equal(t, uint64(1), idx.AlreadyIndexed())

	require.NoError(t, idx.Delete(7))
	require.Equal(t, uint64(0), idx.AlreadyIndexed())
	require.False(t, idx.ContainsDoc(7))
}

// Concurrent deletes of the same id must decrement exactly once, and concurrent
// adds of the same id must increment exactly once.
func TestFlatDeleteAndAddAreConsistentUnderConcurrency(t *testing.T) {
	idx := newDeleteCountTestIndex(t)
	ctx := context.Background()

	const id = uint64(42)
	require.NoError(t, idx.Add(ctx, id, []float32{1, 0}))

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, idx.Delete(id))
		}()
	}
	wg.Wait()

	require.False(t, idx.ContainsDoc(id))
	require.Equal(t, uint64(0), idx.AlreadyIndexed(), "duplicate deletes must not decrement twice")

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, idx.Add(ctx, id, []float32{0, 1}))
		}()
	}
	wg.Wait()

	require.True(t, idx.ContainsDoc(id))
	require.Equal(t, uint64(1), idx.AlreadyIndexed(), "duplicate adds must not increment twice")
}

// Add and Delete racing on the same id must leave the count equal to the number
// of live vectors, in whichever order they land.
func TestFlatAddDeleteInterleavingKeepsCountConsistent(t *testing.T) {
	idx := newDeleteCountTestIndex(t)
	ctx := context.Background()

	const pairs = 64
	var wg sync.WaitGroup
	for i := range pairs {
		id := uint64(100 + i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			require.NoError(t, idx.Add(ctx, id, []float32{1, float32(i)}))
		}()
		go func() {
			defer wg.Done()
			require.NoError(t, idx.Delete(id))
		}()
	}
	wg.Wait()

	var live uint64
	for i := range pairs {
		if idx.ContainsDoc(uint64(100 + i)) {
			live++
		}
	}
	require.Equal(t, live, idx.AlreadyIndexed())
}

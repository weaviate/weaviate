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

package spfresh

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func newTestBlockController(t testing.TB, dims int) *BlockController {
	t.Helper()

	// Create a mock store and encoder for testing
	store := testinghelpers.NewDummyStore(t)

	err := store.CreateBucket(t.Context(), bucketName)
	require.NoError(t, err, "failed to create bucket")

	return NewBlockController(
		NewLSMStore(store),
		new(common.Pool[uint64]),
		&BlockControllerConfig{
			VectorDimensions: dims,
		},
	)
}

func countFreeBlocks(t *testing.T, ctrl *BlockController) int {
	t.Helper()

	var stack common.Stack[uint64]

	var freeBlocks int
	for {
		v, ok := ctrl.blockPool.freeBlockPool.Get()
		if !ok {
			break
		}
		stack.Push(v)
		freeBlocks++
	}

	for {
		v, ok := stack.Pop()
		if !ok {
			break
		}
		ctrl.blockPool.freeBlockPool.Put(v)
	}

	return freeBlocks
}

func TestBlockController(t *testing.T) {
	ctx := t.Context()

	t.Run("Get non-existing posting", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)
		p, err := ctrl.Get(ctx, 0)
		require.ErrorIs(t, err, ErrPostingNotFound)
		require.Nil(t, p)
	})

	t.Run("Put and Get posting", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		posting := Posting{
			{ID: 1, Version: 1, Data: bytes.Repeat([]byte{1}, 32)},
			{ID: 2, Version: 2, Data: bytes.Repeat([]byte{2}, 32)},
		}

		err := ctrl.Put(ctx, 1, posting)
		require.NoError(t, err)

		retrievedPosting, err := ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting, retrievedPosting)
	})

	t.Run("Put and Get single posting with many vectors", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		posting := make(Posting, 0)
		for i := 0; i < 100; i++ {
			data := bytes.Repeat([]byte{byte(i % 256)}, 32)
			posting = append(posting, Vector{
				ID:      uint64(i + 1),
				Version: 1,
				Data:    data,
			})
		}

		err := ctrl.Put(ctx, 1, posting)
		require.NoError(t, err)

		retrievedPosting, err := ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting, retrievedPosting)
	})

	t.Run("Replace and Get posting", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		posting1 := Posting{
			{ID: 1, Version: 1, Data: bytes.Repeat([]byte{1}, 32)},
			{ID: 2, Version: 2, Data: bytes.Repeat([]byte{2}, 32)},
		}

		err := ctrl.Put(ctx, 1, posting1)
		require.NoError(t, err)

		posting2 := Posting{
			{ID: 3, Version: 3, Data: bytes.Repeat([]byte{3}, 32)},
		}

		err = ctrl.Put(ctx, 1, posting2)
		require.NoError(t, err)

		retrievedPosting, err := ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting2, retrievedPosting)

		// verify the number of free blocks
		freeBlocks := countFreeBlocks(t, ctrl)
		require.Equal(t, 1, freeBlocks)
	})

	t.Run("Replace and Get posting with many vectors", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		posting1 := make(Posting, 0, 100)
		for i := 0; i < 100; i++ {
			data := bytes.Repeat([]byte{byte(i % 256)}, 32)
			posting1 = append(posting1, Vector{
				ID:      uint64(i + 3),
				Version: 1,
				Data:    data,
			})
		}

		// 100 * (8 + 1 + 32) = 4100 bytes used
		// 4100 / 4096 = 2 blocks used
		err := ctrl.Put(ctx, 1, posting1)
		require.NoError(t, err)

		posting2 := Posting{
			{ID: 1, Version: 1, Data: bytes.Repeat([]byte{1}, 32)},
			{ID: 2, Version: 2, Data: bytes.Repeat([]byte{2}, 32)},
		}

		// 2 * (8 + 1 + 32) = 70 bytes used
		// 70 / 4096 = 1 block used
		err = ctrl.Put(ctx, 1, posting2)
		require.NoError(t, err)

		retrievedPosting, err := ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting2, retrievedPosting)

		// Replacing the posting added one block, and the previous 2 blocks were freed
		// and added to the free pool.
		freeBlocks := countFreeBlocks(t, ctrl)
		require.Equal(t, 2, freeBlocks)

		// Let's replace the posting again
		posting3 := Posting{
			{ID: 4, Version: 4, Data: bytes.Repeat([]byte{4}, 32)},
			{ID: 5, Version: 5, Data: bytes.Repeat([]byte{5}, 32)},
		}

		// 2 * (8 + 1 + 32) = 70 bytes used
		// 70 / 4096 = 1 block used
		err = ctrl.Put(ctx, 1, posting3)
		require.NoError(t, err)

		retrievedPosting, err = ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, posting3, retrievedPosting)

		// The new posting needed one block, so it took one from the free pool.
		// The previous block was freed, so we have 2 blocks in the free pool.
		freeBlocks = countFreeBlocks(t, ctrl)
		require.Equal(t, 2, freeBlocks)
	})

	t.Run("Append to unexisting posting", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		vector := Vector{
			ID:      1,
			Version: 1,
			Data:    bytes.Repeat([]byte{1}, 32),
		}

		err := ctrl.Append(ctx, 1, &vector)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("Append to existing posting", func(t *testing.T) {
		ctrl := newTestBlockController(t, 32)

		posting := Posting{
			{ID: 1, Version: 1, Data: bytes.Repeat([]byte{1}, 32)},
		}

		err := ctrl.Put(ctx, 1, posting)
		require.NoError(t, err)

		vector := Vector{
			ID:      2,
			Version: 2,
			Data:    bytes.Repeat([]byte{2}, 32),
		}

		err = ctrl.Append(ctx, 1, &vector)
		require.NoError(t, err)

		retrievedPosting, err := ctrl.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, Posting{posting[0], vector}, retrievedPosting)
	})
}

func BenchmarkBlockControllerAppend(b *testing.B) {
	ctx := b.Context()

	ctrl := newTestBlockController(b, 32)

	vector := bytes.Repeat([]byte{1}, 32)
	posting := Posting{
		{ID: 1, Version: 1, Data: vector},
	}

	err := ctrl.Put(ctx, 1, posting)
	if err != nil {
		b.Fatalf("failed to put initial posting: %v", err)
	}

	v := Vector{
		ID:      1,
		Version: 1,
		Data:    vector,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = ctrl.Append(ctx, 1, &v)
		if err != nil {
			b.Fatalf("failed to append vector %d: %v", i+2, err)
		}
	}
}

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

package db

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func TestIndexQueue(t *testing.T) {
	ctx := context.Background()
	os.Setenv("WEAVIATE_ASYNC_INDEXING", "true")
	defer os.Unsetenv("WEAVIATE_ASYNC_INDEXING")

	t.Run("pushes to indexer if batch is full", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{}, 1)
		var ids [][]uint64
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			ids = append(ids, id)
			called <- struct{}{}
			return nil
		}

		q, err := NewIndexQueue(&idx, IndexQueueOptions{
			BatchSize: 2,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, vectorDescriptor{
			id:     1,
			vector: []float32{1, 2, 3},
		})
		require.NoError(t, err)
		require.Equal(t, 0, idx.addBatchCalled)

		err = q.Push(ctx, vectorDescriptor{
			id:     2,
			vector: []float32{4, 5, 6},
		})
		require.NoError(t, err)
		<-called
		require.Equal(t, 1, idx.addBatchCalled)

		require.Equal(t, [][]uint64{{1, 2}}, ids)
	})

	t.Run("retry on indexing error", func(t *testing.T) {
		var idx mockBatchIndexer
		i := 0
		var ids [][]uint64
		called := make(chan struct{}, 1)
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			ids = append(ids, id)
			i++
			if i < 3 {
				return fmt.Errorf("indexing error: %d", i)
			}

			called <- struct{}{}

			return nil
		}

		q, err := NewIndexQueue(&idx, IndexQueueOptions{
			BatchSize:     1,
			RetryInterval: time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, vectorDescriptor{
			id:     1,
			vector: []float32{1, 2, 3},
		})
		require.NoError(t, err)
		<-called
		require.Equal(t, 3, idx.addBatchCalled)
		require.Equal(t, [][]uint64{{1}, {1}, {1}}, ids)
	})

	t.Run("merges results from queries", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{}, 1)
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			called <- struct{}{}
			return nil
		}

		q, err := NewIndexQueue(&idx, IndexQueueOptions{
			BatchSize: 3,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, vectorDescriptor{
			id:     1,
			vector: []float32{1, 2, 3},
		})
		require.NoError(t, err)
		err = q.Push(ctx, vectorDescriptor{
			id:     2,
			vector: []float32{4, 5, 6},
		})
		require.NoError(t, err)
		err = q.Push(ctx, vectorDescriptor{
			id:     3,
			vector: []float32{7, 8, 9},
		})
		require.NoError(t, err)
		err = q.Push(ctx, vectorDescriptor{
			id:     4,
			vector: []float32{1, 2, 3},
		})
		require.NoError(t, err)

		<-called
		res, _, err := q.SearchByVector([]float32{1, 2, 3}, 2, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{1, 4}, res)
	})

	t.Run("queue size", func(t *testing.T) {
		var idx mockBatchIndexer
		closeCh := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			<-closeCh
			return nil
		}

		q, err := NewIndexQueue(&idx, IndexQueueOptions{
			BatchSize: 5,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 101; i++ {
			err = q.Push(ctx, vectorDescriptor{
				id:     i + 1,
				vector: []float32{1, 2, 3},
			})
			require.NoError(t, err)
		}

		time.Sleep(10 * time.Millisecond)
		require.EqualValues(t, 101, q.Size())
		close(closeCh)
	})

	t.Run("deletion", func(t *testing.T) {
		var idx mockBatchIndexer
		var count int
		indexingDone := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			count++
			if count == 5 {
				close(indexingDone)
			}

			return nil
		}

		q, err := NewIndexQueue(&idx, IndexQueueOptions{
			BatchSize:     4,
			IndexInterval: 100 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 20; i++ {
			err = q.Push(ctx, vectorDescriptor{
				id:     i,
				vector: []float32{1, 2, 3},
			})
			require.NoError(t, err)
		}

		err = q.Delete(5, 10, 15)
		require.NoError(t, err)

		<-indexingDone

		// check what has been indexed
		var ids []int
		for id := range idx.vectors {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		require.Equal(t, []int{0, 1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19}, ids)

		// the "deleted" mask should be empty
		require.Empty(t, q.queue.deleted.m)

		// now delete something that's already indexed
		err = q.Delete(0, 4, 8)
		require.NoError(t, err)

		// the "deleted" mask should still be empty
		require.Empty(t, q.queue.deleted.m)

		// check what's in the index
		ids = ids[:0]
		for id := range idx.vectors {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		require.Equal(t, []int{1, 2, 3, 6, 7, 9, 11, 12, 13, 14, 16, 17, 18, 19}, ids)

		// delete something that's not indexed yet
		err = q.Delete(20, 21, 22)
		require.NoError(t, err)

		// the "deleted" mask should contain the deleted ids
		ids = ids[:0]
		for id := range q.queue.deleted.m {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		require.Equal(t, []int{20, 21, 22}, ids)
	})
}

func BenchmarkPush(b *testing.B) {
	var idx mockBatchIndexer

	idx.addBatchFn = func(id []uint64, vector [][]float32) error {
		time.Sleep(1 * time.Second)
		return nil
	}

	q, err := NewIndexQueue(&idx, IndexQueueOptions{
		BatchSize:     1000,
		IndexInterval: 1 * time.Millisecond,
	})
	require.NoError(b, err)
	defer q.Close()

	vecs := make([]vectorDescriptor, 100)
	for j := range vecs {
		vecs[j] = vectorDescriptor{
			id:     uint64(j),
			vector: []float32{1, 2, 3},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			err = q.Push(context.Background(), vecs...)
			require.NoError(b, err)
		}
	}
}

type mockBatchIndexer struct {
	sync.Mutex
	addBatchFn     func(id []uint64, vector [][]float32) error
	addBatchCalled int
	vectors        map[uint64][][]float32
	containsNodeFn func(id uint64) bool
	deleteFn       func(ids ...uint64) error
}

func (m *mockBatchIndexer) AddBatch(id []uint64, vector [][]float32) (err error) {
	m.Lock()
	defer m.Unlock()

	m.addBatchCalled++
	if m.addBatchFn != nil {
		err = m.addBatchFn(id, vector)
	}

	if m.vectors == nil {
		m.vectors = make(map[uint64][][]float32)
	}

	for i, id := range id {
		m.vectors[id] = append(m.vectors[id], vector[i])
	}

	return
}

func (m *mockBatchIndexer) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	m.Lock()
	defer m.Unlock()
	results := newPqMaxPool(k).GetMax(k)
	for id, vectors := range m.vectors {
		// skip filtered data
		if allowList != nil && allowList.Contains(id) {
			continue
		}
		for _, v := range vectors {
			dist, _, err := m.DistanceBetweenVectors(vector, v)
			if err != nil {
				return nil, nil, err
			}

			if results.Len() < k || dist < results.Top().Dist {
				results.Insert(id, dist)
				for results.Len() > k {
					results.Pop()
				}
			}
		}
	}
	ids := make([]uint64, k)
	distances := make([]float32, k)

	for i := k - 1; i >= 0; i-- {
		element := results.Pop()
		ids[i] = element.ID
		distances[i] = element.Dist
	}
	return ids, distances, nil
}

func (m *mockBatchIndexer) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	res := float32(0)
	for i := range x {
		diff := x[i] - y[i]
		res += diff * diff
	}
	return res, true, nil
}

func (m *mockBatchIndexer) ContainsNode(id uint64) bool {
	m.Lock()
	defer m.Unlock()
	if m.containsNodeFn != nil {
		return m.containsNodeFn(id)
	}

	_, ok := m.vectors[id]
	return ok
}

func (m *mockBatchIndexer) Delete(ids ...uint64) error {
	m.Lock()
	defer m.Unlock()
	if m.deleteFn != nil {
		return m.deleteFn(ids...)
	}

	for _, id := range ids {
		delete(m.vectors, id)
	}

	return nil
}

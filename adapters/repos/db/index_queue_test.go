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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func startWorker(t testing.TB, retryInterval ...time.Duration) chan job {
	t.Helper()
	ch := make(chan job)
	t.Cleanup(func() {
		close(ch)
	})

	itv := time.Millisecond
	if len(retryInterval) > 0 {
		itv = retryInterval[0]
	}

	go func() {
		logger := logrus.New()
		logger.Level = logrus.ErrorLevel
		asyncWorker(ch, logger, itv)
	}()

	return ch
}

func pushVector(t testing.TB, ctx context.Context, q *IndexQueue, id uint64, vector ...float32) {
	err := q.Push(ctx, vectorDescriptor{
		id:     id,
		vector: vector,
	})
	require.NoError(t, err)
}

func TestIndexQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	os.Setenv("WEAVIATE_ASYNC_INDEXING", "true")
	defer os.Unsetenv("WEAVIATE_ASYNC_INDEXING")

	t.Run("pushes to indexer if batch is full", func(t *testing.T) {
		var idx mockBatchIndexer
		idsCh := make(chan []uint64, 1)
		idx.addBatchFn = func(ids []uint64, vector [][]float32) error {
			idsCh <- ids
			return nil
		}

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize: 2,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, 1, 2, 3)
		select {
		case <-idsCh:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}

		pushVector(t, ctx, q, 2, 4, 5, 6)
		ids := <-idsCh

		require.Equal(t, []uint64{1, 2}, ids)
	})

	t.Run("doesn't index if batch is not null", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			called <- struct{}{}
			return nil
		}
		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize:     100,
			IndexInterval: time.Microsecond,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, 1, 2, 3)
		select {
		case <-called:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}

		pushVector(t, ctx, q, 2, 4, 5, 6)

		select {
		case <-called:
			t.Fatal("should not have been called")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("retry on indexing error", func(t *testing.T) {
		var idx mockBatchIndexer
		i := int32(0)
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			if atomic.AddInt32(&i, 1) < 3 {
				return fmt.Errorf("indexing error: %d", i)
			}

			close(called)

			return nil
		}

		q, err := NewIndexQueue(&idx, startWorker(t, time.Millisecond), IndexQueueOptions{
			BatchSize: 1,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, 1, 2, 3)
		<-called
	})

	t.Run("merges results from queries", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(called)
			return nil
		}

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize: 3,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, 1, 2, 3)
		pushVector(t, ctx, q, 2, 4, 5, 6)
		pushVector(t, ctx, q, 3, 7, 8, 9)
		pushVector(t, ctx, q, 4, 1, 2, 3)

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

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize: 5,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 101; i++ {
			pushVector(t, ctx, q, i+1, 1, 2, 3)
		}

		time.Sleep(10 * time.Millisecond)
		require.EqualValues(t, 101, q.Size())
		close(closeCh)
	})

	t.Run("deletion", func(t *testing.T) {
		var idx mockBatchIndexer
		var count int32
		indexingDone := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			if atomic.AddInt32(&count, 1) == 5 {
				close(indexingDone)
			}

			return nil
		}

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize:     4,
			IndexInterval: 100 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 20; i++ {
			pushVector(t, ctx, q, i, 1, 2, 3)
		}

		err = q.Delete(5, 10, 15)
		require.NoError(t, err)

		<-indexingDone

		// check what has been indexed
		require.Equal(t, []uint64{0, 1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19}, idx.IDs())

		// the "deleted" mask should be empty
		q.queue.deleted.Lock()
		require.Empty(t, q.queue.deleted.m)
		q.queue.deleted.Unlock()

		// now delete something that's already indexed
		err = q.Delete(0, 4, 8)
		require.NoError(t, err)

		// the "deleted" mask should still be empty
		q.queue.deleted.Lock()
		require.Empty(t, q.queue.deleted.m)
		q.queue.deleted.Unlock()

		// check what's in the index
		require.Equal(t, []uint64{1, 2, 3, 6, 7, 9, 11, 12, 13, 14, 16, 17, 18, 19}, idx.IDs())

		// delete something that's not indexed yet
		err = q.Delete(20, 21, 22)
		require.NoError(t, err)

		// the "deleted" mask should contain the deleted ids
		q.queue.deleted.Lock()
		var ids []int
		for id := range q.queue.deleted.m {
			ids = append(ids, int(id))
		}
		q.queue.deleted.Unlock()
		sort.Ints(ids)
		require.Equal(t, []int{20, 21, 22}, ids)
	})

	t.Run("brute force upper limit", func(t *testing.T) {
		var idx mockBatchIndexer

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize:             1000,
			BruteForceSearchLimit: 2,
		})
		require.NoError(t, err)
		defer q.Close()

		pushVector(t, ctx, q, 1, 1, 2, 3)
		pushVector(t, ctx, q, 2, 4, 5, 6)
		pushVector(t, ctx, q, 3, 7, 8, 9)
		pushVector(t, ctx, q, 4, 1, 2, 3)

		res, _, err := q.SearchByVector([]float32{7, 8, 9}, 2, nil)
		require.NoError(t, err)
		// despite having 4 vectors in the queue
		// only the first two are used for brute force search
		require.Equal(t, []uint64{2, 1}, res)
	})

	t.Run("stale vectors", func(t *testing.T) {
		var idx mockBatchIndexer
		closeCh := make(chan struct{})
		idx.addBatchFn = func(id []uint64, vector [][]float32) error {
			close(closeCh)
			return nil
		}

		q, err := NewIndexQueue(&idx, startWorker(t), IndexQueueOptions{
			BatchSize:     5,
			StaleTimeout:  100 * time.Millisecond,
			IndexInterval: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		for i := uint64(0); i < 3; i++ {
			pushVector(t, ctx, q, i+1, 1, 2, 3)
		}

		select {
		case <-closeCh:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("should have been indexed after 100ms")
		}

		require.EqualValues(t, []uint64{1, 2, 3}, idx.IDs())
	})
}

func BenchmarkPush(b *testing.B) {
	var idx mockBatchIndexer

	idx.addBatchFn = func(id []uint64, vector [][]float32) error {
		time.Sleep(1 * time.Second)
		return nil
	}

	q, err := NewIndexQueue(&idx, startWorker(b), IndexQueueOptions{
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
	vectors        map[uint64][]float32
	containsNodeFn func(id uint64) bool
	deleteFn       func(ids ...uint64) error
}

func (m *mockBatchIndexer) AddBatch(ids []uint64, vector [][]float32) (err error) {
	m.Lock()
	defer m.Unlock()

	if m.addBatchFn != nil {
		err = m.addBatchFn(ids, vector)
	}

	if m.vectors == nil {
		m.vectors = make(map[uint64][]float32)
	}

	for i, id := range ids {
		m.vectors[id] = vector[i]
	}

	return
}

func (m *mockBatchIndexer) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	m.Lock()
	defer m.Unlock()

	results := newPqMaxPool(k).GetMax(k)

	for id, v := range m.vectors {
		// skip filtered data
		if allowList != nil && allowList.Contains(id) {
			continue
		}

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
	ids := make([]uint64, 0, k)
	distances := make([]float32, 0, k)

	for i := k - 1; i >= 0; i-- {
		if results.Len() == 0 {
			break
		}
		element := results.Pop()
		ids = append(ids, element.ID)
		distances = append(distances, element.Dist)
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

func (m *mockBatchIndexer) IDs() []uint64 {
	m.Lock()
	defer m.Unlock()

	ids := make([]uint64, 0, len(m.vectors))
	for id := range m.vectors {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids
}

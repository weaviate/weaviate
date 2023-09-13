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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

func TestIndexQueue(t *testing.T) {
	ctx := context.Background()

	t.Run("pushes to indexer if batch is full", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{}, 1)
		idx.fn = func(id []uint64, vector [][]float32) error {
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
		require.Equal(t, 0, idx.called)

		err = q.Push(ctx, vectorDescriptor{
			id:     2,
			vector: []float32{4, 5, 6},
		})
		require.NoError(t, err)
		<-called
		require.Equal(t, 1, idx.called)

		require.Equal(t, [][]uint64{{1, 2}}, idx.ids)
	})

	t.Run("retry on indexing error", func(t *testing.T) {
		var idx mockBatchIndexer
		i := 0
		called := make(chan struct{}, 1)
		idx.fn = func(id []uint64, vector [][]float32) error {
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
		require.Equal(t, 3, idx.called)
		require.Equal(t, [][]uint64{{1}, {1}, {1}}, idx.ids)
	})

	t.Run("merges results from queries", func(t *testing.T) {
		var idx mockBatchIndexer
		called := make(chan struct{}, 1)
		idx.fn = func(id []uint64, vector [][]float32) error {
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
		ids, _, err := q.SearchByVector([]float32{1, 2, 3}, 2, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{1, 4}, ids)
	})
}

func BenchmarkPush(b *testing.B) {
	var idx mockBatchIndexer

	idx.fn = func(id []uint64, vector [][]float32) error {
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
	fn      func(id []uint64, vector [][]float32) error
	called  int
	ids     [][]uint64
	vectors [][][]float32
}

func (m *mockBatchIndexer) AddBatch(id []uint64, vector [][]float32) (err error) {
	m.called++
	if m.fn != nil {
		err = m.fn(id, vector)
	}

	m.ids = append(m.ids, id)
	m.vectors = append(m.vectors, vector)
	return
}

func (m *mockBatchIndexer) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	results := newPqMaxPool(k).GetMax(k)
	for i, v := range m.vectors {
		for j := range v {
			// skip filtered data
			if allowList != nil && allowList.Contains(m.ids[i][j]) {
				continue
			}

			dist, _, err := m.DistanceBetweenVectors(vector, m.vectors[i][j])
			if err != nil {
				return nil, nil, err
			}

			if results.Len() < k || dist < results.Top().Dist {
				results.Insert(m.ids[i][j], dist)
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

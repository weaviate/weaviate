package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexQueue(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "wal.bin")

	ctx := context.Background()

	t.Run("pushes to indexer if queue is full", func(t *testing.T) {
		var idx mockBatchIndexer
		q, err := NewIndexQueue(walPath, &idx, IndexQueueOptions{
			MaxQueueSize: 3,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, 1, []float32{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 0, idx.called)
		require.Equal(t, 1, len(q.toIndex))

		err = q.Push(ctx, 2, []float32{4, 5, 6})
		require.NoError(t, err)
		require.Equal(t, 0, idx.called)
		require.Equal(t, 2, len(q.toIndex))

		err = q.Push(ctx, 3, []float32{7, 8, 9})
		require.NoError(t, err)
		require.Equal(t, 1, idx.called)
		require.Equal(t, 0, len(q.toIndex))

		require.Equal(t, [][]uint64{{1, 2, 3}}, idx.ids)
	})

	t.Run("retry on indexing error", func(t *testing.T) {
		var idx mockBatchIndexer
		i := 1
		idx.fn = func(id []uint64, vector [][]float32) error {
			i++
			if i < 3 {
				return fmt.Errorf("indexing error: %d", i)
			}

			return nil
		}

		q, err := NewIndexQueue(walPath, &idx, IndexQueueOptions{
			MaxQueueSize:  1,
			MaxStaleTime:  10 * time.Hour,
			RetryInterval: time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, 1, []float32{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 1, idx.called)
		require.Equal(t, [][]uint64{{1}}, idx.ids)
	})

	t.Run("index if stale", func(t *testing.T) {
		var idx mockBatchIndexer
		q, err := NewIndexQueue(walPath, &idx, IndexQueueOptions{
			MaxQueueSize: 100,
			MaxStaleTime: 500 * time.Millisecond,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, 1, []float32{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 0, idx.called)

		time.Sleep(600 * time.Millisecond)
		require.Equal(t, 1, idx.called)
	})

	t.Run("data is persisted", func(t *testing.T) {
		var idx mockBatchIndexer
		q, err := NewIndexQueue(walPath, &idx, IndexQueueOptions{
			MaxQueueSize: 10,
		})
		require.NoError(t, err)
		defer q.Close()

		err = q.Push(ctx, 1, []float32{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 0, idx.called)

		err = q.walFile.Close()
		require.NoError(t, err)

		content, err := os.ReadFile(walPath)
		require.NoError(t, err)

		require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 1}, content[:8])
	})
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
	return nil
}

package queue

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestNewQueue(t *testing.T) {
	tempDir := t.TempDir()

	s := NewScheduler(SchedulerOptions{
		Workers: []chan Batch{make(chan Batch, 1)},
	})

	q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
		return nil
	}))

	require.NoError(t, err)
	require.NotNil(t, q)
	require.Equal(t, "test_queue", q.ID())
}

func TestQueuePush(t *testing.T) {
	s := NewScheduler(SchedulerOptions{
		Workers: []chan Batch{make(chan Batch, 1)},
	})

	t.Run("a few tasks", func(t *testing.T) {
		tempDir := t.TempDir()

		q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
			return nil
		}))
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		require.Equal(t, int64(3), q.Size())
	})

	t.Run("push when closed", func(t *testing.T) {
		tempDir := t.TempDir()

		q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
			return nil
		}))
		require.NoError(t, err)

		err = q.Close()
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.Error(t, err)
	})

	t.Run("keeps track of last push time", func(t *testing.T) {
		tempDir := t.TempDir()

		q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
			return nil
		}))
		require.NoError(t, err)

		lpt := q.lastPushTime.Load()
		require.Nil(t, lpt)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		lpt = q.lastPushTime.Load()
		require.NotNil(t, lpt)

		err = q.Push(1, 400, 500, 600)
		require.NoError(t, err)

		lpt2 := q.lastPushTime.Load()
		require.NotEqual(t, lpt, lpt2)
	})

	t.Run("persistence", func(t *testing.T) {
		tempDir := t.TempDir()

		q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
			return nil
		}))
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		entries, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		stat, err := os.Stat(filepath.Join(tempDir, entries[0].Name()))
		require.NoError(t, err)
		require.EqualValues(t, 9*3, stat.Size())
	})
}

func TestQueueDecodeTask(t *testing.T) {
	s := NewScheduler(SchedulerOptions{
		Workers: []chan Batch{make(chan Batch, 1)},
	})

	t.Run("a few tasks", func(t *testing.T) {
		tempDir := t.TempDir()

		q, err := New(s, newTestLogger(t), "test_queue", tempDir, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
			return nil
		}))
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		entries, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		err = q.Close()
		require.NoError(t, err)

		f, err := os.Open(filepath.Join(tempDir, entries[0].Name()))
		require.NoError(t, err)

		buf := bufio.NewReader(f)

		for i := 0; i < 3; i++ {
			task, err := q.DecodeTask(buf)
			require.NoError(t, err)
			require.NotNil(t, task)
			require.Equal(t, uint8(1), task.Op)
			require.Len(t, task.IDs, 1)
			require.Equal(t, uint64(100*(i+1)), task.IDs[0])
		}

		require.Equal(t, int64(3), q.Size())

		// decoding more tasks should return an error
		_, err = q.DecodeTask(buf)
		require.ErrorIs(t, io.EOF, err)
	})
}

func newTestLogger(t *testing.T) logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

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

func TestEncoder(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.Out = io.Discard

	dir := t.TempDir()
	// test the encoder
	enc, err := NewEncoderWith(dir, logger, 9*50)
	require.NoError(t, err)

	// encode 120 records
	for i := 0; i < 120; i++ {
		err := enc.Encode(uint8(i), uint64(i+1))
		require.NoError(t, err)
	}

	// check the number of files
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	// check if the entry name matches the regex pattern
	require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
	require.Equal(t, "chunk.bin.partial", entries[2].Name())

	// check the content of the files
	checkContent := func(fName string, length int, start, end int) {
		f, err := os.Open(filepath.Join(dir, fName))
		require.NoError(t, err)
		defer f.Close()

		content, err := io.ReadAll(f)
		require.NoError(t, err)

		require.Len(t, content, length)

		if length == 0 {
			return
		}

		f.Seek(0, io.SeekStart)
		buf := bufio.NewReader(f)

		for i := start; i < end; i++ {
			op, key, err := Decode(buf)
			require.NoError(t, err)

			require.Equal(t, uint8(i), op)
			require.Equal(t, uint64(i+1), key)
		}
	}

	checkContent(entries[0].Name(), 9*50, 0, 49)
	checkContent(entries[1].Name(), 9*50, 50, 99)

	// partial file should have 0 records because it was not flushed
	checkContent(entries[2].Name(), 0, 100, 119)

	// flush the encoder
	err = enc.Flush()
	require.NoError(t, err)

	// check the content of the partial file
	checkContent(entries[2].Name(), 9*20, 100, 119)

	// check the queue size
	size := enc.RecordCount()
	require.EqualValues(t, 120, size)

	// check the queue size with a different encoder
	enc2, err := NewEncoderWith(dir, logger, 9*50)
	require.NoError(t, err)
	size = enc2.RecordCount()
	require.EqualValues(t, 120, size)

	// promote the partial file
	err = enc.promoteChunk()
	require.NoError(t, err)

	// check the number of files
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)

	require.Len(t, entries, 3)
	require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[2].Name())

	// check the content of the 3rd file
	checkContent(entries[2].Name(), 9*20, 100, 119)

	// check the queue size again
	size = enc.RecordCount()
	require.EqualValues(t, 120, size)

	// promote again, no-op
	err = enc.promoteChunk()
	require.NoError(t, err)
}

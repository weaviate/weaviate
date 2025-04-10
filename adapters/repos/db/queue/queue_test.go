//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestNewDiskQueue(t *testing.T) {
	tempDir := t.TempDir()

	s := makeScheduler(t)

	q, err := NewDiskQueue(DiskQueueOptions{
		ID:        "test_queue",
		Scheduler: s,
		Logger:    newTestLogger(), Dir: tempDir,
		TaskDecoder: &mockTaskDecoder{},
	})
	require.NoError(t, err)
	require.NotNil(t, q)
	require.Equal(t, "test_queue", q.ID())
}

func TestQueuePush(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close()

	t.Run("a few tasks", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())
		pushMany(t, q, 1, 100, 200, 300)
		require.Equal(t, int64(3), q.Size())
		q.Close()
	})

	t.Run("push when closed", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())

		err := q.Close()
		require.NoError(t, err)

		err = q.Push(makeRecord(1, 100))
		require.Error(t, err)
	})

	t.Run("lazily creates chunk", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)

		require.Len(t, entries, 0)

		err = q.Push(makeRecord(1, 100))
		require.NoError(t, err)

		entries, err = os.ReadDir(q.dir)
		require.NoError(t, err)

		require.Len(t, entries, 1)
	})

	t.Run("re-open", func(t *testing.T) {
		dir := t.TempDir()
		decoder := &mockTaskDecoder{}
		q, err := NewDiskQueue(DiskQueueOptions{
			ID:           "test_queue",
			Scheduler:    s,
			Logger:       newTestLogger(),
			Dir:          dir,
			TaskDecoder:  decoder,
			StaleTimeout: 500 * time.Millisecond,
			ChunkSize:    50,
		})
		require.NoError(t, err)
		err = q.Init()
		require.NoError(t, err)

		pushMany(t, q, 1, 100, 200, 300)

		err = q.Close()
		require.NoError(t, err)

		q, err = NewDiskQueue(DiskQueueOptions{
			ID:           "test_queue",
			Scheduler:    s,
			Logger:       newTestLogger(),
			Dir:          dir,
			TaskDecoder:  decoder,
			StaleTimeout: 500 * time.Millisecond,
			ChunkSize:    50,
		})
		require.NoError(t, err)
		err = q.Init()
		require.NoError(t, err)

		require.Equal(t, int64(3), q.Size())

		err = q.Push(makeRecord(1, 100))
		require.NoError(t, err)

		require.Equal(t, int64(4), q.Size())
	})

	t.Run("keeps track of last push time", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())

		lpt := q.lastPushTime
		require.NotNil(t, lpt)

		pushMany(t, q, 1, 100, 200, 300)

		lpt = q.lastPushTime
		require.NotNil(t, lpt)

		pushMany(t, q, 1, 400, 500, 600)

		lpt2 := q.lastPushTime
		require.NotEqual(t, lpt, lpt2)
	})

	t.Run("persistence", func(t *testing.T) {
		q := makeQueueSize(t, s, discardExecutor(), 1000)

		// ensure the queue doesn't get processed
		q.Pause()

		for i := 0; i < 100; i++ {
			pushMany(t, q, 1, 100, 200, 300)
		}

		err := q.Flush()
		require.NoError(t, err)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 4)

		// first 3 are full
		for i := 0; i < 3; i++ {
			stat, err := os.Stat(filepath.Join(q.dir, entries[i].Name()))
			require.NoError(t, err)
			require.EqualValues(t, 1001, stat.Size())
		}

		// last one is partial
		stat, err := os.Stat(filepath.Join(q.dir, entries[3].Name()))
		require.NoError(t, err)
		require.EqualValues(t, 949, stat.Size())

		// ensure the last chunk is stale
		time.Sleep(q.staleTimeout)

		// dequeue all tasks
		for i := 0; i < 4; i++ {
			batch, err := q.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, batch)

			batch.Done()
		}

		// ensure all chunks are removed
		entries, err = os.ReadDir(q.dir)
		require.NoError(t, err)

		require.Len(t, entries, 0)

		// ensure the queue reports the correct size
		require.EqualValues(t, 0, q.Size())
		require.EqualValues(t, 0, q.diskUsage)
	})
}

func TestQueueDecodeTask(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close()

	t.Run("a few tasks", func(t *testing.T) {
		exec := discardExecutor()
		q := makeQueueSize(t, s, exec, 50)

		pushMany(t, q, 1, 100, 200, 300, 400, 500, 600)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)

		f, err := os.Open(filepath.Join(q.dir, entries[0].Name()))
		require.NoError(t, err)
		defer f.Close()

		batch, err := q.DequeueBatch()
		require.NoError(t, err)
		require.NotNil(t, batch)
		require.Len(t, batch.Tasks, 3)

		for i := 0; i < 3; i++ {
			task := batch.Tasks[i]
			require.NotNil(t, task)
			require.Equal(t, uint8(1), task.Op())
			require.Equal(t, uint64(100*(i+1)), task.Key())
		}

		require.Equal(t, int64(6), q.Size())

		// decoding more tasks should return nil
		batch, err = q.DequeueBatch()
		require.NoError(t, err)
		require.Nil(t, batch)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("many tasks", func(t *testing.T) {
		exec := discardExecutor()
		q := makeQueueSize(t, s, exec, 660)
		q.Pause()

		// encode 120 records
		for i := 0; i < 120; i++ {
			err := q.Push(makeRecord(uint8(i), uint64(i+1)))
			require.NoError(t, err)
		}

		// check the number of files
		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 3)
		// check if the entry name matches the regex pattern
		require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
		require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
		require.Regexp(t, `chunk-\d+\.bin`, entries[2].Name())

		// check the content of the files
		checkContent := func(fName string, size int, start, end int) {
			f, err := os.Open(filepath.Join(q.dir, fName))
			require.NoError(t, err)
			defer f.Close()

			gotSize, err := readChunkHeader(f)
			if size == 0 {
				require.ErrorIs(t, err, io.EOF)
				return
			}
			require.NoError(t, err)
			if gotSize != 0 {
				require.Equal(t, size, int(gotSize))
			}

			buf := bufio.NewReader(f)

			for i := start; i < end; i++ {
				// read the record length
				var rsizeBuf [4]byte
				_, err := io.ReadFull(buf, rsizeBuf[:])
				require.NoError(t, err)
				rSize := binary.BigEndian.Uint32(rsizeBuf[:])
				require.Equal(t, uint32(9), rSize)

				// read the record
				var recordBuf [9]byte
				_, err = io.ReadFull(buf, recordBuf[:])
				require.NoError(t, err)
				op := recordBuf[0]
				key := binary.BigEndian.Uint64(recordBuf[1:])
				require.Equal(t, uint8(i), op)
				require.Equal(t, uint64(i+1), key)
			}
		}

		checkContent(entries[0].Name(), 50, 0, 49)
		checkContent(entries[1].Name(), 50, 50, 99)

		// partial file should have 0 records because it was not flushed
		checkContent(entries[2].Name(), 0, 100, 119)

		// flush the queue
		err = q.Flush()
		require.NoError(t, err)

		// check the content of the partial file
		checkContent(entries[2].Name(), 20, 100, 119)

		// check the queue size
		size := q.Size()
		require.EqualValues(t, 120, size)

		// promote the partial file
		err = q.w.Promote()
		require.NoError(t, err)

		// check the number of files
		entries, err = os.ReadDir(q.dir)
		require.NoError(t, err)

		require.Len(t, entries, 3)
		require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
		require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
		require.Regexp(t, `chunk-\d+\.bin`, entries[2].Name())

		// check the content of the 3rd file
		checkContent(entries[2].Name(), 20, 100, 119)

		// check the queue size again
		size = q.Size()
		require.EqualValues(t, 120, size)

		// promote again, no-op
		err = q.w.Promote()
		require.NoError(t, err)
	})

	t.Run("restart", func(t *testing.T) {
		exec := discardExecutor()
		q := makeQueueSize(t, s, exec, 50)

		pushMany(t, q, 1, 100, 200, 300, 400, 500, 600)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)

		err = q.Close()
		require.NoError(t, err)

		q, err = NewDiskQueue(DiskQueueOptions{
			ID:           "test_queue",
			Scheduler:    s,
			Logger:       newTestLogger(),
			Dir:          q.dir,
			TaskDecoder:  &mockTaskDecoder{},
			StaleTimeout: 500 * time.Millisecond,
			ChunkSize:    50,
		})
		require.NoError(t, err)

		err = q.Init()
		require.NoError(t, err)

		batch, err := q.DequeueBatch()
		require.NoError(t, err)
		require.NotNil(t, batch)
		require.Len(t, batch.Tasks, 3)

		for i := 0; i < 3; i++ {
			task := batch.Tasks[i]
			require.NotNil(t, task)
			require.Equal(t, uint8(1), task.Op())
			require.Equal(t, uint64(100*(i+1)), task.Key())
		}

		require.Equal(t, int64(6), q.Size())

		// decoding more tasks should return nil
		batch, err = q.DequeueBatch()
		require.NoError(t, err)
		require.Nil(t, batch)

		err = q.Close()
		require.NoError(t, err)
	})
}

func newTestLogger() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logger
}

func BenchmarkQueuePush(b *testing.B) {
	rec := make([]byte, 10*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d, err := NewDiskQueue(DiskQueueOptions{
			Dir:         filepath.Join(b.TempDir(), "test_queue"),
			ID:          "test_queue",
			Scheduler:   makeScheduler(b),
			TaskDecoder: &mockTaskDecoder{},
		})
		require.NoError(b, err)
		b.StartTimer()

		for j := 0; j < 20_000; j++ {
			d.Push(rec)
		}
	}
}

func TestPartialChunkRecovery(t *testing.T) {
	tests := []struct {
		name     string
		truncate int64
		records  int
	}{
		{"truncate full record", -10, 4},
		{"truncate mid record", -2, 4},
		{"truncate record length", -11, 4},
		{"truncate after header", 13, 0},
		{"empty file", 0, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := makeScheduler(t, 1)
			s.Start()

			tmpDir := t.TempDir()

			_, e := streamExecutor()
			q := makeQueueWith(t, s, e, 500, tmpDir)

			// write 5 records
			for i := 0; i < 5; i++ {
				err := q.Push(bytes.Repeat([]byte{1}, 10))
				require.NoError(t, err)
			}

			// close the queue to ensure all records are flushed
			err := q.Close()
			require.NoError(t, err)

			// ensure there is a chunk file
			entries, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			require.Len(t, entries, 1)

			stat, err := os.Stat(filepath.Join(tmpDir, entries[0].Name()))
			require.NoError(t, err)
			size := stat.Size()

			// manually corrupt the file
			chunkFile := filepath.Join(tmpDir, entries[0].Name())
			if test.truncate < 0 {
				// truncate the file from the end
				err = os.Truncate(chunkFile, int64(size+test.truncate))
			} else {
				// truncate the file from the beginning
				err = os.Truncate(chunkFile, test.truncate)
			}
			require.NoError(t, err)

			// open the queue again
			// this should not return an error
			q = makeQueueWith(t, s, e, 500, tmpDir)
			require.NoError(t, err)

			s.RegisterQueue(q)
			q.Pause()

			// manually promote a partial chunk to a full chunk
			err = q.w.Promote()
			require.NoError(t, err)

			batch, err := q.DequeueBatch()
			require.NoError(t, err)
			if test.records == 0 {
				require.Nil(t, batch)
			} else {
				require.NotNil(t, batch)
				require.Len(t, batch.Tasks, test.records)
			}
		})
	}
}

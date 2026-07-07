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

package queue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
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
	defer s.Close(t.Context())

	t.Run("a few tasks", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())
		pushMany(t, q, 1, 100, 200, 300)
		require.Equal(t, int64(3), q.Size())
		q.Close(t.Context())
	})

	t.Run("push when closed", func(t *testing.T) {
		q := makeQueue(t, s, discardExecutor())

		err := q.Close(t.Context())
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

		err = q.Close(t.Context())
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
		q.Pause(t.Context())

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
	defer s.Close(t.Context())

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

		err = q.Close(t.Context())
		require.NoError(t, err)
	})

	t.Run("many tasks", func(t *testing.T) {
		exec := discardExecutor()
		q := makeQueueSize(t, s, exec, 660)
		q.Pause(t.Context())

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

		err = q.Close(t.Context())
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

		err = q.Close(t.Context())
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
			err := q.Close(t.Context())
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
			q.Pause(t.Context())

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

// chunkHeader builds a chunk file header with the given version and record count.
func chunkHeader(version byte, count uint64) []byte {
	buf := make([]byte, chunkHeaderSize)
	copy(buf, magicHeader)
	buf[len(magicHeader)] = version
	binary.BigEndian.PutUint64(buf[len(magicHeader)+1:], count)
	return buf
}

// A crash (power loss, disk full during chunk creation) can leave a chunk file
// with a truncated or garbage header. Such a file must not prevent the queue
// from starting: a file too short to contain a full header provably holds no
// records and should be removed, while a full-size file with unreadable framing
// should be quarantined as .corrupt for forensics. Only a well-formed header
// with an unknown version (likely written by a newer weaviate) should keep
// failing Init, since discarding that data would be wrong.
func TestCorruptChunkHeaderRecovery(t *testing.T) {
	// chunk files created during the test are named chunk-<unixmicro>.bin with
	// a 16-digit timestamp starting with 1. sortsBefore/sortsAfter place the
	// corrupt file on either side of the valid chunk in directory order: the
	// "after" position additionally guards chunkWriter.Open, which adopts the
	// last file in the directory as its write target.
	const (
		sortsBefore = "chunk-1.bin"
		sortsAfter  = "chunk-9999999999999999.bin"
	)

	badMagic := append(bytes.Repeat([]byte("X"), len(magicHeader)), chunkHeader(1, 3)[len(magicHeader):]...)
	badMagic = append(badMagic, 0xDE, 0xAD, 0xBE, 0xEF)

	tests := []struct {
		name            string
		file            string
		content         []byte
		wantInitErr     bool
		wantQuarantined bool
	}{
		{name: "empty file", file: sortsBefore, content: nil},
		{name: "torn header", file: sortsBefore, content: chunkHeader(1, 0)[:2]},
		{name: "torn header almost complete", file: sortsBefore, content: chunkHeader(1, 0)[:12]},
		{name: "torn header sorts last", file: sortsAfter, content: chunkHeader(1, 0)[:5]},
		{name: "bad magic", file: sortsBefore, content: badMagic, wantQuarantined: true},
		{name: "bad magic sorts last", file: sortsAfter, content: badMagic, wantQuarantined: true},
		{name: "unknown version", file: sortsBefore, content: chunkHeader(2, 3), wantInitErr: true},
	}

	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			decoder := discardExecutor()

			// write one sealed chunk with 3 records
			q := makeQueueWith(t, s, decoder, 50, tmpDir)
			q.Pause(t.Context())
			pushMany(t, q, 1, 100, 200, 300)
			err := q.w.Promote()
			require.NoError(t, err)
			err = q.Close(t.Context())
			require.NoError(t, err)

			entries, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			validChunk := filepath.Join(tmpDir, entries[0].Name())

			// simulate a crash that left a chunk file with an unreadable header
			corruptFile := filepath.Join(tmpDir, test.file)
			err = os.WriteFile(corruptFile, test.content, 0o644)
			require.NoError(t, err)

			// reopen the queue
			q, err = NewDiskQueue(DiskQueueOptions{
				ID:           "test_queue",
				Scheduler:    s,
				Logger:       newTestLogger(),
				Dir:          tmpDir,
				TaskDecoder:  decoder,
				StaleTimeout: 500 * time.Millisecond,
				ChunkSize:    50,
			})
			require.NoError(t, err)

			err = q.Init()
			if test.wantInitErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err, "a chunk file with an unreadable header must not prevent the queue from starting")

			// the valid chunk must be untouched and its records still counted
			_, err = os.Stat(validChunk)
			require.NoError(t, err)
			require.EqualValues(t, 3, q.Size())

			if test.wantQuarantined {
				// the corrupt file is set aside for forensics, not deleted
				_, err = os.Stat(corruptFile + ".corrupt")
				require.NoError(t, err, "corrupt chunk should be quarantined as .corrupt")

				// backups must not pick it up
				files, err := q.ListFiles(t.Context(), tmpDir)
				require.NoError(t, err)
				require.Contains(t, files, filepath.Base(validChunk))
				for _, f := range files {
					require.False(t, strings.HasSuffix(f, ".corrupt"), "quarantined chunk should not appear in backup list")
				}
			}

			// either way, the corrupt file no longer blocks the queue's path
			_, err = os.Stat(corruptFile)
			require.ErrorIs(t, err, os.ErrNotExist)

			// the valid records must still be dequeueable
			batch, err := q.DequeueBatch()
			require.NoError(t, err)
			require.NotNil(t, batch)
			require.Len(t, batch.Tasks, 3)
			batch.Done()

			// the queue must accept new pushes
			err = q.Push(makeRecord(1, 400))
			require.NoError(t, err)

			err = q.Close(t.Context())
			require.NoError(t, err)

			// a restart must not trip over what recovery left behind, in
			// particular the writer must not adopt a .corrupt file as its
			// write target
			q, err = NewDiskQueue(DiskQueueOptions{
				ID:           "test_queue",
				Scheduler:    s,
				Logger:       newTestLogger(),
				Dir:          tmpDir,
				TaskDecoder:  decoder,
				StaleTimeout: 500 * time.Millisecond,
				ChunkSize:    50,
			})
			require.NoError(t, err)
			err = q.Init()
			require.NoError(t, err, "restart after recovery must succeed")
			require.EqualValues(t, 1, q.Size())
			err = q.Close(t.Context())
			require.NoError(t, err)
		})
	}
}

// An error opening or reading a chunk file (e.g. permissions, I/O) is not
// evidence of corruption: quarantining or removing the chunk in that case
// could silently drop valid tasks, so initialization must fail fast and leave
// the file untouched.
func TestCorruptChunkHeaderRecoveryUnreadableFile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission errors are not enforced for root")
	}

	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	tmpDir := t.TempDir()
	decoder := discardExecutor()

	// write one sealed chunk with 3 records
	q := makeQueueWith(t, s, decoder, 50, tmpDir)
	q.Pause(t.Context())
	pushMany(t, q, 1, 100, 200, 300)
	err := q.w.Promote()
	require.NoError(t, err)
	err = q.Close(t.Context())
	require.NoError(t, err)

	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	validChunk := filepath.Join(tmpDir, entries[0].Name())

	// make the chunk unreadable: opening it fails with a permission error
	err = os.Chmod(validChunk, 0o000)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Chmod(validChunk, 0o644) })

	q, err = NewDiskQueue(DiskQueueOptions{
		ID:           "test_queue",
		Scheduler:    s,
		Logger:       newTestLogger(),
		Dir:          tmpDir,
		TaskDecoder:  decoder,
		StaleTimeout: 500 * time.Millisecond,
		ChunkSize:    50,
	})
	require.NoError(t, err)

	// the queue must fail fast instead of discarding a chunk it cannot read
	err = q.Init()
	require.Error(t, err, "an unreadable chunk file must fail initialization, not be salvaged")

	// the chunk file must be left untouched
	_, err = os.Stat(validChunk)
	require.NoError(t, err)
	_, err = os.Stat(validChunk + ".corrupt")
	require.ErrorIs(t, err, os.ErrNotExist, "an unreadable chunk file must not be quarantined")
}

// writeSealedChunk creates a queue with a single sealed chunk containing
// 3 records and returns the chunk file path.
func writeSealedChunk(t *testing.T, s *Scheduler, dir string) string {
	t.Helper()

	q := makeQueueWith(t, s, discardExecutor(), 50, dir)
	q.Pause(t.Context())
	pushMany(t, q, 1, 100, 200, 300)
	err := q.w.Promote()
	require.NoError(t, err)
	err = q.Close(t.Context())
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	return filepath.Join(dir, entries[0].Name())
}

func patchFile(t *testing.T, path string, off int64, data []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteAt(data, off)
	require.NoError(t, err)
}

// The record count in a chunk header and the per-record length prefixes are
// read from disk and may be corrupt. DequeueBatch must not size allocations
// from them unvalidated: a corrupt count panics the scheduler goroutine,
// which is never restarted, silently halting async indexing for every queue
// on the node, and a corrupt length can allocate up to 4GB.
func TestDequeueBatchCorruptChunk(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	newQueue := func(t *testing.T, dir string) *DiskQueue {
		q, err := NewDiskQueue(DiskQueueOptions{
			ID:           "test_queue",
			Scheduler:    s,
			Logger:       newTestLogger(),
			Dir:          dir,
			TaskDecoder:  discardExecutor(),
			StaleTimeout: 500 * time.Millisecond,
			ChunkSize:    50,
		})
		require.NoError(t, err)
		err = q.Init()
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = q.Close(context.Background())
		})
		return q
	}

	t.Run("corrupt record count", func(t *testing.T) {
		tmpDir := t.TempDir()
		chunkFile := writeSealedChunk(t, s, tmpDir)

		// corrupt the header's record count with an absurdly large value
		var countBuf [8]byte
		binary.BigEndian.PutUint64(countBuf[:], math.MaxUint64)
		patchFile(t, chunkFile, int64(len(magicHeader)+1), countBuf[:])

		q := newQueue(t, tmpDir)

		// the corrupt count must not be trusted to size allocations,
		// and the valid records must still be returned
		var batch *Batch
		var err error
		require.NotPanics(t, func() {
			batch, err = q.DequeueBatch()
		})
		require.NoError(t, err)
		require.NotNil(t, batch)
		require.Len(t, batch.Tasks, 3)
		batch.Done()
		require.EqualValues(t, 0, q.Size())
	})

	t.Run("corrupt record length", func(t *testing.T) {
		tmpDir := t.TempDir()
		chunkFile := writeSealedChunk(t, s, tmpDir)

		// corrupt the first record's length prefix, located right after the header
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], 1<<31)
		patchFile(t, chunkFile, int64(len(magicHeader)+1+8), lenBuf[:])

		q := newQueue(t, tmpDir)

		// the corrupt length must be quarantined before any allocation
		// so the queue does not get stuck on a bad sealed chunk.
		var batch *Batch
		var err error
		require.NotPanics(t, func() {
			batch, err = q.DequeueBatch()
		})
		require.NoError(t, err)
		require.Nil(t, batch)
		_, err = os.Stat(chunkFile + ".corrupt")
		require.NoError(t, err, "corrupt chunk should be quarantined as .corrupt")
		_, err = os.Stat(chunkFile)
		require.ErrorIs(t, err, os.ErrNotExist)
		require.EqualValues(t, 0, q.Size())

		batch, err = q.DequeueBatch()
		require.NoError(t, err)
		require.Nil(t, batch)
	})
}

// A sealed chunk can be torn mid-file by a crash (e.g. power loss before its
// tail reached disk). The records before the tear are still valid and must be
// salvaged, and the file must be quarantined: otherwise the chunk is never
// removed and its records stay counted, so the queue never drains and the
// shard reports INDEXING forever.
func TestDequeueBatchTornChunk(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	truncateBy := func(t *testing.T, path string, delta int64) {
		t.Helper()

		fi, err := os.Stat(path)
		require.NoError(t, err)
		err = os.Truncate(path, fi.Size()+delta)
		require.NoError(t, err)
	}

	// each record is 4 bytes of length prefix + 9 bytes of payload
	tests := []struct {
		name    string
		corrupt func(t *testing.T, path string)
		tasks   int
	}{
		{
			name: "torn mid record",
			corrupt: func(t *testing.T, path string) {
				truncateBy(t, path, -2)
			},
			tasks: 2,
		},
		{
			name: "torn mid length prefix",
			corrupt: func(t *testing.T, path string) {
				truncateBy(t, path, -11)
			},
			tasks: 2,
		},
		{
			name: "torn before first record",
			corrupt: func(t *testing.T, path string) {
				fi, err := os.Stat(path)
				require.NoError(t, err)
				err = os.Truncate(path, fi.Size()-3*13+1)
				require.NoError(t, err)
			},
			tasks: 0,
		},
		{
			name: "zero record length",
			corrupt: func(t *testing.T, path string) {
				// zero out the third record's length prefix
				f, err := os.OpenFile(path, os.O_RDWR, 0o644)
				require.NoError(t, err)
				defer f.Close()
				_, err = f.WriteAt(make([]byte, 4), int64(chunkHeaderSize+2*13))
				require.NoError(t, err)
			},
			tasks: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			decoder := discardExecutor()

			// write one sealed chunk with 3 records
			q := makeQueueWith(t, s, decoder, 50, tmpDir)
			q.Pause(t.Context())
			pushMany(t, q, 1, 100, 200, 300)
			err := q.w.Promote()
			require.NoError(t, err)
			err = q.Close(t.Context())
			require.NoError(t, err)

			entries, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			chunkFile := filepath.Join(tmpDir, entries[0].Name())

			test.corrupt(t, chunkFile)

			// reopen the queue: the header is intact, so the chunk is listed
			// with its full record count
			q, err = NewDiskQueue(DiskQueueOptions{
				ID:           "test_queue",
				Scheduler:    s,
				Logger:       newTestLogger(),
				Dir:          tmpDir,
				TaskDecoder:  decoder,
				StaleTimeout: 500 * time.Millisecond,
				ChunkSize:    50,
			})
			require.NoError(t, err)
			err = q.Init()
			require.NoError(t, err)
			require.EqualValues(t, 3, q.Size())

			// the records before the tear must be salvaged
			batch, err := q.DequeueBatch()
			require.NoError(t, err, "a torn chunk must be salvaged, not fail the dequeue")
			if test.tasks == 0 {
				require.Nil(t, batch)
			} else {
				require.NotNil(t, batch)
				require.Len(t, batch.Tasks, test.tasks)
				batch.Done()
			}

			// the torn chunk must be quarantined so it is no longer scheduled
			_, err = os.Stat(chunkFile + ".corrupt")
			require.NoError(t, err, "torn chunk should be quarantined as .corrupt")
			_, err = os.Stat(chunkFile)
			require.ErrorIs(t, err, os.ErrNotExist)

			// its records must no longer be counted, so the queue can drain
			require.EqualValues(t, 0, q.Size())

			batch, err = q.DequeueBatch()
			require.NoError(t, err)
			require.Nil(t, batch)
		})
	}
}

func TestQueueAutoReleaseResources(t *testing.T) {
	t.Parallel()

	t.Run("releases bufio writer after inactivity period", func(t *testing.T) {
		t.Parallel()

		s := makeScheduler(t)
		s.Start()
		defer s.Close(t.Context())

		q := makeQueue(t, s, discardExecutor())
		q.Pause(t.Context()) // prevent scheduler from processing the queue
		q.inactivityPeriod = 400 * time.Millisecond
		pushMany(t, q, 1, 100, 200, 300)
		require.Equal(t, int64(3), q.Size())
		q.staleTimeout = 0 // disable stale timeout for this test

		batch, err := q.DequeueBatch()
		require.NoError(t, err)
		batch.Done()

		// bufio writer should be in use
		require.NotNil(t, q.w.w.w)

		// wait for longer than the inactivity period
		time.Sleep(700 * time.Millisecond)

		// call DequeueBatch to trigger the inactivity check
		_, err = q.DequeueBatch()
		require.NoError(t, err)

		// bufio writer should be released
		require.Nil(t, q.w.w.w)

		// push another record to ensure the queue still works
		err = q.Push(makeRecord(1, 400))
		require.NoError(t, err)
		require.Equal(t, int64(1), q.Size())

		_, err = q.DequeueBatch()
		require.NoError(t, err)

		// bufio writer should be in use again
		require.NotNil(t, q.w.w.w)
	})

	t.Run("doesn't release bufio writer after inactivity period if queue is not empty", func(t *testing.T) {
		t.Parallel()

		s := makeScheduler(t)
		s.Start()
		defer s.Close(t.Context())

		q := makeQueue(t, s, discardExecutor())
		q.Pause(t.Context()) // prevent scheduler from processing the queue
		q.inactivityPeriod = 400 * time.Millisecond
		pushMany(t, q, 1, 100, 200, 300)
		require.Equal(t, int64(3), q.Size())
		q.staleTimeout = 0 // disable stale timeout for this test

		// bufio writer should be in use
		require.NotNil(t, q.w.w.w)

		// wait for longer than the inactivity period
		time.Sleep(700 * time.Millisecond)

		// call DequeueBatch to trigger the inactivity check
		batch, err := q.DequeueBatch()
		require.NoError(t, err)
		batch.Done()

		// bufio writer should not be released
		require.NotNil(t, q.w.w.w)
	})
}

func TestQueueListFiles(t *testing.T) {
	ctx := t.Context()
	s := makeScheduler(t, 1)
	s.Start()

	tmpDir := t.TempDir()

	_, e := streamExecutor()
	q := makeQueueWith(t, s, e, 100, tmpDir)

	// ListFiles on empty queue
	files, err := q.ListFiles(ctx, tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 0)

	// write 50 records
	for i := 0; i < 50; i++ {
		err := q.Push(bytes.Repeat([]byte{1}, 10))
		require.NoError(t, err)
	}

	// close the queue to ensure all records are flushed
	err = q.Close(t.Context())
	require.NoError(t, err)

	// ensure there is a chunk file
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 8)

	// ListFiles on non-empty queue
	files, err = q.ListFiles(ctx, tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 8)

	// check that returned file names are relative to basePath
	for i, f := range files {
		require.Equal(t, entries[i].Name(), f)
	}

	// use a different basePath
	p := strings.Split(tmpDir, string(os.PathSeparator))
	base, tail := p[:3], p[3:]
	basePath := strings.Join(base, string(os.PathSeparator))
	files, err = q.ListFiles(ctx, basePath)
	require.NoError(t, err)
	require.Len(t, files, 8)

	// check that returned file names are relative to new basePath
	for i, f := range files {
		expected := strings.Join(append(tail, entries[i].Name()), string(os.PathSeparator))
		require.Equal(t, expected, f)
	}
}

func TestEnableMaintenanceMode(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	q := makeQueueSize(t, s, discardExecutor(), 50)
	q.Pause(t.Context())

	pushMany(t, q, 1, 100, 200, 300)
	err := q.Flush()
	require.NoError(t, err)

	// ensure the partial chunk is promoted
	time.Sleep(q.staleTimeout)
	batch, err := q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, batch)

	// record the chunk file path before Done
	entries, err := os.ReadDir(q.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	chunkFile := filepath.Join(q.dir, entries[0].Name())

	initialCount := q.recordCount
	initialUsage := q.diskUsage

	// enable maintenance mode and mark batch done
	q.EnableMaintenanceMode()
	batch.Done()

	// chunk file must still exist on disk
	_, err = os.Stat(chunkFile)
	require.NoError(t, err, "chunk file should still exist when in maintenance mode")

	// tombstone must exist
	tombstonePath := chunkFile + ".processed"
	_, err = os.Stat(tombstonePath)
	require.NoError(t, err, "tombstone file should exist")

	// counters must be decremented
	require.Less(t, q.recordCount, initialCount)
	require.Less(t, q.diskUsage, initialUsage)
}

func TestDisableMaintenanceMode(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	q := makeQueueSize(t, s, discardExecutor(), 50)
	q.Pause(t.Context())

	pushMany(t, q, 1, 100, 200, 300)
	err := q.Flush()
	require.NoError(t, err)

	time.Sleep(q.staleTimeout)
	batch, err := q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, batch)

	entries, err := os.ReadDir(q.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	chunkFile := filepath.Join(q.dir, entries[0].Name())
	tombstonePath := chunkFile + ".processed"

	q.EnableMaintenanceMode()
	batch.Done()

	// both files exist after maintenance mode+Done
	_, err = os.Stat(chunkFile)
	require.NoError(t, err)
	_, err = os.Stat(tombstonePath)
	require.NoError(t, err)

	// disable maintenance mode: both files must be gone
	err = q.DisableMaintenanceMode()
	require.NoError(t, err)

	_, err = os.Stat(chunkFile)
	require.ErrorIs(t, err, os.ErrNotExist, "chunk file should be deleted after disabling maintenance mode")

	_, err = os.Stat(tombstonePath)
	require.ErrorIs(t, err, os.ErrNotExist, "tombstone should be deleted after disabling maintenance mode")
}

func TestEnableMaintenanceModeCrashRecovery(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	tmpDir := t.TempDir()
	decoder := &mockTaskDecoder{}
	q := makeQueueWith(t, s, decoder, 50, tmpDir)
	q.Pause(t.Context())

	// push 6 tasks → 2 full chunks (3 each at chunkSize=50)
	pushMany(t, q, 1, 100, 200, 300, 400, 500, 600)
	err := q.Flush()
	require.NoError(t, err)

	err = q.Close(t.Context())
	require.NoError(t, err)

	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// simulate crash: manually create a tombstone for the first chunk
	// (as if it was processed during maintenance mode but the process crashed before cleanup)
	firstChunk := filepath.Join(tmpDir, entries[0].Name())
	tombstonePath := firstChunk + ".processed"
	err = os.WriteFile(tombstonePath, []byte{}, 0o644)
	require.NoError(t, err)

	// reopen the queue — Init() should clean up the tombstoned chunk
	q2 := makeQueueWith(t, s, decoder, 50, tmpDir)
	require.Equal(t, int64(3), q2.Size()) // only the second chunk remains

	// tombstoned chunk and its tombstone are gone
	_, err = os.Stat(firstChunk)
	require.ErrorIs(t, err, os.ErrNotExist, "tombstoned chunk should be deleted on init")
	_, err = os.Stat(tombstonePath)
	require.ErrorIs(t, err, os.ErrNotExist, "tombstone should be deleted on init")

	// remaining chunk can be dequeued normally
	time.Sleep(q2.staleTimeout)
	batch, err := q2.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Len(t, batch.Tasks, 3)
}

func TestListFilesExcludesTombstoned(t *testing.T) {
	s := makeScheduler(t)
	s.Start()
	defer s.Close(t.Context())

	tmpDir := t.TempDir()
	_, e := streamExecutor()
	q := makeQueueWith(t, s, e, 50, tmpDir)
	q.Pause(t.Context())

	// push 6 tasks → 2 full chunks
	pushMany(t, q, 1, 100, 200, 300, 400, 500, 600)
	err := q.Flush()
	require.NoError(t, err)

	// dequeue and process the first chunk in maintenance mode
	time.Sleep(q.staleTimeout)
	batch, err := q.DequeueBatch()
	require.NoError(t, err)
	require.NotNil(t, batch)

	q.EnableMaintenanceMode()
	batch.Done()

	// tombstone must exist, chunk file must still be present
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	var tombstoneCount, chunkCount int
	for _, e := range entries {
		if processedChunkFilePattern.MatchString(e.Name()) {
			tombstoneCount++
		} else if chunkFilePattern.Match([]byte(e.Name())) {
			chunkCount++
		}
	}
	require.Equal(t, 1, tombstoneCount, "one tombstone should exist")
	require.Equal(t, 2, chunkCount, "both chunk files should still exist on disk")

	// ListFiles must exclude the tombstoned chunk
	files, err := q.ListFiles(t.Context(), tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 1, "only the unprocessed chunk should appear in backup list")

	for _, f := range files {
		require.False(t, strings.HasSuffix(f, ".processed"), "tombstone should not appear in list")
	}
}

func TestQueueForceSwitch(t *testing.T) {
	ctx := t.Context()
	s := makeScheduler(t, 1)
	s.Start()

	tmpDir := t.TempDir()

	_, e := streamExecutor()
	q := makeQueueWith(t, s, e, 100, tmpDir)

	// ListFiles on empty queue
	files, err := q.ListFiles(ctx, tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 0)

	// write 50 records
	for i := 0; i < 50; i++ {
		err := q.Push(bytes.Repeat([]byte{1}, 10))
		require.NoError(t, err)
	}

	// ensure there is a chunk file
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 8)

	// pause the queue
	q.Pause(t.Context())
	require.NoError(t, q.Wait(ctx))

	// call ForceSwitch to promote the last chunk
	got, err := q.ForceSwitch(ctx, q.dir)
	require.NoError(t, err)

	// compare
	for i, f := range got {
		require.Equal(t, entries[i].Name(), f)
	}

	// write something
	err = q.Push(bytes.Repeat([]byte{1}, 10))
	require.NoError(t, err)

	// ListFiles
	files, err = q.ListFiles(ctx, tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 9)
}

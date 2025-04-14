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
	"context"
	"encoding/binary"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	t.Run("start and close", func(t *testing.T) {
		s := makeScheduler(t)
		s.Start()
		time.Sleep(100 * time.Millisecond)
		err := s.Close()
		require.NoError(t, err)
	})

	t.Run("start twice", func(t *testing.T) {
		s := makeScheduler(t)
		s.Start()
		s.Start()
		err := s.Close()
		require.NoError(t, err)
	})

	t.Run("commands before start", func(t *testing.T) {
		s := makeScheduler(t)
		err := s.Close()
		require.NoError(t, err)
		s.PauseQueue("test")
		s.ResumeQueue("test")
		s.Wait("test")
	})

	t.Run("paused queue should not process tasks", func(t *testing.T) {
		s := makeScheduler(t)
		s.Start()

		ch, e := streamExecutor()
		q := makeQueue(t, s, e)

		pushMany(t, q, 1, 100, 200, 300)

		require.EqualValues(t, 100, <-ch)
		require.EqualValues(t, 200, <-ch)
		require.EqualValues(t, 300, <-ch)

		s.PauseQueue(q.ID())

		pushMany(t, q, 1, 400, 500, 600)

		select {
		case <-ch:
			t.Fatal("should not have been called")
		case <-time.After(500 * time.Millisecond):
		}

		s.ResumeQueue(q.ID())

		require.EqualValues(t, 400, <-ch)
		require.EqualValues(t, 500, <-ch)
		require.EqualValues(t, 600, <-ch)

		err := q.Close()
		require.NoError(t, err)
	})

	t.Run("chunk files are removed properly", func(t *testing.T) {
		s := makeScheduler(t, 3)
		s.Start()

		ch, e := streamExecutor()
		q := makeQueue(t, s, e)

		pushMany(t, q, 1, 100, 200, 300)

		res := make([]uint64, 3)
		res[0] = <-ch
		res[1] = <-ch
		res[2] = <-ch
		slices.Sort(res)
		require.Equal(t, []uint64{100, 200, 300}, res)

		time.Sleep(100 * time.Millisecond)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 0)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("chunks are processed in order", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.Start()

		ch, e := streamExecutor()
		q := makeQueue(t, s, e)
		// override chunk size for testing
		q.w.maxSize = 9000

		// consume the channel in a separate goroutine
		var res []uint64
		done := make(chan struct{})
		go func() {
			defer close(done)

			for i := range ch {
				res = append(res, i)
			}
		}()

		for i := 0; i < 10; i++ {
			var batch []uint64
			for j := 0; j < 1000; j++ {
				batch = append(batch, uint64(i*1000+j))
			}

			pushMany(t, q, 1, batch...)
		}

		for i := 0; i < 10; i++ {
			if q.Size() == 0 {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}
		require.Zero(t, q.Size())

		close(ch)
		<-done

		for i := 0; i < 10000; i++ {
			require.EqualValues(t, i, res[i])
		}

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 0)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("chunk is promoted if full", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.Start()

		_, e := streamExecutor()
		q := makeQueue(t, s, e)
		q.staleTimeout = 1 * time.Second

		// override chunk size for testing
		q.w.maxSize = 90

		var batch []uint64
		for i := 0; i < 11; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q, 1, batch...)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("does not read partial chunk", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.Start()

		ch, e := streamExecutor()
		q := makeQueue(t, s, e)
		q.staleTimeout = 1 * time.Second

		var batch []uint64
		for i := 0; i < 10; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q, 1, batch...)

		entries, err := os.ReadDir(q.dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ch:
			t.Fatal("should not have been called")
		}

		err = q.Close()
		require.NoError(t, err)
	})
}

func makeScheduler(t testing.TB, workers ...int) *Scheduler {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	w := 1
	if len(workers) > 0 {
		w = workers[0]
	}

	return NewScheduler(SchedulerOptions{
		Logger:           logger,
		Workers:          w,
		ScheduleInterval: 50 * time.Millisecond,
	})
}

func makeQueueWith(t *testing.T, s *Scheduler, decoder TaskDecoder, chunkSize uint64, dir string) *DiskQueue {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	q, err := NewDiskQueue(DiskQueueOptions{
		ID:           "test_queue",
		Scheduler:    s,
		Logger:       newTestLogger(),
		Dir:          dir,
		TaskDecoder:  decoder,
		StaleTimeout: 500 * time.Millisecond,
		ChunkSize:    chunkSize,
	})
	require.NoError(t, err)

	err = q.Init()
	require.NoError(t, err)

	s.RegisterQueue(q)

	return q
}

func makeQueueSize(t *testing.T, s *Scheduler, decoder TaskDecoder, chunkSize uint64) *DiskQueue {
	return makeQueueWith(t, s, decoder, chunkSize, t.TempDir())
}

func makeQueue(t *testing.T, s *Scheduler, decoder TaskDecoder) *DiskQueue {
	return makeQueueSize(t, s, decoder, 0)
}

func makeRecord(op uint8, id uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = op
	binary.BigEndian.PutUint64(buf[1:], id)
	return buf
}

func pushMany(t testing.TB, q *DiskQueue, op uint8, ids ...uint64) {
	t.Helper()

	for _, id := range ids {
		err := q.Push(makeRecord(op, id))
		require.NoError(t, err)
	}

	err := q.Flush()
	require.NoError(t, err)
}

func streamExecutor() (chan uint64, *mockTaskDecoder) {
	ch := make(chan uint64)

	return ch, &mockTaskDecoder{
		execFn: func(ctx context.Context, t *mockTask) error {
			ch <- t.key
			return nil
		},
	}
}

func discardExecutor() *mockTaskDecoder {
	return &mockTaskDecoder{
		execFn: func(ctx context.Context, t *mockTask) error {
			return nil
		},
	}
}

type mockTaskDecoder struct {
	execFn func(context.Context, *mockTask) error
}

func (m *mockTaskDecoder) DecodeTask(data []byte) (Task, error) {
	t := mockTask{
		op:  data[0],
		key: binary.BigEndian.Uint64(data[1:]),
	}

	t.execFn = func(ctx context.Context) error {
		return m.execFn(ctx, &t)
	}

	return &t, nil
}

type mockTask struct {
	op     uint8
	key    uint64
	execFn func(context.Context) error
}

func (m *mockTask) Op() uint8 {
	return m.op
}

func (m *mockTask) Key() uint64 {
	return m.key
}

func (m *mockTask) Execute(ctx context.Context) error {
	return m.execFn(ctx)
}

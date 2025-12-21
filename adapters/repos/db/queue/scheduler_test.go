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

package queue

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

	t.Run("invalid tasks", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.ScheduleInterval = 200 * time.Millisecond
		s.RetryInterval = 100 * time.Millisecond
		s.Start()

		called := make(map[uint64]int)

		started := make(chan struct{})
		e := mockTaskDecoder{
			execFn: func(ctx context.Context, t *mockTask) error {
				if t.key == 0 {
					close(started)
				}

				called[t.key]++
				if t.key == 3 {
					return errors.New("invalid task")
				}

				return nil
			},
		}

		q := makeQueue(t, s, &e)

		var batch []uint64
		for i := 0; i < 30; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q, 1, batch...)

		s.Schedule(t.Context())
		<-started
		s.Wait(q.ID())
		for i := 0; i < 30; i++ {
			require.Equal(t, 1, called[uint64(i)], "task %d should have been executed once", i)
		}

		err := q.Close()
		require.NoError(t, err)
	})

	t.Run("transient error", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.ScheduleInterval = 200 * time.Millisecond
		s.RetryInterval = 100 * time.Millisecond
		s.Start()

		called := make(map[uint64]int)

		started := make(chan struct{})
		e := mockTaskDecoder{
			execFn: func(ctx context.Context, t *mockTask) error {
				if t.key == 0 {
					close(started)
				}

				called[t.key]++
				if t.key == 3 && called[t.key] < 3 {
					return enterrors.NewNotEnoughMemory("transient OOM")
				}

				return nil
			},
		}

		q := makeQueue(t, s, &e)

		var batch []uint64
		for i := 0; i < 30; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q, 1, batch...)

		s.Schedule(t.Context())
		<-started
		s.Wait(q.ID())

		for i := 0; i < 30; i++ {
			if i == 3 {
				require.Equal(t, 3, called[uint64(i)])
				continue
			}

			require.Equal(t, 1, called[uint64(i)], "task %d should have been executed once", i)
		}

		err := q.Close()
		require.NoError(t, err)
	})

	t.Run("permanent error", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.ScheduleInterval = 200 * time.Millisecond
		s.RetryInterval = 100 * time.Millisecond
		s.Start()

		called := make(map[uint64]int)

		started := make(chan struct{})
		e := mockTaskDecoder{
			execFn: func(ctx context.Context, t *mockTask) error {
				if t.key == 0 {
					close(started)
				}

				called[t.key]++
				if t.key == 3 {
					return common.ErrWrongDimensions
				}

				return nil
			},
		}

		q := makeQueue(t, s, &e)

		var batch []uint64
		for i := 0; i < 30; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q, 1, batch...)

		s.Schedule(t.Context())
		<-started
		s.Wait(q.ID())

		for i := 0; i < 30; i++ {
			require.Equal(t, 1, called[uint64(i)], "task %d should have been executed once", i)
		}

		err := q.Close()
		require.NoError(t, err)
	})

	t.Run("should use any available worker", func(t *testing.T) {
		s := makeScheduler(t, 3 /* workers */)
		s.Start()

		ch1, e1 := streamExecutor()
		q1 := makeQueue(t, s, e1)
		q1.w.maxSize = 1000 // about 75 records per chunk
		ch2, e2 := streamExecutor()
		q2 := makeQueue(t, s, e2)
		q2.w.maxSize = 1000

		// q1 uses only one worker
		for range 100 {
			pushMany(t, q1, 1, 1, 1, 1) // 1 partition
		}
		// q2 uses all
		for range 100 {
			pushMany(t, q2, 1, 3, 4, 5) // 3 partitions
		}

		// do not read from ch1 yet to simulate a busy worker.

		// instead read from ch2 first.
		res := make([]uint64, 300)
		for i := range 300 {
			res[i] = <-ch2
		}
		slices.Sort(res)
		for i, v := range res {
			if i < 100 {
				require.EqualValues(t, 3, v)
			} else if i < 200 {
				require.EqualValues(t, 4, v)
			} else {
				require.EqualValues(t, 5, v)
			}
		}

		// now read from ch1
		for range 300 {
			require.EqualValues(t, 1, <-ch1)
		}

		err := q1.Close()
		require.NoError(t, err)
		err = q2.Close()
		require.NoError(t, err)
	})

	t.Run("should run one queue per group", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.ScheduleInterval = 100 * time.Second
		s.Start()

		// grouped queues
		ch11, e11 := streamExecutor()
		ch12, e12 := streamExecutor()
		ch20, e20 := streamExecutor()
		ch21, e21 := streamExecutor()
		q11 := makeQueueWithGroup(t, s, e11, 0, t.TempDir(), "group1")
		q12 := makeQueueWithGroup(t, s, e12, 0, t.TempDir(), "group1")
		q20 := makeQueueWithGroup(t, s, e20, 0, t.TempDir(), "group2")
		q21 := makeQueueWithGroup(t, s, e21, 0, t.TempDir(), "group2")

		// non-grouped queues
		ch3, e3 := streamExecutor()
		q3 := makeQueueWithGroup(t, s, e3, 0, t.TempDir(), "group3")

		ch4, e4 := streamExecutor()
		q4 := makeQueueWithGroup(t, s, e4, 0, t.TempDir(), "group4")

		var batch []uint64
		for i := 0; i < 10; i++ {
			batch = append(batch, uint64(i))
		}
		pushMany(t, q11, 1, batch...)
		pushMany(t, q12, 1, batch...)
		pushMany(t, q20, 1, batch...)
		pushMany(t, q21, 1, batch...)
		pushMany(t, q3, 1, batch...)
		pushMany(t, q4, 1, batch...)

		s.Schedule(t.Context())

		// group 1
		res1 := make([]uint64, 20)
		for i := range 20 {
			if i%2 == 0 {
				res1[i] = <-ch11
			} else {
				res1[i] = <-ch12
			}
		}

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
		RetryInterval:    100 * time.Millisecond,
	})
}

var queueIDCounter atomic.Int32

func makeQueueWith(t *testing.T, s *Scheduler, decoder TaskDecoder, chunkSize uint64, dir string) *DiskQueue {
	t.Helper()

	return makeQueueWithGroup(t, s, decoder, chunkSize, dir, "")
}

func makeQueueWithGroup(t *testing.T, s *Scheduler, decoder TaskDecoder, chunkSize uint64, dir string, group string) *DiskQueue {
	t.Helper()

	q, err := NewDiskQueue(DiskQueueOptions{
		ID:           fmt.Sprintf("test_queue_%d", queueIDCounter.Add(1)),
		Scheduler:    s,
		Logger:       newTestLogger(),
		Dir:          dir,
		TaskDecoder:  decoder,
		StaleTimeout: 500 * time.Millisecond,
		ChunkSize:    chunkSize,
		Group:        group,
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

func readFromOnlyOneChan[T any](t *testing.T, n int, chan1, chan2 chan T) []T {
	t.Helper()

	var v T
	var fromChan1 bool
	var res []T

	ctx := t.Context()

	select {
	case <-ctx.Done():
		t.Fatalf("context done before reading from channels")
	case v = <-chan1:
		fromChan1 = true
	case v = <-chan2:
		fromChan1 = false
	}

	res = append(res, v)

	for i := 1; i < n; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("context done before reading from channels")
		case vv := <-chan1:
			if !fromChan1 {
				t.Fatalf("expected to read from chan2 only")
			}
			res = append(res, vv)
		case vv := <-chan2:
			if fromChan1 {
				t.Fatalf("expected to read from chan1 only")
			}
			res = append(res, vv)
		}
	}
}

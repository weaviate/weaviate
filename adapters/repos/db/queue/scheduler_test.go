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
		q, err := New(s, "test", t.TempDir(), e)
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		require.EqualValues(t, 100, <-ch)
		require.EqualValues(t, 200, <-ch)
		require.EqualValues(t, 300, <-ch)

		s.PauseQueue(q.ID())

		err = q.Push(1, 400, 500, 600)
		require.NoError(t, err)

		select {
		case <-ch:
			t.Fatal("should not have been called")
		case <-time.After(500 * time.Millisecond):
		}

		s.ResumeQueue(q.ID())

		require.EqualValues(t, 400, <-ch)
		require.EqualValues(t, 500, <-ch)
		require.EqualValues(t, 600, <-ch)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("chunk files are removed properly", func(t *testing.T) {
		s := makeScheduler(t, 3)
		s.Start()

		ch, e := streamExecutor()
		dir := t.TempDir()
		q, err := New(s, "test", dir, e)
		require.NoError(t, err)

		err = q.Push(1, 100, 200, 300)
		require.NoError(t, err)

		res := make([]uint64, 3)
		res[0] = <-ch
		res[1] = <-ch
		res[2] = <-ch
		slices.Sort(res)
		require.Equal(t, []uint64{100, 200, 300}, res)

		time.Sleep(100 * time.Millisecond)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 0)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("chunks are processing in order", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.Start()

		ch, e := streamExecutor()
		dir := t.TempDir()
		q, err := New(s, "test", dir, e)
		require.NoError(t, err)
		// override chunk size for testing
		q.enc.chunkSize = 9000

		// consume the channel in a separate goroutine
		var res []uint64
		go func() {
			for i := range ch {
				res = append(res, i)
			}
		}()

		for i := 0; i < 10; i++ {
			var batch []uint64
			for j := 0; j < 1000; j++ {
				batch = append(batch, uint64(i*1000+j))
			}

			err = q.Push(1, batch...)
			require.NoError(t, err)
		}

		for i := 0; i < 10; i++ {
			if q.Size() == 0 {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}
		require.Zero(t, q.Size())

		close(ch)

		for i := 0; i < 10000; i++ {
			require.EqualValues(t, i, res[i])
		}

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 0)

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("chunk is promoted if full", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.SchedulerOptions.StaleTimeout = 1 * time.Second
		s.Start()

		_, e := streamExecutor()
		dir := t.TempDir()
		q, err := New(s, "test", dir, e)
		require.NoError(t, err)
		// override chunk size for testing
		q.enc.chunkSize = 90

		var batch []uint64
		for i := 0; i < 11; i++ {
			batch = append(batch, uint64(i))
		}
		err = q.Push(1, batch...)
		require.NoError(t, err)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.NotEqual(t, "chunk.bin.partial", entries[0].Name())
		require.Equal(t, "chunk.bin.partial", entries[1].Name())

		err = q.Close()
		require.NoError(t, err)
	})

	t.Run("does not read partial chunk", func(t *testing.T) {
		s := makeScheduler(t, 1)
		s.SchedulerOptions.StaleTimeout = 1 * time.Second
		s.Start()

		ch, e := streamExecutor()
		dir := t.TempDir()
		q, err := New(s, "test", dir, e)
		require.NoError(t, err)

		var batch []uint64
		for i := 0; i < 10; i++ {
			batch = append(batch, uint64(i))
		}
		err = q.Push(1, batch...)
		require.NoError(t, err)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, "chunk.bin.partial", entries[0].Name())

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ch:
			t.Fatal("should not have been called")
		}

		err = q.Close()
		require.NoError(t, err)
	})
}

func makeScheduler(t *testing.T, workers ...int) *Scheduler {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	w := 1
	if len(workers) > 0 {
		w = workers[0]
	}

	return NewScheduler(SchedulerOptions{
		Logger:           logger,
		Workers:          makeWorkers(t, logger, w),
		ScheduleInterval: 50 * time.Millisecond,
		StaleTimeout:     500 * time.Millisecond,
	})
}

func makeWorkers(t *testing.T, logger logrus.FieldLogger, n int) []chan Batch {
	t.Helper()

	chans := make([]chan Batch, n)

	for i := range chans {
		w, ch := NewWorker(logger, 1*time.Second)
		chans[i] = ch

		go w.Run()
	}

	return chans
}

func streamExecutor() (chan uint64, TaskExecutor) {
	ch := make(chan uint64)

	return ch, TaskExecutorFunc(func(ctx context.Context, op uint8, keys ...uint64) error {
		for _, key := range keys {
			ch <- key
		}

		return nil
	})
}

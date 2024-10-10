package queue

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	chans := make([]chan Batch, 10)
	for i := range chans {
		chans[i] = make(chan Batch)

		go func(i int) {
			for b := range chans[i] {
				for _, t := range b.Tasks {
					err := t.Execute(b.Ctx)
					if err != nil {
						log.Printf("error executing task: %v", err)
					}
				}
				b.Done()
			}
		}(i)
	}

	s, err := NewScheduler(SchedulerOptions{
		Logger:           logger,
		Workers:          chans,
		ScheduleInterval: 100 * time.Millisecond,
		StaleTimeout:     500 * time.Millisecond,
	})
	require.NoError(t, err)

	s.Start()

	var count int32
	q, err := NewQueue(s, logger, "test", t.TempDir(), func(ctx context.Context, op uint8, keys ...uint64) error {
		atomic.AddInt32(&count, int32(len(keys)))
		return nil
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		inserts := make([]uint64, 1000)
		for j := 0; j < 1000; j++ {
			inserts[j] = uint64(i*1000 + j)
		}

		err := q.Push(1, inserts...)
		require.NoError(t, err)
	}
	time.Sleep(3 * time.Second)

	require.Equal(t, int32(10000), atomic.LoadInt32(&count))

	err = q.Close()
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)
}

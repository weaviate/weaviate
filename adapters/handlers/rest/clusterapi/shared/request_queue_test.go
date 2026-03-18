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

package shared

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func newTestConfig(enabled bool, queueSize, numWorkers int) cluster.RequestQueueConfig {
	dv := configRuntime.NewDynamicValue(enabled)
	return cluster.RequestQueueConfig{
		IsEnabled:                   dv,
		QueueSize:                   queueSize,
		NumWorkers:                  numWorkers,
		QueueShutdownTimeoutSeconds: 5,
	}
}

func TestRequestQueue_DynamicEnablement(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dv := configRuntime.NewDynamicValue(false)
	config := cluster.RequestQueueConfig{
		IsEnabled:  dv,
		QueueSize:  10,
		NumWorkers: 1,
	}

	rq := NewRequestQueue(config, logger,
		func(item int) {},
		func(item int) bool { return false },
		func(item int) {},
	)

	assert.False(t, rq.Enabled(), "should be disabled initially")

	dv.SetValue(true)
	assert.True(t, rq.Enabled(), "should be enabled after dynamic update")

	dv.SetValue(false)
	assert.False(t, rq.Enabled(), "should be disabled after dynamic update")
}

func TestRequestQueue_EnqueueDequeue(t *testing.T) {
	logger, _ := test.NewNullLogger()
	config := newTestConfig(true, 10, 2)

	var processed sync.WaitGroup
	var count atomic.Int64

	processed.Add(3)
	rq := NewRequestQueue(config, logger,
		func(item int) {
			count.Add(1)
			processed.Done()
		},
		func(item int) bool { return false },
		func(item int) {},
	)

	require.NoError(t, rq.Enqueue(1))
	require.NoError(t, rq.Enqueue(2))
	require.NoError(t, rq.Enqueue(3))

	processed.Wait()
	assert.Equal(t, int64(3), count.Load())

	require.NoError(t, rq.Close(context.Background()))
}

func TestRequestQueue_QueueFull(t *testing.T) {
	logger, _ := test.NewNullLogger()
	config := newTestConfig(true, 2, 0) // 0 workers = will use 1

	// Use a blocking handler so items stay in the queue
	block := make(chan struct{})
	rq := NewRequestQueue(config, logger,
		func(item int) { <-block },
		func(item int) bool { return false },
		func(item int) {},
	)

	// The worker will pick up the first item and block.
	// The channel has capacity 2, so items 2 and 3 go into the buffer.
	require.NoError(t, rq.Enqueue(1))
	// Wait a bit for the worker to pick up item 1
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, rq.Enqueue(2))
	require.NoError(t, rq.Enqueue(3))

	err := rq.Enqueue(4)
	assert.ErrorIs(t, err, ErrQueueFull)

	close(block)
	require.NoError(t, rq.Close(context.Background()))
}

func TestRequestQueue_ExpiredItems(t *testing.T) {
	logger, _ := test.NewNullLogger()
	config := newTestConfig(true, 10, 1)

	var expiredCount atomic.Int64
	var handledCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(3)

	rq := NewRequestQueue(config, logger,
		func(item int) {
			handledCount.Add(1)
			wg.Done()
		},
		func(item int) bool { return item < 0 }, // negative items are "expired"
		func(item int) {
			expiredCount.Add(1)
			wg.Done()
		},
	)

	require.NoError(t, rq.Enqueue(1))
	require.NoError(t, rq.Enqueue(-1)) // expired
	require.NoError(t, rq.Enqueue(2))

	wg.Wait()
	assert.Equal(t, int64(2), handledCount.Load())
	assert.Equal(t, int64(1), expiredCount.Load())

	require.NoError(t, rq.Close(context.Background()))
}

func TestRequestQueue_Shutdown(t *testing.T) {
	logger, _ := test.NewNullLogger()
	config := newTestConfig(true, 10, 2)

	var count atomic.Int64
	rq := NewRequestQueue(config, logger,
		func(item int) { count.Add(1) },
		func(item int) bool { return false },
		func(item int) {},
	)

	require.NoError(t, rq.Close(context.Background()))

	err := rq.Enqueue(1)
	assert.ErrorIs(t, err, ErrShutdown)
}

func TestRequestQueue_CloseIdempotent(t *testing.T) {
	logger, _ := test.NewNullLogger()
	config := newTestConfig(true, 10, 1)

	rq := NewRequestQueue(config, logger,
		func(item int) {},
		func(item int) bool { return false },
		func(item int) {},
	)

	require.NoError(t, rq.Close(context.Background()))
	require.NoError(t, rq.Close(context.Background())) // second close is a no-op
}

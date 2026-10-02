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

package db

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

type geoChunkTestTask struct{ id uint64 }

func (t geoChunkTestTask) Op() uint8                     { return 1 }
func (t geoChunkTestTask) Key() uint64                   { return t.id }
func (t geoChunkTestTask) Execute(context.Context) error { return nil }

type geoChunkTestDecoder struct{}

func (geoChunkTestDecoder) DecodeTask(data []byte) (queue.Task, error) {
	return geoChunkTestTask{id: binary.BigEndian.Uint64(data[1:])}, nil
}

// A queue with a custom (small) chunk size bounds how long one batch
// occupies a worker. ASYNC_INDEXING_BATCH_SIZE must not apply to such
// queues: accumulating in DequeueBatch would merge the small chunks right
// back into the oversized batch the chunk size exists to prevent.
func TestAsyncBatchSizeIgnoredForCustomChunkSize(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_BATCH_SIZE", "10000")

	require.Equal(t, 0, asyncBatchSizeFromEnv(geoQueueChunkSize),
		"a geo queue must not accumulate batches")
	require.Equal(t, 10000, asyncBatchSizeFromEnv(0),
		"a regular vector queue keeps the configured accumulate size")
}

// Behavior-level check of the same guarantee: with small chunks and the
// accumulate size disabled (as the constructor does for geo queues), every
// dispatched batch stays chunk-sized; with it enabled (regular vector
// queues), the same chunks are merged up to the accumulate target.
func TestDequeueBatchSmallChunksStayUnmerged(t *testing.T) {
	newQueue := func(t *testing.T) *queue.DiskQueue {
		logger := logrus.New()
		s := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})
		dq, err := queue.NewDiskQueue(queue.DiskQueueOptions{
			ID:          "geo_chunk_test_queue",
			Scheduler:   s,
			Logger:      logger,
			Dir:         t.TempDir(),
			TaskDecoder: geoChunkTestDecoder{},
			// records are 13 bytes on disk: ~20 tasks per chunk
			ChunkSize:    260,
			StaleTimeout: 50 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, dq.Init())
		t.Cleanup(func() { _ = dq.Close(context.Background()) })

		for i := range 100 {
			rec := make([]byte, 9)
			rec[0] = 1
			binary.BigEndian.PutUint64(rec[1:], uint64(i))
			require.NoError(t, dq.Push(rec))
		}
		require.NoError(t, dq.Flush())
		// let the partial tail chunk become stale so DequeueBatch sees it
		time.Sleep(100 * time.Millisecond)
		return dq
	}

	t.Run("custom chunk size, no accumulation", func(t *testing.T) {
		iq := &VectorIndexQueue{DiskQueue: newQueue(t), batchSize: asyncBatchSizeFromEnv(geoQueueChunkSize)}

		var total, batches int
		for {
			b, err := iq.DequeueBatch()
			require.NoError(t, err)
			if b == nil {
				break
			}
			require.LessOrEqual(t, len(b.Tasks), 25,
				"small chunks must be dispatched as-is, not merged")
			total += len(b.Tasks)
			batches++
			b.Done()
		}
		require.Equal(t, 100, total)
		require.Greater(t, batches, 3)
	})

	t.Run("default chunk size queues still accumulate", func(t *testing.T) {
		t.Setenv("ASYNC_INDEXING_BATCH_SIZE", "10000")
		iq := &VectorIndexQueue{DiskQueue: newQueue(t), batchSize: asyncBatchSizeFromEnv(0)}

		b, err := iq.DequeueBatch()
		require.NoError(t, err)
		require.NotNil(t, b)
		require.Equal(t, 100, len(b.Tasks),
			"an uncapped queue with a batch size must merge chunks into one batch")
		b.Done()
	})
}

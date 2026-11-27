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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
)

// oneAtATimeIndex takes the vectors of a batch one after another and stops
// at the first that fails, as HNSW, HFresh and flat do in AddBatch. The
// vector of id bad fails with an error that is not transient: whatever a
// retry cannot mend, an I/O error that is not a full disk among them.
type oneAtATimeIndex struct {
	*noop.Index

	mu      sync.Mutex
	indexed map[uint64]struct{}
	bad     uint64
}

func (i *oneAtATimeIndex) AddBatch(_ context.Context, ids []uint64, _ [][]float32) error {
	for _, id := range ids {
		if id == i.bad {
			return errors.Errorf("vector %d cannot be indexed", id)
		}
		i.mu.Lock()
		i.indexed[id] = struct{}{}
		i.mu.Unlock()
	}
	return nil
}

func (i *oneAtATimeIndex) Add(ctx context.Context, id uint64, vector []float32) error {
	return i.AddBatch(ctx, []uint64{id}, [][]float32{vector})
}

func (i *oneAtATimeIndex) holds(id uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	_, ok := i.indexed[id]
	return ok
}

// One vector that an index refuses for good must cost that vector, not the
// others that came with it. The scheduler compresses the inserts a worker
// gets of a chunk into one task, a single AddBatch (Scheduler.compressTasks).
// When the index returns an error that is not transient for one vector of
// it, the worker discards the task (queue/worker.go, "permanent errors,
// discarding the failed tasks"): every vector that came after the failing
// one in that AddBatch is never given to the index again, the chunk is
// removed, and nothing but a log line says so. Their objects are in the
// store and no vector search finds them.
func TestVectorIndexQueue_PermanentErrorOfOneVectorLosesTheRestOfItsGroup(t *testing.T) {
	const (
		workers = 4
		total   = 200
		bad     = 50
	)
	logger, _ := test.NewNullLogger()
	s := queue.NewScheduler(queue.SchedulerOptions{
		Logger:           logger,
		Workers:          workers,
		ScheduleInterval: 10 * time.Millisecond,
	})
	s.Start()
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	index := &oneAtATimeIndex{Index: noop.NewIndex(), indexed: map[uint64]struct{}{}, bad: bad}
	viq := &VectorIndexQueue{scheduler: s, shard: &Shard{}, vectorIndex: index}
	dq, err := queue.NewDiskQueue(queue.DiskQueueOptions{
		ID:          "vector_index_queue_permanent_error_test",
		Logger:      logger,
		Scheduler:   s,
		Dir:         t.TempDir(),
		TaskDecoder: &vectorIndexQueueDecoder{q: viq},
	})
	require.NoError(t, err)
	require.NoError(t, dq.Init())
	t.Cleanup(func() { _ = dq.Close(context.Background()) })
	viq.DiskQueue = dq
	s.RegisterQueue(viq)

	// The inserts of one import, in one chunk.
	var buf []byte
	for id := range uint64(total) {
		buf, err = encodeVector(buf[:0], &common.Vector[[]float32]{ID: id, Vector: []float32{1, 2, 3, 4}})
		require.NoError(t, err)
		require.NoError(t, dq.Push(buf))
	}
	require.NoError(t, dq.Flush())

	// The queue goes through the chunk and removes it: nothing is left to
	// hand over again.
	require.Eventually(t, func() bool { return dq.Size() == 0 }, 30*time.Second, 20*time.Millisecond,
		"the queue went through its chunk")
	require.NoError(t, dq.Wait(t.Context()))

	var missing []uint64
	for id := range uint64(total) {
		if id != bad && !index.holds(id) {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	assert.Emptyf(t, missing, "%d vectors that are not the one the index refused were never indexed, and the queue "+
		"holds nothing of them any more: %v", len(missing), missing)
	assert.False(t, index.holds(bad), "the vector the index refused is not indexed")
}

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

package db

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/storagestate"
)

const (
	vectorIndexQueueInsertOp uint8 = iota + 1
	vectorIndexQueueDeleteOp
)

type VectorIndexQueue struct {
	*queue.Queue

	shard     *Shard
	scheduler *queue.Scheduler
	metrics   *IndexQueueMetrics
	// tracks the dimensions of the vectors in the queue
	dims atomic.Int32
	// indicate if the index is being upgraded
	upgrading atomic.Bool

	// prevents replacing the index while
	// the queue is dequeuing vectors
	index struct {
		sync.RWMutex

		i VectorIndex
	}
}

func NewVectorIndexQueue(
	s *queue.Scheduler,
	shard *Shard,
	targetVector string,
	index VectorIndex,
) (*VectorIndexQueue, error) {
	viq := VectorIndexQueue{
		scheduler: s,
		shard:     shard,
	}
	viq.index.i = index

	q, err := queue.New(
		s,
		shard.index.logger,
		fmt.Sprintf("vector_index_queue_%s", shard.vectorIndexID(targetVector)),
		filepath.Join(shard.path(), fmt.Sprintf("%s.queue.d", shard.vectorIndexID(targetVector))),
		&viq,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create vector index queue")
	}
	q.BeforeScheduleFn = viq.beforeScheduleHook

	q.Logger = q.Logger.
		WithField("component", "vector_index_queue").
		WithField("shard_id", shard.ID()).
		WithField("target_vector", targetVector)

	viq.Queue = q
	viq.metrics = NewIndexQueueMetrics(q.Logger, shard.promMetrics, shard.index.Config.ClassName.String(), shard.Name(), targetVector)

	return &viq, nil
}

func (iq *VectorIndexQueue) Insert(vectors ...common.VectorRecord) error {
	iq.index.RLock()
	defer iq.index.RUnlock()

	ids := make([]uint64, len(vectors))
	for i, v := range vectors {
		// ensure the vector is not empty
		if len(v.Vector) == 0 {
			return errors.Errorf("vector is empty")
		}

		// delegate the validation to the index
		err := iq.index.i.ValidateBeforeInsert(v.Vector)
		if err != nil {
			return errors.WithStack(err)
		}

		// if the index is still empty, ensure the first batch is consistent
		// by keeping track of the dimensions of the vectors.
		if iq.dims.CompareAndSwap(0, int32(len(v.Vector))) {
			continue
		}
		if iq.dims.Load() != int32(len(v.Vector)) {
			return errors.Errorf("inconsistent vector lengths: %d != %d", len(vectors[i].Vector), iq.dims.Load())
		}

		// since the queue only stores the vector id on disk,
		// we need to preload the vector index cache to avoid
		// loading the vector from disk when it is indexed.
		iq.index.i.PreloadCache(v.ID, v.Vector)

		ids[i] = v.ID
	}

	return iq.Queue.Push(vectorIndexQueueInsertOp, ids...)
}

func (iq *VectorIndexQueue) Delete(ids ...uint64) error {
	return iq.Queue.Push(vectorIndexQueueDeleteOp, ids...)
}

func (iq *VectorIndexQueue) Execute(ctx context.Context, op uint8, ids ...uint64) error {
	iq.index.RLock()
	defer iq.index.RUnlock()

	switch op {
	case vectorIndexQueueInsertOp:
		return iq.index.i.AddBatchFromDisk(ctx, ids)
	case vectorIndexQueueDeleteOp:
		return iq.index.i.Delete(ids...)
	}

	// TODO: deal with errors
	return errors.Errorf("unknown operation: %d", op)
}

func (iq *VectorIndexQueue) beforeScheduleHook() (skip bool) {
	skip = iq.updateShardStatus()
	if skip {
		return true
	}

	iq.checkCompressionSettings()

	return false
}

// updates the shard status to 'indexing' if the queue is not empty
// and the status is 'ready' otherwise.
func (iq *VectorIndexQueue) updateShardStatus() bool {
	iq.index.RLock()
	defer iq.index.RUnlock()

	if iq.Size() == 0 {
		_, _ = iq.shard.compareAndSwapStatus(storagestate.StatusIndexing.String(), storagestate.StatusReady.String())
		return false /* do not skip, let the scheduler decide what to do */
	}

	status, err := iq.shard.compareAndSwapStatus(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
	if status != storagestate.StatusIndexing || err != nil {
		iq.Logger.WithField("status", status).WithError(err).Warn("failed to set shard status to 'indexing', trying again in ", iq.scheduler.ScheduleInterval)
		return true
	}

	return false
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
}

// triggers compression if the index is ready to be upgraded
func (iq *VectorIndexQueue) checkCompressionSettings() {
	iq.index.RLock()
	defer iq.index.RUnlock()

	ci, ok := iq.index.i.(upgradableIndexer)
	if !ok {
		return
	}

	shouldUpgrade, shouldUpgradeAt := ci.ShouldUpgrade()
	if !shouldUpgrade || ci.Upgraded() || iq.upgrading.Load() {
		return
	}

	if iq.index.i.AlreadyIndexed() > uint64(shouldUpgradeAt) {
		iq.upgrading.Store(true)
		iq.scheduler.PauseQueue(iq.Queue.ID())

		err := ci.Upgrade(func() {
			iq.scheduler.ResumeQueue(iq.Queue.ID())
			iq.upgrading.Store(false)
		})
		if err != nil {
			iq.Queue.Logger.WithError(err).Error("failed to upgrade vector index")
		}

		return
	}
}

func (iq *VectorIndexQueue) ResetWith(vidx VectorIndex) {
	iq.index.Lock()
	iq.index.i = vidx
	iq.index.Unlock()
}

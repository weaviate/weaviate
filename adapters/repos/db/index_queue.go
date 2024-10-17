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
	*queue.DiskQueue

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

	logger := shard.index.logger.WithField("component", "vector_index_queue").
		WithField("shard_id", shard.ID()).
		WithField("target_vector", targetVector)

	q, err := queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:          fmt.Sprintf("vector_index_queue_%s", shard.vectorIndexID(targetVector)),
			Logger:      logger,
			Scheduler:   s,
			Dir:         filepath.Join(shard.path(), fmt.Sprintf("%s.queue.d", shard.vectorIndexID(targetVector))),
			TaskDecoder: &vectorIndexQueueDecoder{idx: index},
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create vector index queue")
	}

	viq.DiskQueue = q
	viq.metrics = NewIndexQueueMetrics(q.Logger, shard.promMetrics, shard.index.Config.ClassName.String(), shard.Name(), targetVector)

	s.RegisterQueue(&viq)

	return &viq, nil
}

func (iq *VectorIndexQueue) Insert(vectors ...common.VectorRecord) error {
	iq.index.RLock()
	defer iq.index.RUnlock()

	r := queue.NewRecord()
	defer r.Release()

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
		if !iq.dims.CompareAndSwap(0, int32(len(v.Vector))) {
			if iq.dims.Load() != int32(len(v.Vector)) {
				return errors.Errorf("inconsistent vector lengths: %d != %d", len(vectors[i].Vector), iq.dims.Load())
			}
		}

		r.Reset()

		// write the operation first
		r.EncodeInt8(int8(vectorIndexQueueInsertOp))
		// write the id
		r.EncodeUint(v.ID)
		// write the vector
		r.EncodeArrayLen(len(v.Vector))
		for _, vv := range v.Vector {
			r.EncodeFloat32(vv)
		}

		err = iq.DiskQueue.Push(r)
		if err != nil {
			return errors.Wrap(err, "failed to push record to queue")
		}
	}

	return iq.DiskQueue.Flush()
}

func (iq *VectorIndexQueue) Delete(ids ...uint64) error {
	r := queue.NewRecord()
	defer r.Release()

	for _, id := range ids {
		r.Reset()
		// write the operation
		r.EncodeInt8(int8(vectorIndexQueueDeleteOp))
		// write the id
		r.EncodeUint(id)
	}

	err := iq.DiskQueue.Push(r)
	if err != nil {
		return errors.Wrap(err, "failed to push record to queue")
	}

	return iq.DiskQueue.Flush()
}

func (iq *VectorIndexQueue) BeforeSchedule() (skip bool) {
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
		iq.scheduler.PauseQueue(iq.DiskQueue.ID())

		err := ci.Upgrade(func() {
			iq.scheduler.ResumeQueue(iq.DiskQueue.ID())
			iq.upgrading.Store(false)
		})
		if err != nil {
			iq.DiskQueue.Logger.WithError(err).Error("failed to upgrade vector index")
		}

		return
	}
}

func (iq *VectorIndexQueue) ResetWith(vidx VectorIndex) {
	iq.index.Lock()
	iq.index.i = vidx
	iq.index.Unlock()
}

type vectorIndexQueueDecoder struct {
	idx VectorIndex
}

func (v *vectorIndexQueueDecoder) DecodeTask(dec *queue.Decoder) (queue.Task, error) {
	op, err := dec.DecodeUint8()
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode operation")
	}

	switch op {
	case vectorIndexQueueInsertOp:
		id, err := dec.DecodeUint()
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode id")
		}

		vec, err := dec.DecodeFloat32Array()
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode vector")
		}

		return &Task{
			op:     op,
			id:     uint64(id),
			vector: vec,
			idx:    v.idx,
		}, nil

	case vectorIndexQueueDeleteOp:
		id, err := dec.DecodeUint()
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode id")
		}

		return &Task{
			op:  op,
			id:  uint64(id),
			idx: v.idx,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type Task struct {
	op     uint8
	id     uint64
	vector []float32
	idx    VectorIndex
}

func (t *Task) Op() uint8 {
	return t.op
}

func (t *Task) Key() uint64 {
	return t.id
}

func (t *Task) Execute(ctx context.Context) error {
	switch t.op {
	case vectorIndexQueueInsertOp:
		return t.idx.Add(t.id, t.vector)
	case vectorIndexQueueDeleteOp:
		return t.idx.Delete(t.id)
	}

	return errors.Errorf("unknown operation: %d", t.Op)
}

func (t *Task) NewGroup(op uint8, tasks ...*Task) queue.Task {
	ids := make([]uint64, len(tasks))
	vectors := make([][]float32, len(tasks))

	for i, task := range tasks {
		ids[i] = task.id
		vectors[i] = task.vector
	}

	return &TaskGroup{
		op:      op,
		ids:     ids,
		vectors: vectors,
		idx:     t.idx,
	}
}

type TaskGroup struct {
	op      uint8
	ids     []uint64
	vectors [][]float32
	idx     VectorIndex
}

func (t *TaskGroup) Op() uint8 {
	return t.op
}

func (t *TaskGroup) Key() uint64 {
	return t.ids[0]
}

func (tg *TaskGroup) Execute(ctx context.Context) error {
	switch tg.op {
	case vectorIndexQueueInsertOp:
		return tg.idx.AddBatch(ctx, tg.ids, tg.vectors)
	case vectorIndexQueueDeleteOp:
		return tg.idx.Delete(tg.ids...)
	}

	return errors.Errorf("unknown operation: %d", tg.Op)
}

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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

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

	asyncEnabled bool
	shard        *Shard
	scheduler    *queue.Scheduler
	metrics      *IndexQueueMetrics
	// tracks the dimensions of the vectors in the queue
	dims atomic.Int32
	// indicate if the index is being upgraded
	upgrading atomic.Bool

	vectorIndex VectorIndex
}

func NewVectorIndexQueue(
	shard *Shard,
	targetVector string,
	index VectorIndex,
) (*VectorIndexQueue, error) {
	viq := VectorIndexQueue{
		shard:        shard,
		scheduler:    shard.scheduler,
		asyncEnabled: asyncEnabled(),
	}
	viq.vectorIndex = index

	logger := shard.index.logger.WithField("component", "vector_index_queue").
		WithField("shard_id", shard.ID()).
		WithField("target_vector", targetVector)

	staleTimeout, _ := time.ParseDuration(os.Getenv("ASYNC_INDEXING_STALE_TIMEOUT"))

	q, err := queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:        fmt.Sprintf("vector_index_queue_%s_%s", shard.ID(), shard.vectorIndexID(targetVector)),
			Logger:    logger,
			Scheduler: shard.scheduler,
			Dir:       filepath.Join(shard.path(), fmt.Sprintf("%s.queue.d", shard.vectorIndexID(targetVector))),
			TaskDecoder: &vectorIndexQueueDecoder{
				q: &viq,
			},
			OnBatchProcessed: viq.OnBatchProcessed,
			StaleTimeout:     staleTimeout,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create vector index queue")
	}

	viq.DiskQueue = q
	viq.metrics = NewIndexQueueMetrics(q.Logger, shard.promMetrics, shard.index.Config.ClassName.String(), shard.Name(), targetVector)

	shard.scheduler.RegisterQueue(&viq)

	return &viq, nil
}

func (iq *VectorIndexQueue) Close() error {
	if iq == nil {
		// the queue is nil when the shard is not fully initialized
		return nil
	}

	return iq.DiskQueue.Close()
}

func (iq *VectorIndexQueue) Insert(ctx context.Context, vectors ...common.VectorRecord) error {
	if !iq.asyncEnabled {
		ids := make([]uint64, len(vectors))
		vecs := make([][]float32, len(vectors))
		for i, v := range vectors {
			ids[i] = v.ID
			vecs[i] = v.Vector
		}

		return iq.vectorIndex.AddBatch(ctx, ids, vecs)
	}

	// ensure the shard is in the right state
	status, err := iq.shard.compareAndSwapStatusIndexingAndReady(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
	if status != storagestate.StatusIndexing || err != nil {
		iq.Logger.WithField("status", status).WithError(err).Warn("failed to set shard status to 'indexing', trying again in ", iq.scheduler.ScheduleInterval)
		return errors.Errorf("failed to set shard status to 'indexing'")
	}

	var buf []byte

	for i, v := range vectors {
		// ensure the vector is not empty
		if len(v.Vector) == 0 {
			return errors.Errorf("vector is empty")
		}

		// delegate the validation to the index
		err := iq.vectorIndex.ValidateBeforeInsert(v.Vector)
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

		buf = buf[:0]

		// write the operation first
		buf = append(buf, vectorIndexQueueInsertOp)
		// write the id
		buf = binary.BigEndian.AppendUint64(buf, v.ID)
		// write the vector
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v.Vector)))
		for _, v := range v.Vector {
			buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(v))
		}

		err = iq.DiskQueue.Push(buf)
		if err != nil {
			return errors.Wrap(err, "failed to push record to queue")
		}
	}

	return nil
}

func (iq *VectorIndexQueue) Delete(ids ...uint64) error {
	if !iq.asyncEnabled {
		return iq.vectorIndex.Delete(ids...)
	}

	var buf []byte

	for _, id := range ids {
		buf = buf[:0]
		// write the operation
		buf = append(buf, vectorIndexQueueDeleteOp)
		// write the id
		buf = binary.BigEndian.AppendUint64(buf, id)

		err := iq.DiskQueue.Push(buf)
		if err != nil {
			return errors.Wrap(err, "failed to push record to queue")
		}
	}

	return nil
}

func (iq *VectorIndexQueue) Flush() error {
	if iq == nil {
		// the queue is nil when the shard is not fully initialized
		return nil
	}

	if !iq.asyncEnabled {
		return iq.vectorIndex.Flush()
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
	if iq.Size() == 0 {
		_, _ = iq.shard.compareAndSwapStatusIndexingAndReady(storagestate.StatusIndexing.String(), storagestate.StatusReady.String())
		return false /* do not skip, let the scheduler decide what to do */
	}

	status, err := iq.shard.compareAndSwapStatusIndexingAndReady(storagestate.StatusReady.String(), storagestate.StatusIndexing.String())
	if status != storagestate.StatusIndexing || err != nil {
		iq.Logger.WithField("status", status).WithError(err).Warn("failed to set shard status to 'indexing', trying again in ", iq.scheduler.ScheduleInterval)
		return true
	}

	return false
}

// Flush the vector index after a batch is processed.
func (iq *VectorIndexQueue) OnBatchProcessed() {
	if err := iq.vectorIndex.Flush(); err != nil {
		iq.Logger.WithError(err).Error("failed to flush vector index")
	}
}

type upgradableIndexer interface {
	Upgraded() bool
	Upgrade(callback func()) error
	ShouldUpgrade() (bool, int)
}

// triggers compression if the index is ready to be upgraded
func (iq *VectorIndexQueue) checkCompressionSettings() {
	ci, ok := iq.vectorIndex.(upgradableIndexer)
	if !ok {
		return
	}

	shouldUpgrade, shouldUpgradeAt := ci.ShouldUpgrade()
	if !shouldUpgrade || ci.Upgraded() || iq.upgrading.Load() {
		return
	}

	if iq.vectorIndex.AlreadyIndexed() > uint64(shouldUpgradeAt) {
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

// ResetWith resets the queue with the given vector index.
// The queue must be paused before calling this method.
func (iq *VectorIndexQueue) ResetWith(vidx VectorIndex) {
	iq.vectorIndex = vidx
}

type vectorIndexQueueDecoder struct {
	q *VectorIndexQueue
}

func (v *vectorIndexQueueDecoder) DecodeTask(data []byte) (queue.Task, error) {
	op := data[0]
	data = data[1:]

	switch op {
	case vectorIndexQueueInsertOp:
		// decode id
		id := binary.BigEndian.Uint64(data)
		data = data[8:]

		// decode array size on 2 bytes
		alen := binary.BigEndian.Uint16(data)
		data = data[2:]

		// decode vector
		vec := make([]float32, alen)
		for i := 0; i < int(alen); i++ {
			bits := binary.BigEndian.Uint32(data)
			vec[i] = math.Float32frombits(bits)
			data = data[4:]
		}

		return &Task{
			op:     op,
			id:     uint64(id),
			vector: vec,
			idx:    v.q.vectorIndex,
		}, nil
	case vectorIndexQueueDeleteOp:
		// decode id
		id := binary.BigEndian.Uint64(data)

		return &Task{
			op:  op,
			id:  uint64(id),
			idx: v.q.vectorIndex,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

var _ queue.TaskGrouper = &Task{}

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
		return t.idx.Add(ctx, t.id, t.vector)
	case vectorIndexQueueDeleteOp:
		return t.idx.Delete(t.id)
	}

	return errors.Errorf("unknown operation: %d", t.Op())
}

func (t *Task) NewGroup(op uint8, tasks ...queue.Task) queue.Task {
	ids := make([]uint64, len(tasks))
	vectors := make([][]float32, len(tasks))

	for i, task := range tasks {
		t := task.(*Task)
		ids[i] = t.id
		vectors[i] = t.vector
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

	return errors.Errorf("unknown operation: %d", tg.Op())
}

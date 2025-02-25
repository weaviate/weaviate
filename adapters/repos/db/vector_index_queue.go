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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/dto"
)

const (
	vectorIndexQueueInsertOp uint8 = iota + 1
	vectorIndexQueueDeleteOp
	vectorIndexQueueMultiInsertOp
	vectorIndexQueueMultiDeleteOp
)

type VectorIndexQueue struct {
	*queue.DiskQueue

	asyncEnabled bool
	shard        *Shard
	scheduler    *queue.Scheduler
	metrics      *VectorIndexQueueMetrics

	// tracks the dimensions of the vectors in the queue
	dims atomic.Int32
	// If positive, accumulates vectors in a batch before indexing them.
	// Otherwise, the batch size is determined by the size of a chunk file
	// (typically 10MB worth of vectors).
	// Batch size is not guaranteed to match this value exactly.
	batchSize int

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
	batchSize, _ := strconv.Atoi(os.Getenv("ASYNC_INDEXING_BATCH_SIZE"))
	if batchSize > 0 {
		viq.batchSize = batchSize
	}

	viq.metrics = NewVectorIndexQueueMetrics(logger, shard.promMetrics, shard.index.Config.ClassName.String(), shard.Name(), targetVector)

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
			Metrics:          viq.metrics.QueueMetrics(),
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create vector index queue")
	}
	viq.DiskQueue = q

	if viq.asyncEnabled {
		err = q.Init()
		if err != nil {
			return nil, errors.Wrap(err, "failed to initialize vector index queue")
		}

		shard.scheduler.RegisterQueue(&viq)
	}

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
		return common.AddVectorsToIndex(ctx, vectors, iq.vectorIndex)
	}

	start := time.Now()
	defer iq.metrics.Insert(start, len(vectors))

	var buf []byte
	var err error

	for _, v := range vectors {
		// validate vector
		if err := v.Validate(iq.vectorIndex); err != nil {
			return errors.Wrap(err, "failed to validate")
		}

		// if the index is still empty, ensure the first batch is consistent
		// by keeping track of the dimensions of the vectors.
		if !iq.dims.CompareAndSwap(0, int32(v.Len())) {
			if iq.dims.Load() != int32(v.Len()) {
				return errors.Errorf("inconsistent vector lengths: %d != %d", v.Len(), iq.dims.Load())
			}
		}

		// encode vector
		buf = buf[:0]
		buf, err = encodeVector(buf, v)
		if err != nil {
			return errors.Wrap(err, "failed to encode record")
		}

		err = iq.DiskQueue.Push(buf)
		if err != nil {
			return errors.Wrap(err, "failed to push record to queue")
		}
	}

	return nil
}

// DequeueBatch dequeues a batch of tasks from the queue.
// If the queue is configured to accumulate vectors in a batch, it will dequeue
// tasks until the target batch size is reached.
// Otherwise, dequeues a single chunk file worth of tasks.
func (iq *VectorIndexQueue) DequeueBatch() (*queue.Batch, error) {
	if iq.batchSize <= 0 {
		return iq.DiskQueue.DequeueBatch()
	}

	var batches []*queue.Batch
	var taskCount int

	for {
		batch, err := iq.DiskQueue.DequeueBatch()
		if err != nil {
			return nil, err
		}

		if batch == nil {
			break
		}

		batches = append(batches, batch)

		taskCount += len(batch.Tasks)
		if taskCount >= iq.batchSize {
			break
		}
	}

	if len(batches) == 0 {
		return nil, nil
	}

	return queue.MergeBatches(batches...), nil
}

func (iq *VectorIndexQueue) Delete(ids ...uint64) error {
	if !iq.asyncEnabled {
		return iq.vectorIndex.Delete(ids...)
	}

	if iq.vectorIndex.Multivector() {
		return iq.delete(vectorIndexQueueMultiDeleteOp, ids...)
	}
	return iq.delete(vectorIndexQueueDeleteOp, ids...)
}

func (iq *VectorIndexQueue) delete(deleteOperation uint8, ids ...uint64) error {
	start := time.Now()
	defer iq.metrics.Delete(start, len(ids))

	var buf []byte

	for _, id := range ids {
		buf = buf[:0]
		// write the operation
		buf = append(buf, deleteOperation)
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
	return iq.checkCompressionSettings()
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
func (iq *VectorIndexQueue) checkCompressionSettings() (skip bool) {
	ci, ok := iq.vectorIndex.(upgradableIndexer)
	if !ok {
		return false
	}

	shouldUpgrade, shouldUpgradeAt := ci.ShouldUpgrade()
	if !shouldUpgrade || ci.Upgraded() {
		return false
	}

	if iq.vectorIndex.AlreadyIndexed() > uint64(shouldUpgradeAt) {
		iq.scheduler.PauseQueue(iq.DiskQueue.ID())

		err := ci.Upgrade(func() {
			iq.scheduler.ResumeQueue(iq.DiskQueue.ID())
		})
		if err != nil {
			iq.DiskQueue.Logger.WithError(err).Error("failed to upgrade vector index")
		}

		return true
	}

	return false
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

		return &Task[[]float32]{
			op:     op,
			id:     uint64(id),
			vector: vec,
			idx:    v.q.vectorIndex,
		}, nil
	case vectorIndexQueueDeleteOp:
		// decode id
		id := binary.BigEndian.Uint64(data)

		return &Task[[]float32]{
			op:  op,
			id:  uint64(id),
			idx: v.q.vectorIndex,
		}, nil
	case vectorIndexQueueMultiInsertOp:
		// decode id
		id := binary.BigEndian.Uint64(data)
		data = data[8:]

		// decode array size on 2 bytes
		alen := binary.BigEndian.Uint16(data)
		data = data[2:]

		// decode vector
		multiVec := make([][]float32, alen)
		for i := 0; i < int(alen); i++ {
			alenvec := binary.BigEndian.Uint16(data)
			data = data[2:]
			vec := make([]float32, int(alenvec))
			for j := 0; j < int(alenvec); j++ {
				bits := binary.BigEndian.Uint32(data)
				vec[j] = math.Float32frombits(bits)
				data = data[4:]
			}
			multiVec[i] = vec
		}

		return &Task[[][]float32]{
			op:     op,
			id:     uint64(id),
			vector: multiVec,
			idx:    v.q.vectorIndex,
		}, nil
	case vectorIndexQueueMultiDeleteOp:
		// decode id
		id := binary.BigEndian.Uint64(data)

		return &Task[[][]float32]{
			op:  op,
			id:  uint64(id),
			idx: v.q.vectorIndex,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type Task[T dto.Embedding] struct {
	op     uint8
	id     uint64
	vector T
	idx    VectorIndex
}

func (t *Task[T]) Op() uint8 {
	return t.op
}

func (t *Task[T]) Key() uint64 {
	return t.id
}

func (t *Task[T]) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	switch t.op {
	case vectorIndexQueueInsertOp:
		return t.idx.Add(ctx, t.id, any(t.vector).([]float32))
	case vectorIndexQueueMultiInsertOp:
		return t.idx.AddMulti(ctx, t.id, any(t.vector).([][]float32))
	case vectorIndexQueueDeleteOp, vectorIndexQueueMultiDeleteOp:
		return t.idx.Delete(t.id)
	}

	return errors.Errorf("unknown operation: %d", t.Op())
}

func (t *Task[T]) NewGroup(op uint8, tasks ...queue.Task) queue.Task {
	ids := make([]uint64, len(tasks))
	vectors := make([]T, len(tasks))

	for i, task := range tasks {
		t := task.(*Task[T])
		ids[i] = t.id
		vectors[i] = t.vector
	}

	return &TaskGroup[T]{
		op:      op,
		ids:     ids,
		vectors: vectors,
		idx:     t.idx,
	}
}

type TaskGroup[T dto.Embedding] struct {
	op      uint8
	ids     []uint64
	vectors []T
	idx     VectorIndex
}

func (t *TaskGroup[T]) Op() uint8 {
	return t.op
}

func (t *TaskGroup[T]) Key() uint64 {
	return t.ids[0]
}

func (t *TaskGroup[T]) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	switch t.op {
	case vectorIndexQueueInsertOp:
		return t.idx.AddBatch(ctx, t.ids, any(t.vectors).([][]float32))
	case vectorIndexQueueMultiInsertOp:
		return t.idx.AddMultiBatch(ctx, t.ids, any(t.vectors).([][][]float32))
	case vectorIndexQueueDeleteOp, vectorIndexQueueMultiDeleteOp:
		return t.idx.Delete(t.ids...)
	}

	return errors.Errorf("unknown operation: %d", t.Op())
}

func encodeVector(buf []byte, vectorRec common.VectorRecord) ([]byte, error) {
	switch v := vectorRec.(type) {
	case *common.Vector[[]float32]:
		// write the operation first
		buf = append(buf, vectorIndexQueueInsertOp)
		// put multi or normal vector operation header!
		buf = binary.BigEndian.AppendUint64(buf, v.ID)
		// write the vector
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v.Vector)))
		for _, v := range v.Vector {
			buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(v))
		}
		return buf, nil
	case *common.Vector[[][]float32]:
		// write the operation first
		buf = append(buf, vectorIndexQueueMultiInsertOp)
		// put multi or normal vector operation header!
		buf = binary.BigEndian.AppendUint64(buf, v.ID)
		// write the vector
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v.Vector)))
		for _, v := range v.Vector {
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(v)))
			for _, v := range v {
				buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(v))
			}
		}
		return buf, nil
	default:
		return nil, errors.Errorf("unrecognized vector type: %T", vectorRec)
	}
}

// compile time check for TaskGrouper interface
var (
	_ = queue.TaskGrouper(new(Task[[]float32]))
	_ = queue.TaskGrouper(new(Task[[][]float32]))
)

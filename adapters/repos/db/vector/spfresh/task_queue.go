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

package spfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

const (
	taskQueueSplitOp uint8 = iota + 1
	taskQueueMergeOp
	taskQueueReassignOp
)

type TaskQueue struct {
	*queue.DiskQueue

	scheduler *queue.Scheduler

	spfreshIndex *SPFresh
	splitList    *deduplicator // Prevents duplicate split operations
	mergeList    *deduplicator // Prevents duplicate merge operations
}

func NewTaskQueue(
	spfreshIndex *SPFresh,
	targetVector string,
) (*TaskQueue, error) {
	opq := TaskQueue{
		spfreshIndex: spfreshIndex,
		scheduler:    spfreshIndex.scheduler,
		splitList:    newDeduplicator(),
		mergeList:    newDeduplicator(),
	}

	q, err := queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:        fmt.Sprintf("spfresh_ops_queue_%s_%s", spfreshIndex.config.ShardName, spfreshIndex.config.ID),
			Logger:    spfreshIndex.logger,
			Scheduler: spfreshIndex.scheduler,
			Dir:       filepath.Join(spfreshIndex.config.RootPath, fmt.Sprintf("%s.queue.d", spfreshIndex.config.ID)),
			TaskDecoder: &TaskQueueDecoder{
				q: &opq,
			},
			OnBatchProcessed: opq.OnBatchProcessed,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create vector index queue")
	}
	opq.DiskQueue = q

	err = q.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize vector index queue")
	}

	spfreshIndex.scheduler.RegisterQueue(&opq)

	return &opq, nil
}

func (opq *TaskQueue) SplitDone(postingID uint64) {
	opq.splitList.done(postingID)
}

func (opq *TaskQueue) MergeDone(postingID uint64) {
	opq.mergeList.done(postingID)
}

func (opq *TaskQueue) MergeContains(postingID uint64) bool {
	return opq.mergeList.contains(postingID)
}

func (opq *TaskQueue) EnqueueSplit(ctx context.Context, postingID uint64) error {
	if opq.spfreshIndex.ctx == nil {
		return nil // Not started yet
	}

	if err := opq.spfreshIndex.ctx.Err(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Check if the task is already in progress
	if !opq.splitList.tryAdd(postingID) {
		return nil
	}
	var buf []byte
	var err error

	// encode split
	buf = buf[:0]
	buf, err = encodeTask(buf, postingID, taskQueueSplitOp)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	if err := opq.Push(buf); err != nil {
		return errors.Wrap(err, "failed to push split operation to queue")
	}

	opq.spfreshIndex.metrics.EnqueueSplitTask()

	return nil
}

func (opq *TaskQueue) EnqueueMerge(ctx context.Context, postingID uint64) error {
	if opq.spfreshIndex.ctx == nil {
		return nil // Not started yet
	}

	if err := opq.spfreshIndex.ctx.Err(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Check if the operation is already in progress
	if !opq.mergeList.tryAdd(postingID) {
		return nil
	}

	var buf []byte
	var err error

	// encode split
	buf = buf[:0]
	buf, err = encodeTask(buf, postingID, taskQueueMergeOp)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	if err := opq.Push(buf); err != nil {
		return errors.Wrap(err, "failed to push merge operation to queue")
	}

	opq.spfreshIndex.metrics.EnqueueMergeTask()

	return nil
}

func (opq *TaskQueue) EnqueueReassign(ctx context.Context, postingID uint64, vecID uint64, version VectorVersion) error {
	if opq.spfreshIndex.ctx == nil {
		return nil // Not started yet
	}

	if err := opq.spfreshIndex.ctx.Err(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	buf := make([]byte, 18)
	buf[0] = taskQueueReassignOp
	binary.LittleEndian.PutUint64(buf[1:9], postingID)
	binary.LittleEndian.PutUint64(buf[9:17], vecID)
	buf[17] = byte(version)

	if err := opq.Push(buf); err != nil {
		return errors.Wrap(err, "failed to push reassign operation to queue")
	}

	opq.spfreshIndex.metrics.EnqueueReassignTask()

	return nil
}

// Flush the vector index after a batch is processed.
func (opq *TaskQueue) OnBatchProcessed() {
	if err := opq.spfreshIndex.Flush(); err != nil {
		opq.Logger.WithError(err).Error("failed to flush vector index")
	}
}

type TaskQueueDecoder struct {
	q *TaskQueue
}

func (v *TaskQueueDecoder) DecodeTask(data []byte) (queue.Task, error) {
	op := data[0]
	data = data[1:]

	switch op {
	case taskQueueSplitOp:
		// decode posting ID
		postingID := binary.LittleEndian.Uint64(data)

		return &SplitTask{
			id:  postingID,
			idx: v.q.spfreshIndex,
		}, nil
	case taskQueueMergeOp:
		// decode posting ID
		postingID := binary.LittleEndian.Uint64(data)

		return &MergeTask{
			id:  postingID,
			idx: v.q.spfreshIndex,
		}, nil
	case taskQueueReassignOp:
		// decode posting ID
		postingID := binary.LittleEndian.Uint64(data)
		data = data[8:]
		// decode vector ID
		vecID := binary.LittleEndian.Uint64(data)
		data = data[8:]
		// decode version
		version := uint8(data[0])
		return &ReassignTask{
			id:      postingID,
			vecID:   vecID,
			version: version,
			idx:     v.q.spfreshIndex,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type SplitTask struct {
	id  uint64
	idx *SPFresh
}

func (t *SplitTask) Op() uint8 {
	return taskQueueSplitOp
}

func (t *SplitTask) Key() uint64 {
	// postings sharing same lock are run by the same worker to reduce contention
	return t.idx.postingLocks.Hash(t.id)
}

func (t *SplitTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	t.idx.metrics.DequeueSplitTask()
	return t.idx.doSplit(t.id, true)
}

type MergeTask struct {
	id  uint64
	idx *SPFresh
}

func (t *MergeTask) Op() uint8 {
	return taskQueueMergeOp
}

func (t *MergeTask) Key() uint64 {
	// merge must be run sequentially, so we use a fixed key
	return uint64(taskQueueMergeOp)
}

func (t *MergeTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	t.idx.metrics.DequeueMergeTask()
	return t.idx.doMerge(t.id)
}

type ReassignTask struct {
	id      uint64
	vecID   uint64
	version uint8
	idx     *SPFresh
}

func (t *ReassignTask) Op() uint8 {
	return taskQueueReassignOp
}

func (t *ReassignTask) Key() uint64 {
	// postings sharing same lock are run by the same worker to reduce contention
	return t.idx.postingLocks.Hash(t.id)
}

func (t *ReassignTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	t.idx.metrics.DequeueReassignTask()
	return t.idx.doReassign(reassignOperation{PostingID: t.id, VectorID: t.vecID, Version: t.version})
}

func encodeTask(buf []byte, id uint64, op uint8) ([]byte, error) {
	switch op {
	case taskQueueSplitOp, taskQueueMergeOp:
		// write the operation first
		buf = append(buf, op)
		buf = binary.LittleEndian.AppendUint64(buf, id)
		return buf, nil
	default:
		return nil, errors.Errorf("unrecognized operation: %d", op)
	}
}

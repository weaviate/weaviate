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
	stderrors "errors"
	"fmt"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

const (
	taskQueueSplitOp uint8 = iota + 1
	taskQueueMergeOp
	taskQueueReassignOp
)

type TaskQueue struct {
	// queue for split and reassign operations
	q *queue.DiskQueue
	// queue for merge operations, as these need to be run sequentially
	qMerge *queue.DiskQueue

	scheduler *queue.Scheduler

	index     *SPFresh
	splitList *deduplicator // Prevents duplicate split operations
	mergeList *deduplicator // Prevents duplicate merge operations
}

func NewTaskQueue(index *SPFresh) (*TaskQueue, error) {
	var err error

	tq := TaskQueue{
		index:     index,
		scheduler: index.scheduler,
		splitList: newDeduplicator(),
		mergeList: newDeduplicator(),
	}

	// create main queue for split and reassign operations
	tq.q, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("spfresh_ops_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, fmt.Sprintf("%s.queue.d", index.config.ID)),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create spfresh main queue")
	}
	err = tq.q.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize spfresh main queue")
	}

	// create separate queue for merge operations
	tq.qMerge, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("spfresh_merge_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, fmt.Sprintf("%s.merge.queue.d", index.config.ID)),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create spfresh merge queue")
	}
	err = tq.qMerge.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize spfresh merge queue")
	}

	index.scheduler.RegisterQueue(tq.q)
	index.scheduler.RegisterQueue(tq.qMerge)

	return &tq, nil
}

func (tq *TaskQueue) Close() error {
	var errs []error
	if err := tq.q.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close main queue"))
	}

	if err := tq.qMerge.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close merge queue"))
	}

	return stderrors.Join(errs...)
}

func (tq *TaskQueue) SplitDone(postingID uint64) {
	tq.splitList.done(postingID)
}

func (tq *TaskQueue) MergeDone(postingID uint64) {
	tq.mergeList.done(postingID)
}

func (tq *TaskQueue) MergeContains(postingID uint64) bool {
	return tq.mergeList.contains(postingID)
}

func (tq *TaskQueue) EnqueueSplit(postingID uint64) error {
	// Check if the task is already in progress
	if !tq.splitList.tryAdd(postingID) {
		return nil
	}

	if err := tq.q.Push(encodeTask(postingID, taskQueueSplitOp)); err != nil {
		return errors.Wrap(err, "failed to push split operation to queue")
	}

	tq.index.metrics.EnqueueSplitTask()

	return nil
}

func (tq *TaskQueue) EnqueueMerge(postingID uint64) error {
	// Check if the operation is already in progress
	if !tq.mergeList.tryAdd(postingID) {
		return nil
	}

	if err := tq.qMerge.Push(encodeTask(postingID, taskQueueMergeOp)); err != nil {
		return errors.Wrap(err, "failed to push merge operation to queue")
	}

	tq.index.metrics.EnqueueMergeTask()

	return nil
}

func (tq *TaskQueue) EnqueueReassign(postingID uint64, vecID uint64, version VectorVersion) error {
	buf := make([]byte, 18)
	buf[0] = taskQueueReassignOp
	binary.LittleEndian.PutUint64(buf[1:9], postingID)
	binary.LittleEndian.PutUint64(buf[9:17], vecID)
	buf[17] = byte(version)

	if err := tq.q.Push(buf); err != nil {
		return errors.Wrap(err, "failed to push reassign operation to queue")
	}

	tq.index.metrics.EnqueueReassignTask()

	return nil
}

// Flush the vector index after a batch is processed.
func (tq *TaskQueue) OnBatchProcessed() {
	if err := tq.index.Flush(); err != nil {
		tq.index.logger.WithError(err).Error("failed to flush vector index")
	}
}

func (tq *TaskQueue) DecodeTask(data []byte) (queue.Task, error) {
	op := data[0]
	data = data[1:]

	switch op {
	case taskQueueSplitOp:
		// decode posting ID
		postingID := binary.LittleEndian.Uint64(data)

		return &SplitTask{
			id:  postingID,
			idx: tq.index,
		}, nil
	case taskQueueMergeOp:
		// decode posting ID
		postingID := binary.LittleEndian.Uint64(data)

		return &MergeTask{
			id:  postingID,
			idx: tq.index,
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
			idx:     tq.index,
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

	err := t.idx.doSplit(ctx, t.id, true)
	if err != nil {
		return err
	}

	t.idx.metrics.DequeueSplitTask()
	return nil
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
	// by hashing the index ID
	return xxhash.Sum64String(t.idx.id)
}

func (t *MergeTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	err := t.idx.doMerge(ctx, t.id)
	if err != nil {
		return err
	}

	t.idx.metrics.DequeueMergeTask()
	return nil
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

	err := t.idx.doReassign(ctx, reassignOperation{PostingID: t.id, VectorID: t.vecID, Version: t.version})
	if err != nil {
		return err
	}

	t.idx.metrics.DequeueReassignTask()
	return nil
}

func encodeTask(id uint64, op uint8) []byte {
	buf := make([]byte, 9)
	buf[0] = op
	binary.LittleEndian.PutUint64(buf[1:9], id)
	return buf
}

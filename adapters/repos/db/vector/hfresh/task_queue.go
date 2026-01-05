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

package hfresh

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

const (
	taskQueueSplitOp uint8 = iota + 1
	taskQueueMergeOp
	taskQueueReassignOp
)

// limit the size of each task chunk to
// to avoid sending too many tasks at once.
const (
	splitTaskQueueChunkSize    = 16 * 1024 // 16KB
	reassignTaskQueueChunkSize = 16 * 1024 // 16KB
	mergeTaskQueueChunkSize    = 16 * 1024 // 16KB
)

type TaskQueue struct {
	// queue for split operations
	splitQueue *queue.DiskQueue
	// queue for reassign operations
	reassignQueue *queue.DiskQueue
	// queue for merge operations, as these need to be run sequentially
	mergeQueue *queue.DiskQueue

	scheduler *queue.Scheduler

	index        *HFresh
	splitList    *deduplicator         // Prevents duplicate split operations
	mergeList    *deduplicator         // Prevents duplicate merge operations
	reassignList *reassignDeduplicator // Prevents duplicate reassign operations
}

func NewTaskQueue(index *HFresh, bucket *lsmkv.Bucket) (*TaskQueue, error) {
	var err error

	reassignList, err := newReassignDeduplicator(bucket)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create reassign deduplicator")
	}

	tq := TaskQueue{
		index:        index,
		scheduler:    index.scheduler,
		splitList:    newDeduplicator(),
		mergeList:    newDeduplicator(),
		reassignList: reassignList,
	}

	// create queue for split operations
	tq.splitQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("hfresh_split_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "split.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			ChunkSize:        splitTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create hfresh split queue")
	}
	err = tq.splitQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize hfresh split queue")
	}

	// create queue for reassign operations
	tq.reassignQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("hfresh_reassign_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "reassign.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			ChunkSize:        reassignTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create hfresh reassign queue")
	}
	err = tq.reassignQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize hfresh reassign queue")
	}

	// create queue for merge operations
	tq.mergeQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("hfresh_merge_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "merge.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			ChunkSize:        mergeTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create hfresh merge queue")
	}
	err = tq.mergeQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize hfresh merge queue")
	}

	index.scheduler.RegisterQueue(tq.splitQueue)
	index.scheduler.RegisterQueue(tq.reassignQueue)
	index.scheduler.RegisterQueue(tq.mergeQueue)

	return &tq, nil
}

func (tq *TaskQueue) Close() error {
	var errs []error
	if err := tq.Flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush task queue before close"))
	}

	if err := tq.splitQueue.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close split queue"))
	}

	if err := tq.reassignQueue.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close reassign queue"))
	}

	if err := tq.mergeQueue.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close merge queue"))
	}

	return stderrors.Join(errs...)
}

func (tq *TaskQueue) Flush() error {
	var errs []error

	if err := tq.reassignList.flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush reassign list"))
	}

	if err := tq.splitQueue.Flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush split queue"))
	}

	if err := tq.reassignQueue.Flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush reassign queue"))
	}

	if err := tq.mergeQueue.Flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush merge queue"))
	}

	return stderrors.Join(errs...)
}

func (tq *TaskQueue) Size() int64 {
	return tq.splitQueue.Size() + tq.reassignQueue.Size() + tq.mergeQueue.Size()
}

func (tq *TaskQueue) SplitDone(postingID uint64) {
	tq.splitList.done(postingID)
}

func (tq *TaskQueue) MergeDone(postingID uint64) {
	tq.mergeList.done(postingID)
}

func (tq *TaskQueue) ReassignDone(vectorID uint64) {
	tq.reassignList.done(vectorID)
}

func (tq *TaskQueue) MergeContains(postingID uint64) bool {
	return tq.mergeList.contains(postingID)
}

func (tq *TaskQueue) EnqueueSplit(postingID uint64) error {
	// Check if the operation is already enqueued
	if !tq.splitList.tryAdd(postingID) {
		return nil
	}

	if err := tq.splitQueue.Push(encodeTask(postingID, taskQueueSplitOp)); err != nil {
		return errors.Wrap(err, "failed to push split operation to queue")
	}

	tq.index.metrics.EnqueueSplitTask()

	return nil
}

func (tq *TaskQueue) EnqueueMerge(postingID uint64) error {
	// Check if the operation is already enqueued
	if !tq.mergeList.tryAdd(postingID) {
		return nil
	}

	if err := tq.mergeQueue.Push(encodeTask(postingID, taskQueueMergeOp)); err != nil {
		return errors.Wrap(err, "failed to push merge operation to queue")
	}

	tq.index.metrics.EnqueueMergeTask()

	return nil
}

func (tq *TaskQueue) EnqueueReassign(postingID uint64, vecID uint64, version VectorVersion) error {
	// Check if the operation is already enqueued
	if !tq.reassignList.tryAdd(vecID, postingID) {
		return nil
	}

	if err := tq.reassignQueue.Push(encodeTask(vecID, taskQueueReassignOp)); err != nil {
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
		// decode vector ID
		vecID := binary.LittleEndian.Uint64(data)

		return &ReassignTask{
			vecID:     vecID,
			postingID: tq.reassignList.getLastKnownPostingID(vecID),
			idx:       tq.index,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type SplitTask struct {
	id  uint64
	idx *HFresh
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
	t.idx.metrics.IncSplitCount()
	return nil
}

type MergeTask struct {
	id  uint64
	idx *HFresh
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
	t.idx.metrics.IncMergeCount()
	return nil
}

type ReassignTask struct {
	vecID     uint64
	postingID uint64
	idx       *HFresh
}

func (t *ReassignTask) Op() uint8 {
	return taskQueueReassignOp
}

func (t *ReassignTask) Key() uint64 {
	return t.vecID
}

func (t *ReassignTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	err := t.idx.doReassign(ctx, reassignOperation{VectorID: t.vecID, PostingID: t.postingID})
	if err != nil {
		return err
	}

	t.idx.metrics.DequeueReassignTask()
	t.idx.metrics.IncReassignCount()
	return nil
}

func encodeTask(id uint64, op uint8) []byte {
	buf := make([]byte, 9)
	buf[0] = op
	binary.LittleEndian.PutUint64(buf[1:9], id)
	return buf
}

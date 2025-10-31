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
	case taskQueueSplitOp, taskQueueMergeOp:
		// decode id
		id := binary.LittleEndian.Uint64(data)

		return &Task{
			op:  op,
			id:  id,
			idx: v.q.spfreshIndex,
		}, nil
	case taskQueueReassignOp:
		// decode id
		postingID := binary.LittleEndian.Uint64(data)
		data = data[8:]
		vecID := binary.LittleEndian.Uint64(data)
		data = data[8:]
		version := VectorVersion(data[0])
		return &Task{
			op:      op,
			id:      postingID,
			vecID:   vecID,
			version: version,
			idx:     v.q.spfreshIndex,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type Task struct {
	op      uint8
	id      uint64
	vecID   uint64
	version VectorVersion
	idx     *SPFresh
}

func (t *Task) Op() uint8 {
	return t.op
}

func (t *Task) Key() uint64 {
	// TODO: find a better way to get the key
	return t.id
}

func (t *Task) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	switch t.op {
	case taskQueueSplitOp:
		return t.idx.doSplit(t.id, true)
	case taskQueueMergeOp:
		return t.idx.doMerge(t.id)
	case taskQueueReassignOp:
		return t.idx.doReassign(reassignOperation{PostingID: t.id, Vector: &RawVector{id: t.vecID, version: t.version, data: nil}})
	}

	return errors.Errorf("unknown operation: %d", t.Op())
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

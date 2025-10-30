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
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

const (
	operationsQueueSplitOp uint8 = iota + 1
	operationsQueueMergeOp
	operationsQueueReassignOp
)

type OperationsQueue struct {
	*queue.DiskQueue

	scheduler *queue.Scheduler
	// metrics      *VectorIndexQueueMetrics TODO: add metrics

	// If positive, accumulates vectors in a batch before indexing them.
	// Otherwise, the batch size is determined by the size of a chunk file
	// (typically 10MB worth of vectors).
	// Batch size is not guaranteed to match this value exactly.
	batchSize int

	spfreshIndex *SPFresh
}

func NewOperationsQueue(
	spfreshIndex *SPFresh,
	targetVector string,
) (*OperationsQueue, error) {
	opq := OperationsQueue{
		spfreshIndex: spfreshIndex,
		scheduler:    spfreshIndex.scheduler,
	}

	staleTimeout, _ := time.ParseDuration(os.Getenv("SPFRESH_OPERATIONS_STALE_TIMEOUT"))
	batchSize, _ := strconv.Atoi(os.Getenv("SPFRESH_OPERATIONS_BATCH_SIZE"))
	if batchSize > 0 {
		opq.batchSize = batchSize
	}

	// viq.metrics = NewVectorIndexQueueMetrics(logger, shard.promMetrics, shard.index.Config.ClassName.String(), shard.Name(), targetVector) TODO: add metrics

	q, err := queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:        fmt.Sprintf("vector_index_queue_%s_%s", spfreshIndex.config.ShardName, spfreshIndex.config.ID),
			Logger:    spfreshIndex.logger,
			Scheduler: spfreshIndex.scheduler,
			Dir:       filepath.Join(spfreshIndex.config.RootPath, fmt.Sprintf("%s.queue.d", spfreshIndex.config.ID)),
			TaskDecoder: &OperationsQueueDecoder{
				q: &opq,
			},
			OnBatchProcessed: opq.OnBatchProcessed,
			StaleTimeout:     staleTimeout,
			// Metrics:          opq.metrics.QueueMetrics(), TODO: add metrics
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

func (opq *OperationsQueue) Close() error {
	if opq == nil {
		// the queue is nil when the shard is not fully initialized
		return nil
	}

	return opq.DiskQueue.Close()
}

func (opq *OperationsQueue) EnqueueSplit(ctx context.Context, postingID uint64) error {
	/*start := time.Now()
	defer opq.metrics.Insert(start, 1) TODO: add metrics*/

	var buf []byte
	var err error

	// TODO: validate postingID

	// encode split
	buf = buf[:0]
	buf, err = encodeOperation(buf, postingID, operationsQueueSplitOp)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	err = opq.Push(buf)
	if err != nil {
		return errors.Wrap(err, "failed to push record to queue")
	}

	return nil
}

func (opq *OperationsQueue) EnqueueMerge(ctx context.Context, postingID uint64) error {
	/*start := time.Now()
	defer opq.metrics.Insert(start, 1) TODO: add metrics*/

	var buf []byte
	var err error

	// TODO: validate postingID

	// encode split
	buf = buf[:0]
	buf, err = encodeOperation(buf, postingID, operationsQueueMergeOp)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	err = opq.Push(buf)
	if err != nil {
		return errors.Wrap(err, "failed to push record to queue")
	}

	return nil
}

func (opq *OperationsQueue) EnqueueReassign(ctx context.Context, postingID uint64, vecID uint64, version VectorVersion) error {
	/*start := time.Now()
	defer opq.metrics.Insert(start, 1) TODO: add metrics*/

	// TODO: validate postingID

	buf := make([]byte, 18)
	buf[0] = operationsQueueReassignOp
	binary.BigEndian.PutUint64(buf[1:9], postingID)
	binary.BigEndian.PutUint64(buf[9:17], vecID)
	buf[17] = byte(version)

	err := opq.Push(buf)
	if err != nil {
		return errors.Wrap(err, "failed to push record to queue")
	}

	return nil
}

func (opq *OperationsQueue) Flush() error {
	if opq == nil {
		// the queue is nil when the shard is not fully initialized
		return nil
	}

	return opq.DiskQueue.Flush()
}

func (opq *OperationsQueue) BeforeSchedule() (skip bool) {
	// TODO: do we need this?
	return false
}

// Flush the vector index after a batch is processed.
func (opq *OperationsQueue) OnBatchProcessed() {
	if err := opq.spfreshIndex.Flush(); err != nil {
		opq.Logger.WithError(err).Error("failed to flush vector index")
	}
}

type OperationsQueueDecoder struct {
	q *OperationsQueue
}

func (v *OperationsQueueDecoder) DecodeTask(data []byte) (queue.Task, error) {
	op := data[0]
	data = data[1:]

	switch op {
	case operationsQueueSplitOp, operationsQueueMergeOp:
		// decode id
		id := binary.BigEndian.Uint64(data)

		return &Task{
			op:  op,
			id:  id,
			idx: v.q.spfreshIndex,
		}, nil
	case operationsQueueReassignOp:
		// decode id
		postingID := binary.BigEndian.Uint64(data)
		data = data[8:]
		vecID := binary.BigEndian.Uint64(data)
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
	case operationsQueueSplitOp:
		return t.idx.doSplit(t.id, true) // TODO: check if we need to pass reassign
	case operationsQueueMergeOp:
		return t.idx.doMerge(t.id)
	case operationsQueueReassignOp:
		return t.idx.doReassign(reassignOperation{PostingID: t.id, Vector: &RawVector{id: t.vecID, version: t.version, data: nil}}) // TODO: get vector from the index
	}

	return errors.Errorf("unknown operation: %d", t.Op())
}

func encodeOperation(buf []byte, id uint64, op uint8) ([]byte, error) {
	switch op {
	case operationsQueueSplitOp:
		// write the operation first
		buf = append(buf, operationsQueueSplitOp)
		// put multi or normal vector operation header!
		buf = binary.BigEndian.AppendUint64(buf, id)
		return buf, nil
	case operationsQueueMergeOp:
		// write the operation first
		buf = append(buf, operationsQueueMergeOp)
		// put multi or normal vector operation header!
		buf = binary.BigEndian.AppendUint64(buf, id)
		return buf, nil
	default:
		return nil, errors.Errorf("unrecognized operation: %d", op)
	}
}

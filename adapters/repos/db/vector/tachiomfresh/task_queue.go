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

package tachiomfresh

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// This file is a copy of hfresh's task_queue.go: the index reuses the
// HFresh task queue and its triggers. hfresh's queue cannot be imported
// for this index: hfresh.NewTaskQueue takes *HFresh, and its tasks call
// HFresh's unexported doSplit, doMerge and doReassign. The differences:
//
//   - There is no analyze queue. hfresh's analyze syncs the on-disk posting
//     map with the posting store. The posting table here is already in
//     memory.
//   - The reassign task carries a token (docID, tokenPos) where hfresh's
//     carries a vector id.
//   - The reassign dedup list is a hash set where hfresh's is a PagedBitset.
//     A PagedBitset holds one bit per id in lazily allocated pages of 64K
//     ids, up to 2^16 pages, so it covers ids below 2^32. That fits vector
//     and posting ids. A token key is 48 bits (4 bytes of doc id, 2 of
//     token position). In a bitset it would need one 8 KiB page per
//     document with a pending reassign and a page table of 2^32 entries.
//   - A cleanup task (cleanup.go), which hfresh does not have, uses the
//     split queue under its own op code. It is keyed by the posting's lock
//     hash, as a split is, and it has its own dedup bitset. There is no
//     fourth queue directory to back up and list (see queues in index.go).

// The same values as hfresh's, whose list starts with the analyze op at 1.
const (
	taskQueueSplitOp uint8 = iota + 2
	taskQueueMergeOp
	taskQueueReassignOp
	// Not in hfresh: the cleanup of a posting's dead entries (cleanup.go).
	// It comes after hfresh's values so that no value is reused.
	taskQueueCleanupOp
)

// The size of each task chunk is limited, to avoid sending too many tasks
// at once.
const (
	splitTaskQueueChunkSize    = 16 * 1024 // 16KB
	reassignTaskQueueChunkSize = 16 * 1024 // 16KB
	mergeTaskQueueChunkSize    = 16 * 1024 // 16KB
	taskDeduplicatorMaxPages   = 1 << 16   // Supports IDs < 1<<32 with 64K IDs per page
)

type TaskQueue struct {
	// queue for split operations
	splitQueue *queue.DiskQueue
	// queue for reassign operations
	reassignQueue *queue.DiskQueue
	// queue for merge operations, as these need to be run sequentially
	mergeQueue *queue.DiskQueue

	scheduler *queue.Scheduler

	index        *TachiomFresh
	splitList    *common.PagedBitset // Prevents duplicate split operations
	mergeList    *common.PagedBitset // Prevents duplicate merge operations
	reassignList *tokenSet           // Prevents duplicate reassign operations
	cleanupList  *common.PagedBitset // Prevents duplicate cleanup operations
}

// tokenSet is a concurrent set of reassign tasks, the dedup list of the
// reassign queue (the top of the file says why it is not a PagedBitset). It
// is kept minimal, in the spirit of Rust's FxHashSet. The key is the packed
// 48-bit token and the posting the task names: a split or a merge that moves
// a token asks for its reassign in the new posting while a reassign in the
// old one may still be queued, and the old one does nothing once its
// posting is gone. The shard is picked with the single multiply of FxHash,
// so workers that enqueue rarely share a lock.
//
// A Go map keeps its buckets when its entries are deleted. A shard that
// held many tokens during a burst of reassigns would therefore keep that
// memory for good: after a build the empty set held a large share of the
// live heap. A shard therefore starts a new map when it empties after
// having held more than tokenSetShrinkAt tokens (its peak).
type tokenSet struct {
	shards [tokenSetShards]struct {
		sync.Mutex
		m    map[reassignKey]struct{}
		peak int
	}
}

// reassignKey is a reassign task: the token's packed key and its posting.
type reassignKey struct {
	token, posting uint64
}

const (
	tokenSetShardBits = 6
	tokenSetShards    = 1 << tokenSetShardBits
	// the multiplier of FxHash (rustc-hash), 64-bit
	fxSeed = 0x517cc1b727220a95
	// Below this a shard's map is a few tens of KB and is kept. A queue
	// that fills and drains all the time should not reallocate each time.
	tokenSetShrinkAt = 1024
)

func newTokenSet() *tokenSet {
	var s tokenSet
	for i := range s.shards {
		s.shards[i].m = make(map[reassignKey]struct{})
	}
	return &s
}

func tokenSetShard(key reassignKey) uint64 {
	return ((key.token ^ key.posting<<16) * fxSeed) >> (64 - tokenSetShardBits)
}

// TryAdd adds the reassign of ref in posting postingID if it is not already
// present. It returns true if it was added, and false if it was already
// present.
func (s *tokenSet) TryAdd(postingID uint64, ref tokenRef) bool {
	key := reassignKey{token: ref.key(), posting: postingID}
	shard := &s.shards[tokenSetShard(key)]
	shard.Lock()
	defer shard.Unlock()
	if _, ok := shard.m[key]; ok {
		return false
	}
	shard.m[key] = struct{}{}
	shard.peak = max(shard.peak, len(shard.m))
	return true
}

func (s *tokenSet) Delete(postingID uint64, ref tokenRef) {
	key := reassignKey{token: ref.key(), posting: postingID}
	shard := &s.shards[tokenSetShard(key)]
	shard.Lock()
	delete(shard.m, key)
	if len(shard.m) == 0 && shard.peak > tokenSetShrinkAt {
		shard.m = make(map[reassignKey]struct{})
		shard.peak = 0
	}
	shard.Unlock()
}

// Len returns the number of tokens in the set.
func (s *tokenSet) Len() int {
	var n int
	for i := range s.shards {
		s.shards[i].Lock()
		n += len(s.shards[i].m)
		s.shards[i].Unlock()
	}
	return n
}

func NewTaskQueue(index *TachiomFresh) (*TaskQueue, error) {
	var err error

	tq := TaskQueue{
		index:        index,
		scheduler:    index.scheduler,
		splitList:    common.NewPagedBitset(taskDeduplicatorMaxPages),
		mergeList:    common.NewPagedBitset(taskDeduplicatorMaxPages),
		reassignList: newTokenSet(),
		cleanupList:  common.NewPagedBitset(taskDeduplicatorMaxPages),
	}

	// create queue for split operations
	tq.splitQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("tachiomfresh_split_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "split.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			OnChunkRemoved:   tq.OnChunkRemoved,
			ChunkSize:        splitTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create tachiomfresh split queue")
	}
	err = tq.splitQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize tachiomfresh split queue")
	}

	// create queue for reassign operations
	tq.reassignQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("tachiomfresh_reassign_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "reassign.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			OnChunkRemoved:   tq.OnChunkRemoved,
			ChunkSize:        reassignTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create tachiomfresh reassign queue")
	}
	err = tq.reassignQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize tachiomfresh reassign queue")
	}

	// create queue for merge operations
	tq.mergeQueue, err = queue.NewDiskQueue(
		queue.DiskQueueOptions{
			ID:               fmt.Sprintf("tachiomfresh_merge_queue_%s_%s", index.config.ShardName, index.config.ID),
			Logger:           index.logger,
			Scheduler:        index.scheduler,
			Dir:              filepath.Join(index.config.RootPath, "merge.queue.d"),
			TaskDecoder:      &tq,
			OnBatchProcessed: tq.OnBatchProcessed,
			OnChunkRemoved:   tq.OnChunkRemoved,
			ChunkSize:        mergeTaskQueueChunkSize,
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create tachiomfresh merge queue")
	}
	err = tq.mergeQueue.Init()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize tachiomfresh merge queue")
	}

	return &tq, nil
}

func (tq *TaskQueue) Register() {
	tq.scheduler.RegisterQueue(tq.splitQueue)
	tq.scheduler.RegisterQueue(tq.reassignQueue)
	tq.scheduler.RegisterQueue(tq.mergeQueue)
}

func (tq *TaskQueue) Close(ctx context.Context) error {
	var errs []error
	if err := tq.Flush(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to flush task queue before close"))
	}

	if err := tq.splitQueue.Close(ctx); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close split queue"))
	}

	if err := tq.reassignQueue.Close(ctx); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close reassign queue"))
	}

	if err := tq.mergeQueue.Close(ctx); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close merge queue"))
	}

	return stderrors.Join(errs...)
}

func (tq *TaskQueue) Flush() error {
	var errs []error

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
	tq.splitList.Delete(postingID)
}

func (tq *TaskQueue) MergeDone(postingID uint64) {
	tq.mergeList.Delete(postingID)
}

func (tq *TaskQueue) ReassignDone(postingID uint64, ref tokenRef) {
	tq.reassignList.Delete(postingID, ref)
}

func (tq *TaskQueue) CleanupDone(postingID uint64) {
	tq.cleanupList.Delete(postingID)
}

func (tq *TaskQueue) MergeContains(postingID uint64) bool {
	return tq.mergeList.Contains(postingID)
}

func (tq *TaskQueue) EnqueueSplit(postingID uint64) error {
	// Check if the operation is already enqueued
	if !tq.splitList.TryAdd(postingID) {
		return nil
	}

	if err := tq.splitQueue.Push(encodeTask(postingID, taskQueueSplitOp)); err != nil {
		// not hfresh's: as EnqueueReassign, so that a later enqueue queues it
		tq.splitList.Delete(postingID)
		return errors.Wrap(err, "failed to push split operation to queue")
	}

	tq.index.metrics.SetPendingSplitTasks(tq.splitQueue.Size())

	return nil
}

func (tq *TaskQueue) EnqueueMerge(postingID uint64) error {
	// Check if the operation is already enqueued
	if !tq.mergeList.TryAdd(postingID) {
		return nil
	}

	if err := tq.mergeQueue.Push(encodeTask(postingID, taskQueueMergeOp)); err != nil {
		// not hfresh's: as EnqueueReassign, so that a later enqueue queues it
		tq.mergeList.Delete(postingID)
		return errors.Wrap(err, "failed to push merge operation to queue")
	}

	tq.index.metrics.SetPendingMergeTasks(tq.mergeQueue.Size())

	return nil
}

// EnqueueCleanup asks for the entries of deleted documents to be removed
// from posting postingID (cleanup.go). It does what EnqueueSplit does, on
// the split queue (see the top of the file).
func (tq *TaskQueue) EnqueueCleanup(postingID uint64) error {
	// Check if the operation is already enqueued
	if !tq.cleanupList.TryAdd(postingID) {
		return nil
	}

	if err := tq.splitQueue.Push(encodeTask(postingID, taskQueueCleanupOp)); err != nil {
		// not hfresh's: as EnqueueReassign, so that a later enqueue queues it
		tq.cleanupList.Delete(postingID)
		return errors.Wrap(err, "failed to push cleanup operation to queue")
	}

	tq.index.metrics.SetPendingSplitTasks(tq.splitQueue.Size())

	return nil
}

// EnqueueReassign asks for token ref, currently in posting postingID, to be
// routed again.
func (tq *TaskQueue) EnqueueReassign(postingID uint64, ref tokenRef) error {
	// Check if the operation is already enqueued
	if !tq.reassignList.TryAdd(postingID, ref) {
		return nil
	}

	if err := tq.reassignQueue.Push(encodeReassignTask(ref, postingID)); err != nil {
		tq.reassignList.Delete(postingID, ref)
		return errors.Wrap(err, "failed to push reassign operation to queue")
	}

	tq.index.metrics.SetPendingReassignTasks(tq.reassignQueue.Size())

	return nil
}

// OnBatchProcessed flushes the vector index after a batch is processed, as
// hfresh's does. A non-nil error keeps the processed chunk on disk, so the
// queue replays it on a later cycle.
func (tq *TaskQueue) OnBatchProcessed() error {
	// the postings live in the shard's buckets, and the centroid commit log
	// must not reach its file before them
	if tq.index.store != nil {
		if err := tq.index.store.WriteWALs(); err != nil {
			return err
		}
	}

	return tq.index.Flush()
}

// OnChunkRemoved updates the pending metrics, now that the queue sizes have
// been decremented.
func (tq *TaskQueue) OnChunkRemoved() {
	tq.index.metrics.SetPendingSplitTasks(tq.splitQueue.Size())
	tq.index.metrics.SetPendingMergeTasks(tq.mergeQueue.Size())
	tq.index.metrics.SetPendingReassignTasks(tq.reassignQueue.Size())
}

func (tq *TaskQueue) DecodeTask(data []byte) (queue.Task, error) {
	op := data[0]
	data = data[1:]

	switch op {
	case taskQueueSplitOp:
		if len(data) < 8 {
			return nil, errors.Errorf("invalid split task length: %d", len(data)+1)
		}
		return &SplitTask{
			id:  binary.LittleEndian.Uint64(data),
			idx: tq.index,
		}, nil
	case taskQueueMergeOp:
		if len(data) < 8 {
			return nil, errors.Errorf("invalid merge task length: %d", len(data)+1)
		}
		return &MergeTask{
			id:  binary.LittleEndian.Uint64(data),
			idx: tq.index,
		}, nil
	case taskQueueReassignOp:
		if len(data) < 15 {
			return nil, errors.Errorf("invalid reassign task length: %d", len(data)+1)
		}
		return &ReassignTask{
			ref:       newTokenRef(binary.LittleEndian.Uint32(data[:4]), binary.LittleEndian.Uint16(data[4:6]), data[14]),
			postingID: binary.LittleEndian.Uint64(data[6:14]),
			idx:       tq.index,
		}, nil
	case taskQueueCleanupOp:
		if len(data) < 8 {
			return nil, errors.Errorf("invalid cleanup task length: %d", len(data)+1)
		}
		return &CleanupTask{
			id:  binary.LittleEndian.Uint64(data),
			idx: tq.index,
		}, nil
	}

	return nil, errors.Errorf("unknown operation: %d", op)
}

type SplitTask struct {
	id  uint64
	idx *TachiomFresh
}

func (t *SplitTask) Op() uint8 {
	return taskQueueSplitOp
}

func (t *SplitTask) Key() uint64 {
	// Postings that share a lock are run by the same worker, to reduce
	// contention.
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

	t.idx.metrics.IncSplitCount()
	return nil
}

type MergeTask struct {
	id  uint64
	idx *TachiomFresh
}

func (t *MergeTask) Op() uint8 {
	return taskQueueMergeOp
}

func (t *MergeTask) Key() uint64 {
	// Merges must run one after another, so the key is fixed: the hash of
	// the index ID.
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

	t.idx.metrics.IncMergeCount()
	return nil
}

// CleanupTask removes the entries of deleted documents from one posting
// (cleanup.go). hfresh has no such task. Its metric counts the cleanups that
// rewrote a posting.
type CleanupTask struct {
	id  uint64
	idx *TachiomFresh
}

func (t *CleanupTask) Op() uint8 {
	return taskQueueCleanupOp
}

func (t *CleanupTask) Key() uint64 {
	// The same key as SplitTask, the posting's lock hash. A split and a
	// cleanup of the same posting then run on the same worker, one after
	// the other.
	return t.idx.postingLocks.Hash(t.id)
}

func (t *CleanupTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return t.idx.doCleanup(ctx, t.id)
}

type ReassignTask struct {
	ref       tokenRef
	postingID uint64
	idx       *TachiomFresh
}

func (t *ReassignTask) Op() uint8 {
	return taskQueueReassignOp
}

func (t *ReassignTask) Key() uint64 {
	return t.ref.key()
}

func (t *ReassignTask) Execute(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	err := t.idx.doReassign(ctx, reassignOperation{Ref: t.ref, PostingID: t.postingID})
	if err != nil {
		return err
	}

	t.idx.metrics.IncReassignCount()
	return nil
}

func encodeTask(id uint64, op uint8) []byte {
	buf := make([]byte, 9)
	buf[0] = op
	binary.LittleEndian.PutUint64(buf[1:9], id)
	return buf
}

// encodeReassignTask writes op, docID (u32), tokenPos (u16), the posting id
// (u64) and the entry's version (u8, 0 for the first): the posting entry,
// with the posting the token was in when the task was enqueued.
func encodeReassignTask(ref tokenRef, postingID uint64) []byte {
	buf := make([]byte, 16)
	buf[0] = taskQueueReassignOp
	binary.LittleEndian.PutUint32(buf[1:5], ref.DocID)
	binary.LittleEndian.PutUint16(buf[5:7], ref.Pos)
	binary.LittleEndian.PutUint64(buf[7:15], postingID)
	buf[15] = ref.Version
	return buf
}

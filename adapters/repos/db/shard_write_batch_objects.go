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
	"github.com/pkg/errors"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/configbase"
	"github.com/weaviate/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) PutObjectBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	s.activityTracker.Add(1)
	if s.isReadOnly() {
		return []error{storagestate.ErrStatusReadOnly}
	}

	return s.putBatch(ctx, objects)
}

// asyncEnabled is a quick and dirty way to create a feature flag for async
// indexing.
func asyncEnabled() bool {
	return configbase.Enabled(os.Getenv("ASYNC_INDEXING"))
}

// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
// When the last batch finishes, all workers receive a shutdown signal and exit
func (s *Shard) putBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	if asyncEnabled() {
		return s.putBatchAsync(ctx, objects)
	}
	// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
	// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
	// When the last batch finishes, all workers receive a shutdown signal and exit
	batcher := newObjectsBatcher(s)
	err := batcher.PutObjects(ctx, objects)

	// block until all objects of batch have been added
	batcher.wg.Wait()
	s.metrics.VectorIndex(batcher.batchStartTime)

	return err
}

func (s *Shard) putBatchAsync(ctx context.Context, objects []*storobj.Object) []error {
	beforeBatch := time.Now()
	defer s.metrics.BatchObject(beforeBatch, len(objects))

	batcher := newObjectsBatcher(s)

	batcher.initPut(objects)
	batcher.putInObjectStore(ctx)
	batcher.markDeletedInVectorStorage(ctx)
	batcher.storeAdditionalStorageWithAsyncQueue(ctx)
	batcher.flushWALs(ctx)

	return batcher.putErrors
}

func (s *Shard) MergeObjectBatch(ctx context.Context,
	docs []*objects.BatchMergeDocument,
) []error {
	if s.isReadOnly() {
		return []error{storagestate.ErrStatusReadOnly}
	}

	return s.mergeBatch(ctx, docs)
}

func (s *Shard) mergeBatch(ctx context.Context, docs []*objects.BatchMergeDocument) []error {
	if asyncEnabled() {
		// TODO
	}

	batcher := newObjectsBatcher(s)
	err := batcher.MergeObjects(ctx, docs)

	batcher.wg.Wait()
	return err
}

// objectsBatcher is a helper type wrapping around an underlying shard that can
// execute objects batch operations on a shard (as opposed to references batch
// operations)
type objectsBatcher struct {
	sync.Mutex
	shard ShardLike
	// putErrors are the errors encountered while `put`ing a batch.
	// This is not needed for `merge`, because `BatchMergeDocument`
	// already contains an error property
	putErrors      []error
	putStatuses    map[strfmt.UUID]objectInsertStatus
	duplicates     map[int]struct{}
	objects        []*storobj.Object
	mergeDocs      []*objects.BatchMergeDocument
	wg             sync.WaitGroup
	batchStartTime time.Time
}

func newObjectsBatcher(s ShardLike) *objectsBatcher {
	return &objectsBatcher{shard: s}
}

// Objects imports the specified objects in parallel in a batch-fashion
func (ob *objectsBatcher) PutObjects(ctx context.Context,
	objects []*storobj.Object,
) []error {
	beforeBatch := time.Now()
	defer ob.shard.Metrics().BatchObject(beforeBatch, len(objects))

	ob.initPut(objects)
	ob.putInObjectStore(ctx)
	ob.markDeletedInVectorStorage(ctx)
	ob.storeAdditionalStorageWithWorkers(ctx)
	ob.flushWALs(ctx)
	return ob.putErrors
}

func (ob *objectsBatcher) MergeObjects(ctx context.Context,
	docs []*objects.BatchMergeDocument,
) []error {
	ob.initMerge(docs)
	ob.mergeInObjectStore(ctx)
	return ob.putErrors
}

func (ob *objectsBatcher) initPut(objects []*storobj.Object) {
	ob.objects = objects
	ob.putStatuses = map[strfmt.UUID]objectInsertStatus{}
	ob.putErrors = make([]error, len(objects))
	ob.duplicates = findDuplicatesInBatchObjects(objects)
}

func (ob *objectsBatcher) initMerge(docs []*objects.BatchMergeDocument) {
	ob.mergeDocs = docs
}

func (ob *objectsBatcher) mergeInObjectStore(ctx context.Context) {
	errs := ob.mergeSingleBatchInLSM(ctx)
	for i, err := range errs {
		if err != nil {
			ob.setErrorAtIndex(err, i)
		}
	}
}

func (ob *objectsBatcher) mergeSingleBatchInLSM(ctx context.Context,
) []error {
	errs := make([]error, len(ob.mergeDocs))
	errLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range errs {
			errs[i] = fmt.Errorf("begin batch merge: %w", err)
		}
		return errs
	}

	wg := &sync.WaitGroup{}
	concurrencyLimit := make(chan struct{}, _NUMCPU)

	for j, doc := range ob.mergeDocs {
		wg.Add(1)
		doc := doc
		index := j
		f := func() {
			defer wg.Done()

			// Acquire a semaphore to control the concurrency. Otherwise we would
			// spawn one routine per object here. With very large batch sizes (e.g.
			// 1000 or 10000+), this isn't helpuful and just leads to more lock
			// contention down the line – especially when there's lots of text to be
			// indexed in the inverted index.
			concurrencyLimit <- struct{}{}
			defer func() {
				// Release the semaphore when the goroutine is done.
				<-concurrencyLimit
			}()

			if err := ob.mergeObjectOfBatchInLSM(ctx, index, doc); err != nil {
				errLock.Lock()
				errs[index] = err
				errLock.Unlock()
			}
		}
		enterrors.GoWrapper(f, ob.shard.Index().logger)

	}
	wg.Wait()

	return errs
}

// putInObjectStore performs all storage operations on the underlying
// key/value store, this is they object-by-id store, the docID-lookup tables,
// as well as all inverted indices.
func (ob *objectsBatcher) putInObjectStore(ctx context.Context) {
	beforeObjectPut := time.Now()

	errs := ob.putSingleBatchInLSM(ctx)
	for i, err := range errs {
		if err != nil {
			ob.setErrorAtIndex(err, i)
		}
	}

	ob.shard.Metrics().ObjectStore(beforeObjectPut)
}

func (ob *objectsBatcher) putSingleBatchInLSM(ctx context.Context,
) []error {
	errs := make([]error, len(ob.objects))
	errLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range errs {
			errs[i] = fmt.Errorf("begin batch: %w", err)
		}
		return errs
	}

	wg := &sync.WaitGroup{}
	concurrencyLimit := make(chan struct{}, _NUMCPU)

	for j, object := range ob.objects {
		wg.Add(1)
		object := object
		index := j
		f := func() {
			defer wg.Done()

			// Acquire a semaphore to control the concurrency. Otherwise we would
			// spawn one routine per object here. With very large batch sizes (e.g.
			// 1000 or 10000+), this isn't helpuful and just leads to more lock
			// contention down the line – especially when there's lots of text to be
			// indexed in the inverted index.
			concurrencyLimit <- struct{}{}
			defer func() {
				// Release the semaphore when the goroutine is done.
				<-concurrencyLimit
			}()

			if err := ob.putObjectOfBatchInLSM(ctx, index, object); err != nil {
				errLock.Lock()
				errs[index] = err
				errLock.Unlock()
			}
		}
		enterrors.GoWrapper(f, ob.shard.Index().logger)

	}
	wg.Wait()

	return errs
}

func (ob *objectsBatcher) mergeObjectOfBatchInLSM(ctx context.Context,
	objectIndex int, mergeDoc *objects.BatchMergeDocument,
) error {
	if _, ok := ob.duplicates[objectIndex]; ok {
		return nil
	}
	uuidParsed, err := uuid.Parse(mergeDoc.ID.String())
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}

	idBytes, err := uuidParsed.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = ob.shard.mutableMergeObjectLSM(*mergeDoc.MergeDocument, idBytes)
	if err != nil {
		return err
	}

	// TODO: do we need this?
	//ob.setStatusForID(status, mergeDoc.ID)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("end store object %d of batch: %w", objectIndex, err)
	}
	return nil
}

func (ob *objectsBatcher) putObjectOfBatchInLSM(ctx context.Context,
	objectIndex int, object *storobj.Object,
) error {
	if _, ok := ob.duplicates[objectIndex]; ok {
		return nil
	}
	uuidParsed, err := uuid.Parse(object.ID().String())
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}

	idBytes, err := uuidParsed.MarshalBinary()
	if err != nil {
		return err
	}

	status, err := ob.shard.putObjectLSM(object, idBytes)
	if err != nil {
		return err
	}

	ob.setStatusForID(status, object.ID())

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("end store object %d of batch: %w", objectIndex, err)
	}
	return nil
}

// setStatusForID is thread-safe as it uses the underlying mutex to lock the
// statuses map when writing into it
func (ob *objectsBatcher) setStatusForID(status objectInsertStatus, id strfmt.UUID) {
	ob.Lock()
	defer ob.Unlock()
	ob.putStatuses[id] = status
}

func (ob *objectsBatcher) markDeletedInVectorStorage(ctx context.Context) {
	var docIDsToDelete []uint64
	var positions []int
	for pos, object := range ob.objects {
		status := ob.putStatuses[object.ID()]
		if status.docIDChanged {
			docIDsToDelete = append(docIDsToDelete, status.oldDocID)
			positions = append(positions, pos)
		}
	}

	if len(docIDsToDelete) == 0 {
		return
	}

	if ob.shard.hasTargetVectors() {
		for targetVector, queue := range ob.shard.Queues() {
			if err := queue.Delete(docIDsToDelete...); err != nil {
				for _, pos := range positions {
					ob.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), pos)
				}
			}
		}
	} else {
		if err := ob.shard.Queue().Delete(docIDsToDelete...); err != nil {
			for _, pos := range positions {
				ob.setErrorAtIndex(err, pos)
			}
		}
	}
}

// storeAdditionalStorageWithWorkers stores the object in all non-key-value
// stores, such as the main vector index as well as the property-specific
// indices, such as the geo-index.
func (ob *objectsBatcher) storeAdditionalStorageWithWorkers(ctx context.Context) {
	if ok := ob.checkContext(ctx); !ok {
		// if the context is no longer OK, there's no point in continuing - abort
		// early
		return
	}

	ob.batchStartTime = time.Now()

	for i, object := range ob.objects {
		status := ob.putStatuses[object.ID()]
		if ob.shouldSkipInAdditionalStorage(i, status) {
			continue
		}

		ob.wg.Add(1)
		ob.shard.addJobToQueue(job{
			object:  object,
			status:  status,
			index:   i,
			ctx:     ctx,
			batcher: ob,
		})
	}
}

func (ob *objectsBatcher) storeAdditionalStorageWithAsyncQueue(ctx context.Context) {
	if ok := ob.checkContext(ctx); !ok {
		// if the context is no longer OK, there's no point in continuing - abort
		// early
		return
	}

	ob.batchStartTime = time.Now()
	shouldGeoIndex := ob.shard.hasGeoIndex()

	var vectors []vectorDescriptor
	var targetVectors map[string][]vectorDescriptor
	hasTargetVectors := ob.shard.hasTargetVectors()
	if hasTargetVectors {
		targetVectors = make(map[string][]vectorDescriptor)
	} else {
		vectors = make([]vectorDescriptor, 0, len(ob.objects))
	}

	for i, object := range ob.objects {
		status := ob.putStatuses[object.ID()]

		if ob.shouldSkipInAdditionalStorage(i, status) {
			continue
		}

		if shouldGeoIndex {
			if err := ob.shard.updatePropertySpecificIndices(object, status); err != nil {
				ob.setErrorAtIndex(fmt.Errorf("update prop-specific indices: %w", err), i)
				continue
			}
		}

		// skip vector update, as vector was not changed
		// https://github.com/weaviate/weaviate/issues/3948
		if status.docIDPreserved {
			continue
		}

		if len(object.Vector) == 0 && len(object.Vectors) == 0 {
			continue
		}

		if hasTargetVectors {
			for targetVector, vector := range object.Vectors {
				targetVectors[targetVector] = append(targetVectors[targetVector], vectorDescriptor{
					id:     status.docID,
					vector: vector,
				})
			}
		} else {
			if len(object.Vector) > 0 {
				vectors = append(vectors, vectorDescriptor{
					id:     status.docID,
					vector: object.Vector,
				})
			}
		}
	}

	if hasTargetVectors {
		for targetVector, vectors := range targetVectors {
			queue, ok := ob.shard.Queues()[targetVector]
			if !ok {
				ob.setErrorAtIndex(fmt.Errorf("queue not found for target vector %s", targetVector), 0)
			} else {
				err := queue.Push(ctx, vectors...)
				if err != nil {
					ob.setErrorAtIndex(err, 0)
				}
			}
		}
	} else {
		err := ob.shard.Queue().Push(ctx, vectors...)
		if err != nil {
			ob.setErrorAtIndex(err, 0)
		}
	}
}

func (ob *objectsBatcher) shouldSkipInAdditionalStorage(i int, status objectInsertStatus) bool {
	if ok := ob.hasErrorAtIndex(i); ok {
		// had an error prior, ignore
		return true
	}

	// object was not changed, skip further updates
	// https://github.com/weaviate/weaviate/issues/3949
	if status.skipUpsert {
		return true
	}

	// no need to lock the mutex for a duplicate check, as we only ever write
	// during init() in there - not concurrently
	if _, ok := ob.duplicates[i]; ok {
		// is a duplicate, ignore
		return true
	}

	return false
}

func (ob *objectsBatcher) storeSingleObjectInAdditionalStorage(ctx context.Context,
	object *storobj.Object, status objectInsertStatus, index int,
) {
	defer func() {
		err := recover()
		if err != nil {
			ob.setErrorAtIndex(fmt.Errorf("an unexpected error occurred: %s", err), index)
			fmt.Fprintf(os.Stderr, "panic: %s\n", err)
			debug.PrintStack()
		}
	}()

	if err := ctx.Err(); err != nil {
		ob.setErrorAtIndex(fmt.Errorf("insert to vector index: %w", err), index)
		return
	}

	if object.Vector != nil || len(object.Vectors) > 0 {
		// By this time all required deletes (e.g. because of DocID changes) have
		// already been grouped and performed in bulk. Only the insertions are
		// left. The motivation for this change is explained in
		// https://github.com/weaviate/weaviate/pull/2697.
		//
		// Before this change, two identical batches in sequence would lead to
		// massive lock contention in the hnsw index, as each individual delete
		// requires a costly RW.Lock() operation which first drains all "readers"
		// which represent the regular imports. See "deleteVsInsertLock" inside the
		// hnsw store.
		//
		// With the improved logic, we group all batches up front in a single call,
		// so this highly concurrent method no longer needs to compete for those
		// expensive locks.
		//
		// Since this behavior is exclusive to batching, we can no longer call
		// shard.updateVectorIndex which would also handle the delete as required
		// for a non-batch update. Instead a new method has been introduced that
		// ignores deletes.
		if ob.shard.hasTargetVectors() {
			if len(object.Vectors) > 0 {
				if err := ob.shard.updateVectorIndexesIgnoreDelete(object.Vectors, status); err != nil {
					ob.setErrorAtIndex(fmt.Errorf("insert to vector index: %w", err), index)
					return
				}
			}
		} else {
			if object.Vector != nil {
				if err := ob.shard.updateVectorIndexIgnoreDelete(object.Vector, status); err != nil {
					ob.setErrorAtIndex(fmt.Errorf("insert to vector index: %w", err), index)
					return
				}
			}
		}
	}

	if err := ob.shard.updatePropertySpecificIndices(object, status); err != nil {
		ob.setErrorAtIndex(fmt.Errorf("update prop-specific indices: %w", err), index)
		return
	}
}

// hasErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// before reading from the errs map
func (ob *objectsBatcher) hasErrorAtIndex(i int) bool {
	ob.Lock()
	defer ob.Unlock()
	return ob.putErrors[i] != nil
}

// setErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// writing into the errs map
func (ob *objectsBatcher) setErrorAtIndex(err error, index int) {
	ob.Lock()
	defer ob.Unlock()
	ob.putErrors[index] = err
}

// checkContext does nothing if the context is still active. But if the context
// has error'd, it marks all objects which have not previously error'd yet with
// the ctx error
func (ob *objectsBatcher) checkContext(ctx context.Context) bool {
	if err := ctx.Err(); err != nil {
		for i, err := range ob.putErrors {
			if err == nil {
				// already has an error, ignore
				continue
			}

			ob.putErrors[i] = errors.Wrapf(err,
				"inverted indexing complete, about to start vector indexing")
		}

		return false
	}
	return true
}

func (ob *objectsBatcher) flushWALs(ctx context.Context) {
	if err := ob.shard.Store().WriteWALs(); err != nil {
		for i := range ob.objects {
			ob.setErrorAtIndex(err, i)
		}
	}

	if ob.shard.hasTargetVectors() {
		for targetVector, vectorIndex := range ob.shard.VectorIndexes() {
			if err := vectorIndex.Flush(); err != nil {
				for i := range ob.objects {
					ob.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), i)
				}
			}
		}
	} else {
		if err := ob.shard.VectorIndex().Flush(); err != nil {
			for i := range ob.objects {
				ob.setErrorAtIndex(err, i)
			}
		}
	}

	if err := ob.shard.GetPropertyLengthTracker().Flush(); err != nil {
		for i := range ob.objects {
			ob.setErrorAtIndex(err, i)
		}
	}
}

// returns the originalIndexIDs to be ignored
func findDuplicatesInBatchObjects(in []*storobj.Object) map[int]struct{} {
	count := map[strfmt.UUID]int{}
	for _, obj := range in {
		count[obj.ID()] = count[obj.ID()] + 1
	}

	ignore := map[int]struct{}{}
	for i, obj := range in {
		if c := count[obj.ID()]; c > 1 {
			count[obj.ID()] = c - 1
			ignore[i] = struct{}{}
		}
	}

	return ignore
}

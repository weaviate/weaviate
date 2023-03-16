//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	if s.isReadOnly() {
		return []error{storagestate.ErrStatusReadOnly}
	}

	return s.putBatch(ctx, objects)
}

// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
// When the last batch finishes, all workers receive a shutdown signal and exit
func (s *Shard) putBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
	// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
	// When the last batch finishes, all workers receive a shutdown signal and exit
	batcher := newObjectsBatcher(s)
	err := batcher.Objects(ctx, objects)

	// block until all objects of batch have been added
	batcher.wg.Wait()

	return err
}

// objectsBatcher is a helper type wrapping around an underlying shard that can
// execute objects batch operations on a shard (as opposed to references batch
// operations)
type objectsBatcher struct {
	sync.Mutex
	shard      *Shard
	statuses   map[strfmt.UUID]objectInsertStatus
	errs       []error
	duplicates map[int]struct{}
	objects    []*storobj.Object
	wg         sync.WaitGroup
}

func newObjectsBatcher(s *Shard) *objectsBatcher {
	return &objectsBatcher{shard: s}
}

// Objects imports the specified objects in parallel in a batch-fashion
func (b *objectsBatcher) Objects(ctx context.Context,
	objects []*storobj.Object,
) []error {
	beforeBatch := time.Now()
	defer b.shard.metrics.BatchObject(beforeBatch, len(objects))

	b.init(objects)
	b.storeInObjectStore(ctx)
	b.markDeletedInVectorStorage(ctx)
	b.storeAdditionalStorageWithWorkers(ctx)
	b.flushWALs(ctx)
	return b.errs
}

func (b *objectsBatcher) init(objects []*storobj.Object) {
	b.objects = objects
	b.statuses = map[strfmt.UUID]objectInsertStatus{}
	b.errs = make([]error, len(objects))
	b.duplicates = findDuplicatesInBatchObjects(objects)
}

// storeInObjectStore performs all storage operations on the underlying
// key/value store, this is they object-by-id store, the docID-lookup tables,
// as well as all inverted indices.
func (b *objectsBatcher) storeInObjectStore(ctx context.Context) {
	beforeObjectStore := time.Now()

	errs := b.storeSingleBatchInLSM(ctx, b.objects)
	for i, err := range errs {
		if err != nil {
			b.setErrorAtIndex(err, i)
		}
	}

	b.shard.metrics.ObjectStore(beforeObjectStore)
}

func (b *objectsBatcher) storeSingleBatchInLSM(ctx context.Context,
	batch []*storobj.Object,
) []error {
	errs := make([]error, len(batch))
	errLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range errs {
			errs[i] = errors.Wrap(err, "begin batch")
		}
		return errs
	}

	wg := &sync.WaitGroup{}
	for j, object := range batch {
		wg.Add(1)
		go func(index int, object *storobj.Object) {
			defer wg.Done()

			if err := b.storeObjectOfBatchInLSM(ctx, index, object); err != nil {
				errLock.Lock()
				errs[index] = err
				errLock.Unlock()
			}
		}(j, object)
	}
	wg.Wait()

	return errs
}

func (b *objectsBatcher) storeObjectOfBatchInLSM(ctx context.Context,
	objectIndex int, object *storobj.Object,
) error {
	if _, ok := b.duplicates[objectIndex]; ok {
		return nil
	}
	uuidParsed, err := uuid.Parse(object.ID().String())
	if err != nil {
		return errors.Wrap(err, "invalid id")
	}

	idBytes, err := uuidParsed.MarshalBinary()
	if err != nil {
		return err
	}

	status, err := b.shard.putObjectLSM(object, idBytes, false)
	if err != nil {
		return err
	}

	b.setStatusForID(status, object.ID())

	if err := ctx.Err(); err != nil {
		return errors.Wrapf(err, "end store object %d of batch", objectIndex)
	}
	return nil
}

// setStatusForID is thread-safe as it uses the underlying mutex to lock the
// statuses map when writing into it
func (b *objectsBatcher) setStatusForID(status objectInsertStatus, id strfmt.UUID) {
	b.Lock()
	defer b.Unlock()
	b.statuses[id] = status
}

func (b *objectsBatcher) markDeletedInVectorStorage(ctx context.Context) {
	var docIDsToDelete []uint64
	var positions []int
	for pos, object := range b.objects {
		status := b.statuses[object.ID()]
		if status.docIDChanged {
			docIDsToDelete = append(docIDsToDelete, status.oldDocID)
			positions = append(positions, pos)
		}
	}

	if len(docIDsToDelete) == 0 {
		return
	}

	if err := b.shard.vectorIndex.Delete(docIDsToDelete...); err != nil {
		for _, pos := range positions {
			b.setErrorAtIndex(err, pos)
		}
	}
}

// storeAdditionalStorageWithWorkers stores the object in all non-key-value
// stores, such as the main vector index as well as the property-specific
// indices, such as the geo-index.
func (b *objectsBatcher) storeAdditionalStorageWithWorkers(ctx context.Context) {
	if ok := b.checkContext(ctx); !ok {
		// if the context is no longer OK, there's no point in continuing - abort
		// early
		return
	}

	beforeVectorIndex := time.Now()

	for i, object := range b.objects {
		if b.shouldSkipInAdditionalStorage(i) {
			continue
		}

		b.wg.Add(1)
		status := b.statuses[object.ID()]
		b.shard.centralJobQueue <- job{
			object:  object,
			status:  status,
			index:   i,
			ctx:     ctx,
			batcher: b,
		}
	}

	b.shard.metrics.VectorIndex(beforeVectorIndex)
}

func (b *objectsBatcher) shouldSkipInAdditionalStorage(i int) bool {
	if ok := b.hasErrorAtIndex(i); ok {
		// had an error prior, ignore
		return true
	}

	// no need to lock the mutex for a duplicate check, as we only ever write
	// during init() in there - not concurrently
	if _, ok := b.duplicates[i]; ok {
		// is a duplicate, ignore
		return true
	}

	return false
}

func (b *objectsBatcher) storeSingleObjectInAdditionalStorage(ctx context.Context,
	object *storobj.Object, status objectInsertStatus, index int,
) {
	if err := ctx.Err(); err != nil {
		b.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
		return
	}

	if object.Vector != nil {
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
		if err := b.shard.updateVectorIndexIgnoreDelete(object.Vector, status); err != nil {
			b.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
			return
		}
	}

	if err := b.shard.updatePropertySpecificIndices(object, status); err != nil {
		b.setErrorAtIndex(errors.Wrap(err, "update prop-specific indices"), index)
		return
	}
}

// hasErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// before reading from the errs map
func (b *objectsBatcher) hasErrorAtIndex(i int) bool {
	b.Lock()
	defer b.Unlock()
	return b.errs[i] != nil
}

// setErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// writing into the errs map
func (b *objectsBatcher) setErrorAtIndex(err error, index int) {
	b.Lock()
	defer b.Unlock()
	b.errs[index] = err
}

// checkContext does nothing if the context is still active. But if the context
// has error'd, it marks all objects which have not previously error'd yet with
// the ctx error
func (s *objectsBatcher) checkContext(ctx context.Context) bool {
	if err := ctx.Err(); err != nil {
		for i, err := range s.errs {
			if err == nil {
				// already has an error, ignore
				continue
			}

			s.errs[i] = errors.Wrapf(err,
				"inverted indexing complete, about to start vector indexing")
		}

		return false
	}

	return true
}

func (b *objectsBatcher) flushWALs(ctx context.Context) {
	if err := b.shard.store.WriteWALs(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	if err := b.shard.vectorIndex.Flush(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
		}
	}

	if err := b.shard.propLengths.Flush(); err != nil {
		for i := range b.objects {
			b.setErrorAtIndex(err, i)
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

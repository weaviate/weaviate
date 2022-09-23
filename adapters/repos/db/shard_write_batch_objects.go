//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	if s.isReadOnly() {
		return []error{storagestate.ErrStatusReadOnly}
	}

	// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
	// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
	// When the last batch finishes, all workers receive a shutdown signal and exit
	s.startBatch()
	batcher := newObjectsBatcher(s)
	err := batcher.Objects(ctx, objects)

	// block until all objects of batch have been added
	batcher.wg.Wait()
	s.endBatch()

	return err
}

func (s *Shard) startBatch() {
	s.activeBatchesLock.Lock()
	s.numActiveBatches += 1

	// start workers in go routines that can be used by all batches
	if s.numActiveBatches == 1 {
		s.shutDownWg.Wait() // wait until any shutdown job is completed
		if s.promMetrics != nil {
			metric, err := s.promMetrics.GoroutinesCount.GetMetricWithLabelValues("object batcher", "")
			if err == nil {
				metric.Add(float64(s.maxNumberGoroutines))
			}
		}
		for i := 0; i < s.maxNumberGoroutines; i++ {
			go s.worker()
		}
		s.shutDownWg.Add(s.maxNumberGoroutines)
	}
	s.activeBatchesLock.Unlock()
}

func (s *Shard) endBatch() {
	s.activeBatchesLock.Lock()
	s.numActiveBatches -= 1

	// send abort jobs and wait for all workers to end (and finish all jobs before)
	if s.numActiveBatches == 0 {
		for i := 0; i < s.maxNumberGoroutines; i++ {
			s.jobQueueCh <- job{
				index: -1,
			}
		}
		if s.promMetrics != nil {
			metric, err := s.promMetrics.GoroutinesCount.GetMetricWithLabelValues("object batcher", "")
			if err == nil {
				metric.Sub(float64(s.maxNumberGoroutines))
			}
		}
	}

	s.activeBatchesLock.Unlock()
}

func (s *Shard) worker() {
	for job := range s.jobQueueCh {
		if job.index < 0 {
			s.shutDownWg.Done()
			return
		}
		job.batcher.storeSingleObjectInAdditionalStorage(job.ctx, job.object, job.status, job.index)
		job.batcher.wg.Done()
	}
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
func (b *objectsBatcher)  Objects(ctx context.Context,
	objects []*storobj.Object,
) []error {
	beforeBatch := time.Now()
	defer b.shard.metrics.BatchObject(beforeBatch, len(objects))

	b.init(objects)
	b.storeInObjectStore(ctx)
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
		b.shard.jobQueueCh <- job{
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
		// vector is now optional as of
		// https://github.com/semi-technologies/weaviate/issues/1800
		if err := b.shard.updateVectorIndex(object.Vector, status); err != nil {
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

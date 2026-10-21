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

package db

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// PutObjectBatch returns one error per object, in input order, refusals of the
// whole batch included. The caller maps position i onto that object's position
// in its own batch, so a shorter slice reports the objects it omits as written.
func (s *Shard) PutObjectBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	if err := s.isReadOnly(); err != nil {
		return duplicateErr(err, len(objects))
	}

	if err := s.index.usageLimits.CheckObjects(ctx, int64(len(objects)), s.index.Config.ClassName.String()); err != nil {
		return duplicateErr(err, len(objects))
	}

	return s.putBatch(ctx, objects)
}

// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
// When the last batch finishes, all workers receive a shutdown signal and exit
func (s *Shard) putBatch(ctx context.Context,
	objects []*storobj.Object,
) []error {
	s.activityTrackerWrite.Add(1)
	if s.index.AsyncIndexingEnabled {
		return s.putBatchAsync(ctx, objects)
	}
	// Workers are started with the first batch and keep working as there are objects to add from any batch. Each batch
	// adds its jobs (that contain the respective object) to a single queue that is then processed by the workers.
	// When the last batch finishes, all workers receive a shutdown signal and exit
	batcher := newObjectsBatcher(s, s.index.logger)
	err := batcher.Objects(ctx, objects)

	// block until all objects of batch have been added
	batcher.wg.Wait()
	s.metrics.VectorIndex(batcher.batchStartTime)

	return err
}

func (s *Shard) putBatchAsync(ctx context.Context, objects []*storobj.Object) []error {
	beforeBatch := time.Now()
	defer s.metrics.BatchObject(beforeBatch, len(objects))

	batcher := newObjectsBatcher(s, s.index.logger)

	batcher.init(objects)
	batcher.validateVectorDimensions(ctx)
	batcher.storeInObjectStore(ctx)
	batcher.markDeletedInVectorStorage(ctx)
	batcher.storeAdditionalStorageWithAsyncQueue(ctx)
	batcher.flushWALs(ctx)

	return batcher.errs
}

// objectsBatcher is a helper type wrapping around an underlying shard that can
// execute objects batch operations on a shard (as opposed to references batch
// operations)
type objectsBatcher struct {
	sync.Mutex
	shard          ShardLike
	statuses       map[strfmt.UUID]objectInsertStatus
	errs           []error
	duplicates     map[int]struct{}
	objects        []*storobj.Object
	wg             sync.WaitGroup
	batchStartTime time.Time
	logger         logrus.FieldLogger
}

func newObjectsBatcher(s ShardLike, logger logrus.FieldLogger) *objectsBatcher {
	return &objectsBatcher{shard: s, logger: logger}
}

// Objects imports the specified objects in parallel in a batch-fashion
func (ob *objectsBatcher) Objects(ctx context.Context,
	objects []*storobj.Object,
) []error {
	beforeBatch := time.Now()
	defer ob.shard.Metrics().BatchObject(beforeBatch, len(objects))

	ob.init(objects)
	ob.validateVectorDimensions(ctx)
	ob.storeInObjectStore(ctx)
	ob.markDeletedInVectorStorage(ctx)
	ob.storeAdditionalStorageWithWorkers(ctx)
	ob.flushWALs(ctx)
	return ob.errs
}

func (ob *objectsBatcher) init(objects []*storobj.Object) {
	ob.objects = objects
	ob.statuses = map[strfmt.UUID]objectInsertStatus{}
	ob.errs = make([]error, len(objects))
	ob.duplicates = findDuplicatesInBatchObjects(objects)
}

func (ob *objectsBatcher) validateVectorDimensions(ctx context.Context) {
	targetVectors := make(map[string]struct{})
	for _, obj := range ob.objects {
		if len(obj.Vector) > 0 {
			targetVectors[""] = struct{}{}
		}
		for targetVector, vec := range obj.Vectors {
			if len(vec) > 0 {
				targetVectors[targetVector] = struct{}{}
			}
		}
		for targetVector, mvec := range obj.MultiVectors {
			if len(mvec) > 0 {
				targetVectors[targetVector] = struct{}{}
			}
		}
	}

	keys := make([]string, 0, len(targetVectors))
	for k := range targetVectors {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, targetVector := range keys {
		// First: verify internal token consistency across all token vectors for each object
		for i, obj := range ob.objects {
			if ob.hasErrorAtIndex(i) {
				continue
			}
			if len(obj.MultiVectors) > 0 {
				if mvec, ok := obj.MultiVectors[targetVector]; ok && len(mvec) > 0 {
					tokenDim := -1
					hasVaryingDims := false
					for _, tokenVec := range mvec {
						if tokenDim == -1 {
							tokenDim = len(tokenVec)
						} else if len(tokenVec) != tokenDim {
							hasVaryingDims = true
							break
						}
					}
					if hasVaryingDims {
						ob.setErrorAtIndex(fmt.Errorf("target vector %q: multi vector array consists of vectors with varying dimensions", targetVector), i)
					}
				}
			}
		}

		targetVector := targetVector
		found, err := ob.shard.WithVectorIndex(targetVector, func(index VectorIndex) error {
			expectedDim := index.Dimension()
			if expectedDim <= 0 {
				dimIndices := make(map[int][]int)
				for i, obj := range ob.objects {
					if ob.hasErrorAtIndex(i) {
						continue
					}

					dim := -1
					if targetVector == "" && len(obj.Vector) > 0 {
						dim = len(obj.Vector)
					} else if len(obj.Vectors) > 0 {
						if vec, ok := obj.Vectors[targetVector]; ok && len(vec) > 0 {
							dim = len(vec)
						}
					}
					if dim == -1 && len(obj.MultiVectors) > 0 {
						if mvec, ok := obj.MultiVectors[targetVector]; ok && len(mvec) > 0 {
							dim = len(mvec[0])
						}
					}
					if dim >= 0 {
						dimIndices[dim] = append(dimIndices[dim], i)
					}
				}

				if len(dimIndices) > 1 {
					dims := make([]int, 0, len(dimIndices))
					for d := range dimIndices {
						dims = append(dims, d)
					}
					sort.Ints(dims)

					parts := make([]string, len(dims))
					for j, d := range dims {
						parts[j] = strconv.Itoa(d)
					}
					dimList := fmt.Sprintf("[%s]", strings.Join(parts, ", "))

					err := fmt.Errorf("target vector %q: conflicting dimensions in batch for uninitialized index (found %s); cannot establish dimension", targetVector, dimList)
					seen := make(map[int]struct{})
					for _, indices := range dimIndices {
						for _, idx := range indices {
							if _, ok := seen[idx]; !ok {
								seen[idx] = struct{}{}
								ob.setErrorAtIndex(err, idx)
							}
						}
					}
				}
				return nil
			}

			for i, obj := range ob.objects {
				if ob.hasErrorAtIndex(i) {
					continue
				}

				dim := -1
				if targetVector == "" && len(obj.Vector) > 0 {
					dim = len(obj.Vector)
				} else if len(obj.Vectors) > 0 {
					if vec, ok := obj.Vectors[targetVector]; ok && len(vec) > 0 {
						dim = len(vec)
					}
				}
				if dim == -1 && len(obj.MultiVectors) > 0 {
					if mvec, ok := obj.MultiVectors[targetVector]; ok && len(mvec) > 0 {
						dim = len(mvec[0])
					}
				}
				if dim >= 0 && dim != expectedDim {
					ob.setErrorAtIndex(fmt.Errorf("target vector %q: dimension mismatch: expected %d, got %d", targetVector, expectedDim, dim), i)
				}
			}
			return nil
		})
		if err != nil {
			ob.logger.WithField("targetVector", targetVector).Warnf("failed to validate vector dimensions for target vector: %v", err)
		}
		_ = found
	}
}

// storeInObjectStore performs all storage operations on the underlying
// key/value store, this is they object-by-id store, the docID-lookup tables,
// as well as all inverted indices.
func (ob *objectsBatcher) storeInObjectStore(ctx context.Context) {
	beforeObjectStore := time.Now()

	errs := ob.storeSingleBatchInLSM(ctx, ob.objects)
	for i, err := range errs {
		if err != nil {
			ob.setErrorAtIndex(err, i)
		}
	}

	ob.shard.Metrics().ObjectStore(beforeObjectStore)
}

func (ob *objectsBatcher) storeSingleBatchInLSM(ctx context.Context,
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

	eg := enterrors.NewErrorGroupWrapper(ob.shard.Index().logger)
	eg.SetLimit(_NUMCPU)

	// Read once and reuse: the class is invariant across the batch and
	// getClass() clones on every call.
	class := ob.shard.Index().getClass()

	for j, object := range batch {
		object := object
		index := j
		f := func() error {
			if ob.hasErrorAtIndex(index) {
				return nil
			}
			if err := ob.storeObjectOfBatchInLSM(ctx, class, index, object); err != nil {
				errLock.Lock()
				errs[index] = err
				errLock.Unlock()
			}
			return nil
		}
		eg.Go(f)

	}
	_ = eg.Wait() // no errors can happen here, this is just for concurrency control

	return errs
}

func (ob *objectsBatcher) storeObjectOfBatchInLSM(ctx context.Context,
	class *models.Class, objectIndex int, object *storobj.Object,
) error {
	if ob.hasErrorAtIndex(objectIndex) {
		return nil
	}

	if _, ok := ob.duplicates[objectIndex]; ok {
		return nil
	}

	// Reject batch items carrying a dropped vector before persisting anything.
	if len(object.Vectors) > 0 || len(object.MultiVectors) > 0 {
		if err := rejectDroppedObjectVectors(class, object); err != nil {
			return err
		}
	}

	uuidParsed, err := uuid.Parse(object.ID().String())
	if err != nil {
		return errors.Wrap(err, "invalid id")
	}

	idBytes, err := uuidParsed.MarshalBinary()
	if err != nil {
		return err
	}

	status, err := ob.shard.putObjectLSM(ctx, object, idBytes)
	if err != nil {
		return err
	}

	ob.setStatusForID(status, object.ID())

	if err := ctx.Err(); err != nil {
		return errors.Wrapf(err, "end store object %d of batch", objectIndex)
	}

	return nil
}

// setStatusForID is thread-safe as it uses the underlying mutex to lock the
// statuses map when writing into it
func (ob *objectsBatcher) setStatusForID(status objectInsertStatus, id strfmt.UUID) {
	ob.Lock()
	defer ob.Unlock()
	ob.statuses[id] = status
}

func (ob *objectsBatcher) markDeletedInVectorStorage(ctx context.Context) {
	var docIDsToDelete []uint64
	var positions []int
	for pos, object := range ob.objects {
		status := ob.statuses[object.ID()]
		if status.docIDChanged {
			docIDsToDelete = append(docIDsToDelete, status.oldDocID)
			positions = append(positions, pos)
		}
	}

	if len(docIDsToDelete) == 0 {
		return
	}

	_ = ob.shard.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err := queue.Delete(docIDsToDelete...); err != nil {
			for _, pos := range positions {
				ob.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), pos)
			}
		}
		return nil
	})

	_ = ob.shard.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err := queue.Delete(docIDsToDelete...); err != nil {
			for _, pos := range positions {
				ob.setErrorAtIndex(fmt.Errorf("geo prop %s: %w", propName, err), pos)
			}
		}
		return nil
	})
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
		status := ob.statuses[object.ID()]
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

	targetVectors := make(map[string][]common.VectorRecord)
	for i, object := range ob.objects {
		status := ob.statuses[object.ID()]

		if ob.shouldSkipInAdditionalStorage(i, status) {
			continue
		}

		if shouldGeoIndex {
			if err := ob.shard.updatePropertySpecificIndices(ctx, object, status); err != nil {
				ob.setErrorAtIndex(errors.Wrap(err, "update prop-specific indices"), i)
				continue
			}
		}

		// skip vector update, as vector was not changed
		// https://github.com/weaviate/weaviate/issues/3948
		if status.docIDPreserved {
			continue
		}

		if len(object.Vector) == 0 && len(object.Vectors) == 0 && len(object.MultiVectors) == 0 {
			continue
		}

		for targetVector, vector := range object.Vectors {
			targetVectors[targetVector] = append(targetVectors[targetVector], &common.Vector[[]float32]{
				ID:     status.docID,
				Vector: vector,
			})
		}
		for targetVector, vector := range object.MultiVectors {
			targetVectors[targetVector] = append(targetVectors[targetVector], &common.Vector[[][]float32]{
				ID:     status.docID,
				Vector: vector,
			})
		}

		if len(object.Vector) > 0 {
			// use empty string for legacy vector, downstream code will handle that appropriately
			targetVectors[""] = append(targetVectors[""], &common.Vector[[]float32]{
				ID:     status.docID,
				Vector: object.Vector,
			})
		}
	}

	for targetVector, vectors := range targetVectors {
		found, err := ob.shard.WithVectorIndexQueue(targetVector, func(queue *VectorIndexQueue) error {
			return queue.Insert(ctx, vectors...)
		})
		if !found {
			ob.setErrorAtIndex(fmt.Errorf("queue not found for target vector %s", targetVector), 0)
		} else if err != nil {
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
			entsentry.Recover(err)
			ob.setErrorAtIndex(fmt.Errorf("an unexpected error occurred: %s", err), index)
			fmt.Fprintf(os.Stderr, "panic: %s\n", err)
			enterrors.PrintStack(ob.logger)
		}
	}()

	if err := ctx.Err(); err != nil {
		ob.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
		return
	}

	if len(object.Vector) > 0 || len(object.Vectors) > 0 || len(object.MultiVectors) > 0 {
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
		if len(object.Vectors) > 0 {
			if err := ob.shard.updateVectorIndexesIgnoreDelete(ctx, object.Vectors, status); err != nil {
				ob.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
				return
			}
		}
		if len(object.MultiVectors) > 0 {
			if err := ob.shard.updateMultiVectorIndexesIgnoreDelete(ctx, object.MultiVectors, status); err != nil {
				ob.setErrorAtIndex(errors.Wrap(err, "insert to multi vector index"), index)
				return
			}
		}
		if len(object.Vector) > 0 {
			if err := ob.shard.updateVectorIndexIgnoreDelete(ctx, object.Vector, status); err != nil {
				ob.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
				return
			}
		}
	}

	if err := ob.shard.updatePropertySpecificIndices(ctx, object, status); err != nil {
		ob.setErrorAtIndex(errors.Wrap(err, "update prop-specific indices"), index)
		return
	}
}

// hasErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// before reading from the errs map
func (ob *objectsBatcher) hasErrorAtIndex(i int) bool {
	ob.Lock()
	defer ob.Unlock()
	return ob.errs[i] != nil
}

// setErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// writing into the errs map
func (ob *objectsBatcher) setErrorAtIndex(err error, index int) {
	ob.Lock()
	defer ob.Unlock()
	ob.errs[index] = err
}

// checkContext does nothing if the context is still active. But if the context
// has error'd, it marks all objects which have not previously error'd yet with
// the ctx error
func (ob *objectsBatcher) checkContext(ctx context.Context) bool {
	if err := ctx.Err(); err != nil {
		for i, err := range ob.errs {
			if err == nil {
				// already has an error, ignore
				continue
			}

			ob.errs[i] = errors.Wrapf(err,
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

	_ = ob.shard.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err := queue.Flush(); err != nil {
			for i := range ob.objects {
				ob.setErrorAtIndex(fmt.Errorf("target vector %s: %w", targetVector, err), i)
			}
		}
		return nil
	})

	_ = ob.shard.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err := queue.Flush(); err != nil {
			for i := range ob.objects {
				ob.setErrorAtIndex(fmt.Errorf("geo prop %s: %w", propName, err), i)
			}
		}
		return nil
	})

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

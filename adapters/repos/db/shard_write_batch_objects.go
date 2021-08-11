//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
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
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context,
	objects []*storobj.Object) []error {
	return newObjectsBatcher(s).Objects(ctx, objects)
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
}

func newObjectsBatcher(s *Shard) *objectsBatcher {
	return &objectsBatcher{shard: s}
}

// Objects imports the specified objects in parallel in a batch-fashion
func (b *objectsBatcher) Objects(ctx context.Context,
	objects []*storobj.Object) []error {
	beforeBatch := time.Now()
	defer b.shard.metrics.BatchObject(beforeBatch, len(objects))

	b.init(objects)
	b.storeInObjectStore(ctx)
	b.storeAdditionalStorage(ctx)
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
	batch []*storobj.Object) []error {
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
	objectIndex int, object *storobj.Object) error {
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

// storeAdditionalStorage stores the object in all non-key-value stores,
// such as the main vector index as well as the property-specific indices, such
// as the geo-index.
func (b *objectsBatcher) storeAdditionalStorage(ctx context.Context) {
	if ok := b.checkContext(ctx); !ok {
		// if the context is no longer OK, there's no point in continuing - abort
		// early
		return
	}

	beforeVectorIndex := time.Now()
	wg := &sync.WaitGroup{}
	for i, object := range b.objects {
		if b.shouldSkipInAdditionalStorage(i) {
			continue
		}

		wg.Add(1)
		status := b.statuses[object.ID()]
		go func(object *storobj.Object, status objectInsertStatus, index int) {
			defer wg.Done()
			b.storeSingleObjectInAdditionalStorage(ctx, object, status, index)
		}(object, status, i)
	}
	wg.Wait()
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
	object *storobj.Object, status objectInsertStatus, index int) {
	if err := ctx.Err(); err != nil {
		b.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
		return
	}

	if err := b.shard.updateVectorIndex(object.Vector, status); err != nil {
		b.setErrorAtIndex(errors.Wrap(err, "insert to vector index"), index)
		return
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

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context,
	objects []*storobj.Object) map[int]error {
	return newObjectsBatcher(s).Objects(ctx, objects)
}

// objectsBatcher is a helper type wrapping around an underlying shard that can
// execute objects batch operations on a shard (as opposed to references batch
// operations)
type objectsBatcher struct {
	sync.Mutex
	shard      *Shard
	statuses   map[strfmt.UUID]objectInsertStatus
	errs       map[int]error
	duplicates map[int]struct{}
	objects    []*storobj.Object
}

func newObjectsBatcher(s *Shard) *objectsBatcher {
	return &objectsBatcher{shard: s}
}

// Objects imports the specified objects in parallel in a batch-fashion
func (b *objectsBatcher) Objects(ctx context.Context,
	objects []*storobj.Object) map[int]error {
	beforeBatch := time.Now()
	defer b.shard.metrics.BatchObject(beforeBatch, len(objects))

	b.init(objects)
	b.storeObjectsInObjectStore(ctx)
	b.storeObjectsAdditionalStorage(ctx)
	return b.errs
}

func (b *objectsBatcher) init(objects []*storobj.Object) {
	b.objects = objects
	b.statuses = map[strfmt.UUID]objectInsertStatus{}
	b.errs = map[int]error{} // int represents original index
	b.duplicates = findDuplicatesInBatchObjects(objects)
}

// storeObjectsInObjectStore performs all storage operations on the underlying
// key/value store, this is they object-by-id store, the docID-lookup tables,
// as well as all inverted indices.
func (b *objectsBatcher) storeObjectsInObjectStore(ctx context.Context) {
	maxPerTransaction := 30
	beforeObjectStore := time.Now()
	wg := &sync.WaitGroup{}
	for i := 0; i < len(b.objects); i += maxPerTransaction {
		end := i + maxPerTransaction
		if end > len(b.objects) {
			end = len(b.objects)
		}

		batch := b.objects[i:end]
		wg.Add(1)
		go func(i int, batch []*storobj.Object) {
			defer wg.Done()
			var affectedIndices []int
			if err := b.shard.db.Batch(func(tx *bolt.Tx) error {
				var err error
				affectedIndices, err = b.storeSingleBatchInTx(ctx, tx, i, batch)
				return err
			}); err != nil {
				b.setErrorsForIndices(err, affectedIndices)
			}
		}(i, batch)
	}
	wg.Wait()
	b.shard.metrics.ObjectStore(beforeObjectStore)
}

func (b *objectsBatcher) setErrorsForIndices(err error, affectedIndices []int) {
	b.Lock()
	err = errors.Wrap(err, "bolt batch tx")
	for _, affected := range affectedIndices {
		b.errs[affected] = err
	}
	b.Unlock()
}

func (b *objectsBatcher) storeSingleBatchInTx(ctx context.Context, tx *bolt.Tx,
	batchId int, batch []*storobj.Object) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrapf(err, "begin transaction %d of batch", batchId)
	}

	var affectedIndices []int

	for j := range batch {
		// so we can reference potential errors
		affectedIndices = append(affectedIndices, batchId+j)
	}

	for j, object := range batch {
		if err := b.storeObjectOfBatchInTx(ctx, tx, batchId, j, object); err != nil {
			return nil, errors.Wrapf(err, "object %d", j)
		}
	}
	return affectedIndices, nil
}

func (b *objectsBatcher) storeObjectOfBatchInTx(ctx context.Context, tx *bolt.Tx,
	batchId int, objectIndex int, object *storobj.Object) error {
	if _, ok := b.duplicates[batchId+objectIndex]; ok {
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

	status, err := b.shard.putObjectInTx(tx, object, idBytes)
	if err != nil {
		return err
	}

	b.setStatusForID(status, object.ID())

	if err := ctx.Err(); err != nil {
		return errors.Wrapf(err, "end transaction %d of batch", batchId)
	}
	return nil
}

// setStatusForID is thread-safe as it uses the underlying mutex to lock the
// statuses map when writing into it
func (b *objectsBatcher) setStatusForID(status objectInsertStatus, id strfmt.UUID) {
	b.Lock()
	b.statuses[id] = status
	b.Unlock()
}

// storeObjectsAdditionalStorage stores the object in all non-key-value stores,
// such as the main vector index as well as the property-specific indices, such
// as the geo-index.
func (b *objectsBatcher) storeObjectsAdditionalStorage(ctx context.Context) {
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
	b.Lock()
	_, ok := b.errs[i]
	b.Unlock()
	if ok {
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

// setErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// writing into the errs map
func (b *objectsBatcher) setErrorAtIndex(err error, index int) {
	b.Lock()
	b.errs[index] = err
	b.Unlock()
}

// checkContext does nothing if the context is still active, but marks all
// objects which have not previously error'd yet with the ctx error
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

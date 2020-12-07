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
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) addReferencesBatch(ctx context.Context,
	refs kinds.BatchReferences) map[int]error {
	return newReferencesBatcher(s).References(ctx, refs)
}

// referencesBatcher is a helper type wrapping around an underlying shard that can
// execute references batch operations on a shard (as opposed to object batch
// operations)
type referencesBatcher struct {
	sync.Mutex
	shard                    *Shard
	errs                     map[int]error
	refs                     kinds.BatchReferences
	additionalStorageUpdates map[uint64]additionalStorageUpdate // by docID
}

// additionalStorageUpdate is a helper type to group the results of a merge, so
// that secondary index updates - if required - can be performed on those
type additionalStorageUpdate struct {
	obj    *storobj.Object
	status objectInsertStatus
	index  int
}

func newReferencesBatcher(s *Shard) *referencesBatcher {
	return &referencesBatcher{
		shard:                    s,
		additionalStorageUpdates: map[uint64]additionalStorageUpdate{},
	}
}

func (b *referencesBatcher) References(ctx context.Context,
	refs kinds.BatchReferences) map[int]error {
	b.init(refs)
	b.storeInObjectStore(ctx)
	b.storeAdditionalStorage(ctx)
	return b.errs
}

func (b *referencesBatcher) init(refs kinds.BatchReferences) {
	b.refs = refs
	b.errs = map[int]error{} // int represents original index
}

func (b *referencesBatcher) storeInObjectStore(
	ctx context.Context) {
	maxPerTransaction := 30

	wg := &sync.WaitGroup{}
	for i := 0; i < len(b.refs); i += maxPerTransaction {
		end := i + maxPerTransaction
		if end > len(b.refs) {
			end = len(b.refs)
		}

		batch := b.refs[i:end]
		wg.Add(1)
		go func(i int, batch kinds.BatchReferences) {
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

	// adding references can not alter the vector position, so no need to alter
	// the vector index
}

func (b *referencesBatcher) storeSingleBatchInTx(ctx context.Context, tx *bolt.Tx,
	batchId int, batch kinds.BatchReferences) ([]int, error) {
	var affectedIndices []int
	for i := range batch {
		// so we can reference potential errors
		affectedIndices = append(affectedIndices, batchId+i)
	}

	for i, ref := range batch {
		uuidParsed, err := uuid.Parse(ref.From.TargetID.String())
		if err != nil {
			return nil, errors.Wrap(err, "invalid id")
		}

		idBytes, err := uuidParsed.MarshalBinary()
		if err != nil {
			return nil, err
		}

		mergeDoc := mergeDocFromBatchReference(ref)
		n, s, err := b.shard.mergeObjectInTx(tx, mergeDoc, idBytes)
		if err != nil {
			return nil, err
		}

		b.addAdditionalStorageUpdate(n, s, batchId+i)
	}

	return affectedIndices, nil
}

// storeAdditionalStorage stores the object in all non-key-value stores,
// such as the main vector index as well as the property-specific indices, such
// as the geo-index.
func (b *referencesBatcher) storeAdditionalStorage(ctx context.Context) {
	if ok := b.checkContext(ctx); !ok {
		// if the context is no longer OK, there's no point in continuing - abort
		// early
		return
	}

	before := time.Now()
	wg := &sync.WaitGroup{}
	for _, update := range b.additionalStorageUpdates {
		wg.Add(1)
		go func(object *storobj.Object, status objectInsertStatus, index int) {
			defer wg.Done()
			b.storeSingleObjectInAdditionalStorage(ctx, object, status, index)
		}(update.obj, update.status, update.index)
	}
	wg.Wait()
	b.shard.metrics.VectorIndex(before)
}

func (b *referencesBatcher) storeSingleObjectInAdditionalStorage(ctx context.Context,
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

func (b *referencesBatcher) addAdditionalStorageUpdate(obj *storobj.Object,
	status objectInsertStatus, originalIndex int) {
	b.Lock()
	defer b.Unlock()

	if status.docIDChanged {
		// If we've already seen the docID that is now considered "old" before , we
		// need to explicitly delete this. Why? Imagine several updates on the same
		// source object which originally had the docID 1. After a few updates, it
		// might now have the docID 4 (as we have 3 updates: 1->2, 2->3, 3->4).
		//
		// We must now make sure that docIDs 2 and 3 (1 was never on our list in
		// the first place) are removed from our additional-storage todo list. This
		// is for two reasons: (1) The updates are pointless, if we already know
		// they'll be removed later. (2) Additional index updates will happen
		// concurrently, so we cannot guarantee the order. It might be that doc ID
		// 3 would be deleted before it ever gets added, which would be highly
		// problematic on indices such as HNSW, where doc IDs must also be
		// immutable.

		oldDocID := status.oldDocID
		if previousUpdate, ok := b.additionalStorageUpdates[oldDocID]; ok {
			delete(b.additionalStorageUpdates, status.oldDocID)

			// in addition to deleting the intermediary update, we must also use
			// their old id as ours. As outlined above the original doc ID before the
			// batch update (in the example doc ID 1) is not part of the batch.
			// However, the first udpate (1->2) would have taken care of deleting
			// this outdated id. Since we have just deleted this update, we must now
			// set our own oldID to 1. Essentially, at the end of the batch we have
			// then merged 1->2->3->4 to 1->4 instead of simply cutting off the
			// beginning and ending up with 3->4. With the latter we'd try to delete
			// a non-existent id (3) and would leave the obsolete id 1 unchanged.
			status.oldDocID = previousUpdate.status.oldDocID
		}
	}

	b.additionalStorageUpdates[status.docID] = additionalStorageUpdate{
		obj:    obj,
		status: status,
		index:  originalIndex,
	}
}

func (b *referencesBatcher) setErrorsForIndices(err error, affectedIndices []int) {
	b.Lock()
	defer b.Unlock()

	err = errors.Wrap(err, "bolt batch tx")
	for _, affected := range affectedIndices {
		b.errs[affected] = err
	}
}

// setErrorAtIndex is thread-safe as it uses the underlying mutex to lock
// writing into the errs map
func (b *referencesBatcher) setErrorAtIndex(err error, index int) {
	b.Lock()
	defer b.Unlock()
	b.errs[index] = err
}

func mergeDocFromBatchReference(ref kinds.BatchReference) kinds.MergeDocument {
	return kinds.MergeDocument{
		Kind:       ref.From.Kind,
		Class:      ref.From.Class.String(),
		ID:         ref.From.TargetID,
		UpdateTime: time.Now().UnixNano(),
		References: kinds.BatchReferences{ref},
	}
}

// checkContext does nothing if the context is still active. But if the context
// has error'd, it marks all objects which have not previously error'd yet with
// the ctx error
func (s *referencesBatcher) checkContext(ctx context.Context) bool {
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

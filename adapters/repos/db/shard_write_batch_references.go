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

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) addReferencesBatch(ctx context.Context,
	refs objects.BatchReferences) []error {
	return newReferencesBatcher(s).References(ctx, refs)
}

// referencesBatcher is a helper type wrapping around an underlying shard that can
// execute references batch operations on a shard (as opposed to object batch
// operations)
type referencesBatcher struct {
	sync.Mutex
	shard *Shard
	errs  []error
	refs  objects.BatchReferences
}

func newReferencesBatcher(s *Shard) *referencesBatcher {
	return &referencesBatcher{
		shard: s,
	}
}

func (b *referencesBatcher) References(ctx context.Context,
	refs objects.BatchReferences) []error {
	b.init(refs)
	b.storeInObjectStore(ctx)
	b.flushWALs(ctx)
	return b.errs
}

func (b *referencesBatcher) init(refs objects.BatchReferences) {
	b.refs = refs
	b.errs = []error{}
}

func (b *referencesBatcher) storeInObjectStore(
	ctx context.Context) {
	errs := b.storeSingleBatchInLSM(ctx, b.refs)
	for i, err := range errs {
		if err != nil {
			b.setErrorAtIndex(err, i)
		}
	}

	// adding references can not alter the vector position, so no need to alter
	// the vector index
}

func (b *referencesBatcher) storeSingleBatchInLSM(ctx context.Context,
	batch objects.BatchReferences) []error {
	errs := make([]error, len(batch))
	errLock := &sync.Mutex{}

	// if the context is expired fail all
	if err := ctx.Err(); err != nil {
		for i := range errs {
			errs[i] = errors.Wrap(err, "begin batch")
		}
		return errs
	}

	invertedMerger := inverted.NewDeltaMerger()

	// TODO: is there any benefit in having this parallelized? if so, don't forget to lock before assigning errors
	// If we want them to run in parallel we need to look individual objects,
	// otherwise we have a race inside the merge functions
	// wg := &sync.WaitGroup{}
	for i, ref := range batch {
		// wg.Add(1)
		// go func(index int, reference objects.BatchReference) {
		// 	defer wg.Done()
		uuidParsed, err := uuid.Parse(ref.From.TargetID.String())
		if err != nil {
			errLock.Lock()
			errs[i] = errors.Wrap(err, "invalid id")
			errLock.Unlock()
		}

		idBytes, err := uuidParsed.MarshalBinary()
		if err != nil {
			errLock.Lock()
			errs[i] = err
			errLock.Unlock()
		}

		mergeDoc := mergeDocFromBatchReference(ref)
		res, err := b.shard.mutableMergeObjectLSM(mergeDoc, idBytes)
		if err != nil {
			errLock.Lock()
			errs[i] = err
			errLock.Unlock()
		}

		_ = res

		// generally the batch ref is an append only change which does not alter
		// the vector position. There is however one inverted index link that needs
		// to be cleanup: the ref count
		if err := b.analyzeInverted(invertedMerger, res, ref); err != nil {
			if err != nil {
				errLock.Lock()
				errs[i] = err
				errLock.Unlock()
			}
		}
	}

	if err := b.writeInverted(invertedMerger.Merge()); err != nil {
		for i := range errs {
			errs[i] = errors.Wrap(err, "write inverted batch")
		}
		return errs
	}

	return errs
}

func (b *referencesBatcher) analyzeInverted(
	invertedMerger *inverted.DeltaMerger, mergeResult mutableMergeResult,
	ref objects.BatchReference) error {
	prevProps, err := b.analyzeRef(mergeResult.previous, ref)
	if err != nil {
		return err
	}

	nextProps, err := b.analyzeRef(mergeResult.next, ref)
	if err != nil {
		return err
	}

	delta := inverted.Delta(prevProps, nextProps)
	invertedMerger.AddAdditions(delta.ToAdd, mergeResult.status.docID)
	invertedMerger.AddDeletions(delta.ToDelete, mergeResult.status.docID)

	return nil
}

func (b *referencesBatcher) writeInverted(in inverted.DeltaMergeResult) error {
	before := time.Now()
	if err := b.writeInvertedAdditions(in.Additions); err != nil {
		return errors.Wrap(err, "write additions")
	}
	b.shard.metrics.InvertedExtend(before, len(in.Additions))

	before = time.Now()
	if err := b.writeInvertedDeletions(in.Deletions); err != nil {
		return errors.Wrap(err, "write deletions")
	}
	b.shard.metrics.InvertedDeleteDelta(before)

	return nil
}

func (b *referencesBatcher) writeInvertedDeletions(
	in []inverted.MergeProperty) error {
	// in the references batcher we can only ever write ref count entires which
	// are guaranteed to be not have a frequency, meaning they will use the
	// "Set" strategy in the lsmkv store
	for _, prop := range in {
		bucket := b.shard.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
		if bucket == nil {
			return errors.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		hashBucket := b.shard.store.Bucket(helpers.HashBucketFromPropNameLSM(prop.Name))
		if hashBucket == nil {
			return errors.Errorf("no hash bucket for prop '%s' found", prop.Name)
		}

		for _, item := range prop.MergeItems {
			for _, id := range item.DocIDs {
				err := b.shard.deleteInvertedIndexItemLSM(bucket, hashBucket,
					inverted.Countable{Data: item.Data}, id.DocID)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *referencesBatcher) writeInvertedAdditions(
	in []inverted.MergeProperty) error {
	// in the references batcher we can only ever write ref count entires which
	// are guaranteed to be not have a frequency, meaning they will use the
	// "Set" strategy in the lsmkv store
	for _, prop := range in {
		bucket := b.shard.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
		if bucket == nil {
			return errors.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		hashBucket := b.shard.store.Bucket(helpers.HashBucketFromPropNameLSM(prop.Name))
		if hashBucket == nil {
			return errors.Errorf("no hash bucket for prop '%s' found", prop.Name)
		}

		for _, item := range prop.MergeItems {
			err := b.shard.batchExtendInvertedIndexItemsLSMNoFrequency(bucket, hashBucket,
				item)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *referencesBatcher) analyzeRef(obj *storobj.Object,
	ref objects.BatchReference) ([]inverted.Property, error) {
	props := obj.Properties()
	if props == nil {
		return nil, nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	var refs models.MultipleRef
	refProp, ok := propMap[ref.From.Property.String()]
	if !ok {
		refs = make(models.MultipleRef, 0) // explicitly mark as length zero
	} else {
		parsed, ok := refProp.(models.MultipleRef)
		if !ok {
			return nil, errors.Errorf("prop %s is present, but not a ref, got: %T",
				ref.From.Property.String(), refProp)
		}
		refs = parsed
	}

	a := inverted.NewAnalyzer(nil)

	countItems, err := a.RefCount(refs)
	if err != nil {
		return nil, err
	}

	valueItems, err := a.Ref(refs)
	if err != nil {
		return nil, err
	}

	return []inverted.Property{{
		Name:         helpers.MetaCountProp(ref.From.Property.String()),
		Items:        countItems,
		HasFrequency: false,
	}, {
		Name:         ref.From.Property.String(),
		Items:        valueItems,
		HasFrequency: false,
	}}, nil
}

func (b *referencesBatcher) setErrorAtIndex(err error, i int) {
	b.Lock()
	defer b.Unlock()

	err = errors.Wrap(err, "ref batch")
	b.errs[i] = err
}

func mergeDocFromBatchReference(ref objects.BatchReference) objects.MergeDocument {
	return objects.MergeDocument{
		Class:      ref.From.Class.String(),
		ID:         ref.From.TargetID,
		UpdateTime: time.Now().UnixNano(),
		References: objects.BatchReferences{ref},
	}
}

func (b *referencesBatcher) flushWALs(ctx context.Context) {
	if err := b.shard.store.WriteWALs(); err != nil {
		for i := range b.refs {
			b.setErrorAtIndex(err, i)
		}
	}

	if err := b.shard.vectorIndex.Flush(); err != nil {
		for i := range b.refs {
			b.setErrorAtIndex(err, i)
		}
	}
}

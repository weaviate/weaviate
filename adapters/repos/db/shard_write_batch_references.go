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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error {
	if s.isReadOnly() {
		return []error{errors.Errorf("shard is read-only")}
	}

	return newReferencesBatcher(s).References(ctx, refs)
}

// referencesBatcher is a helper type wrapping around an underlying shard that can
// execute references batch operations on a shard (as opposed to object batch
// operations)
type referencesBatcher struct {
	sync.Mutex
	shard ShardLike
	errs  []error
	refs  objects.BatchReferences
}

func newReferencesBatcher(s ShardLike) *referencesBatcher {
	return &referencesBatcher{
		shard: s,
	}
}

func (b *referencesBatcher) References(ctx context.Context,
	refs objects.BatchReferences,
) []error {
	b.init(refs)
	b.storeInObjectStore(ctx)
	b.flushWALs(ctx)
	return b.errs
}

func (b *referencesBatcher) init(refs objects.BatchReferences) {
	b.refs = refs
	b.errs = make([]error, len(refs))
}

func (b *referencesBatcher) storeInObjectStore(
	ctx context.Context,
) {
	errs := b.storeSingleBatchInLSM(ctx, b.refs)
	for i, err := range errs {
		if err != nil {
			b.setErrorAtIndex(err, i)
		}
	}

	// adding references can not alter the vector position, so no need to alter
	// the vector index
}

func (b *referencesBatcher) storeSingleBatchInLSM(ctx context.Context, batch objects.BatchReferences) []error {
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
	propsByName, err := b.getSchemaPropsByName()
	if err != nil {
		for i := range errs {
			errs[i] = errors.Wrap(err, "getting schema properties")
		}
		return errs
	}

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
			continue
		}

		idBytes, err := uuidParsed.MarshalBinary()
		if err != nil {
			errLock.Lock()
			errs[i] = err
			errLock.Unlock()
			continue
		}

		mergeDoc := mergeDocFromBatchReference(ref)
		res, err := b.shard.mutableMergeObjectLSM(mergeDoc, idBytes)
		if err != nil {
			errLock.Lock()
			errs[i] = err
			errLock.Unlock()
			continue
		}

		prop, ok := propsByName[ref.From.Property.String()]
		if !ok {
			errLock.Lock()
			errs[i] = fmt.Errorf("property '%s' not found in schema", ref.From.Property)
			errLock.Unlock()
			continue
		}

		// generally the batch ref is an append only change which does not alter
		// the vector position. There is however one inverted index link that needs
		// to be cleanup: the ref count
		if err := b.analyzeInverted(invertedMerger, res, ref, prop); err != nil {
			errLock.Lock()
			errs[i] = err
			errLock.Unlock()
			continue
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

func (b *referencesBatcher) analyzeInverted(invertedMerger *inverted.DeltaMerger, mergeResult mutableMergeResult, ref objects.BatchReference, prop *models.Property) error {
	prevProps, err := b.analyzeRef(mergeResult.previous, ref, prop)
	if err != nil {
		return err
	}

	nextProps, err := b.analyzeRef(mergeResult.next, ref, prop)
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
	b.shard.Metrics().InvertedExtend(before, len(in.Additions))

	before = time.Now()
	if err := b.writeInvertedDeletions(in.Deletions); err != nil {
		return errors.Wrap(err, "write deletions")
	}
	b.shard.Metrics().InvertedDeleteDelta(before)

	return nil
}

// TODO text_rbm_inverted_index unify bucket write
func (b *referencesBatcher) writeInvertedDeletions(in []inverted.MergeProperty) error {
	for _, prop := range in {
		// in the references batcher we can only ever write ref count entire which
		// are guaranteed to be not have a frequency, meaning they will use the
		// "Set" strategy in the lsmkv store
		if prop.HasFilterableIndex {
			bucket := b.shard.Store().Bucket(helpers.BucketFromPropNameLSM(prop.Name))
			if bucket == nil {
				return errors.Errorf("no bucket for prop '%s' found", prop.Name)
			}

			for _, item := range prop.MergeItems {
				for _, id := range item.DocIDs {
					err := b.shard.deleteInvertedIndexItemLSM(bucket,
						inverted.Countable{Data: item.Data}, id.DocID)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// TODO text_rbm_inverted_index unify bucket write
func (b *referencesBatcher) writeInvertedAdditions(in []inverted.MergeProperty) error {
	for _, prop := range in {
		// in the references batcher we can only ever write ref count entire which
		// are guaranteed to be not have a frequency, meaning they will use the
		// "Set" strategy in the lsmkv store
		if prop.HasFilterableIndex {
			bucket := b.shard.Store().Bucket(helpers.BucketFromPropNameLSM(prop.Name))
			if bucket == nil {
				return errors.Errorf("no bucket for prop '%s' found", prop.Name)
			}

			for _, item := range prop.MergeItems {
				err := b.shard.batchExtendInvertedIndexItemsLSMNoFrequency(bucket, item)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *referencesBatcher) analyzeRef(obj *storobj.Object, ref objects.BatchReference, prop *models.Property) ([]inverted.Property, error) {
	if prop == nil {
		return nil, fmt.Errorf("analyzeRef: property %q not found in schema", ref.From.Property)
	}

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
		Name:               helpers.MetaCountProp(ref.From.Property.String()),
		Items:              countItems,
		HasFilterableIndex: inverted.HasFilterableIndexMetaCount && inverted.HasInvertedIndex(prop),
		HasSearchableIndex: inverted.HasSearchableIndexMetaCount && inverted.HasInvertedIndex(prop),
	}, {
		Name:               ref.From.Property.String(),
		Items:              valueItems,
		HasFilterableIndex: inverted.HasFilterableIndex(prop),
		HasSearchableIndex: inverted.HasSearchableIndex(prop),
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
		UpdateTime: time.Now().UnixMilli(),
		References: objects.BatchReferences{ref},
	}
}

func (b *referencesBatcher) flushWALs(ctx context.Context) {
	if err := b.shard.Store().WriteWALs(); err != nil {
		for i := range b.refs {
			b.setErrorAtIndex(err, i)
		}
	}

	if err := b.shard.VectorIndex().Flush(); err != nil {
		for i := range b.refs {
			b.setErrorAtIndex(err, i)
		}
	}
}

func (b *referencesBatcher) getSchemaPropsByName() (map[string]*models.Property, error) {
	idx := b.shard.Index()
	sch := idx.getSchema.GetSchemaSkipAuth().Objects
	class, err := schema.GetClassByName(sch, idx.Config.ClassName.String())
	if err != nil {
		return nil, err
	}

	propsByName := map[string]*models.Property{}
	for _, prop := range class.Properties {
		propsByName[prop.Name] = prop
	}
	return propsByName, nil
}

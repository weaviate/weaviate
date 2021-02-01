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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/objects"
	bolt "go.etcd.io/bbolt"
)

// return value map[int]error gives the error for the index as it received it
func (s *Shard) addReferencesBatch(ctx context.Context,
	refs objects.BatchReferences) map[int]error {
	return newReferencesBatcher(s).References(ctx, refs)
}

// referencesBatcher is a helper type wrapping around an underlying shard that can
// execute references batch operations on a shard (as opposed to object batch
// operations)
type referencesBatcher struct {
	sync.Mutex
	shard *Shard
	errs  map[int]error
	refs  objects.BatchReferences
}

func newReferencesBatcher(s *Shard) *referencesBatcher {
	return &referencesBatcher{
		shard: s,
	}
}

func (b *referencesBatcher) References(ctx context.Context,
	refs objects.BatchReferences) map[int]error {
	b.init(refs)
	b.storeInObjectStore(ctx)
	return b.errs
}

func (b *referencesBatcher) init(refs objects.BatchReferences) {
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
		go func(i int, batch objects.BatchReferences) {
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
	batchId int, batch objects.BatchReferences) ([]int, error) {
	var affectedIndices []int

	for i := range batch {
		// so we can reference potential errors
		affectedIndices = append(affectedIndices, batchId+i)
	}

	for _, ref := range batch {
		uuidParsed, err := uuid.Parse(ref.From.TargetID.String())
		if err != nil {
			return nil, errors.Wrap(err, "invalid id")
		}

		idBytes, err := uuidParsed.MarshalBinary()
		if err != nil {
			return nil, err
		}

		mergeDoc := mergeDocFromBatchReference(ref)
		res, err := b.shard.mutableMergeObjectInTx(tx, mergeDoc, idBytes)
		if err != nil {
			return nil, err
		}

		// generally the batch ref is an append only change which does not alter
		// the vector position. There is however one inverted index link that needs
		// to be cleanup: the ref count
		if err := b.updateInverted(tx, res, ref); err != nil {
			return nil, errors.Wrap(err, "determine ref count cleanup")
		}
	}

	return affectedIndices, nil
}

// TODO: batch instead of doing those one by one
func (b *referencesBatcher) updateInverted(tx *bolt.Tx,
	mergeResult mutableMergeResult, ref objects.BatchReference) error {
	prevProps, err := b.analyzeRefCount(mergeResult.previous, ref)
	if err != nil {
		return err
	}

	nextProps, err := b.analyzeRefCount(mergeResult.next, ref)
	if err != nil {
		return err
	}

	delta := inverted.Delta(prevProps, nextProps)

	before := time.Now()
	err = b.shard.extendInvertedIndices(tx, delta.ToAdd, mergeResult.status.docID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
	}
	b.shard.metrics.InvertedExtend(before, len(delta.ToAdd))

	return b.deleteProps(tx, delta.ToDelete, mergeResult.status.docID)
}

func (b *referencesBatcher) deleteProps(tx *bolt.Tx,
	props []inverted.Property, docID uint64) error {
	for _, prop := range props {
		bucket := tx.Bucket(helpers.BucketFromPropName(prop.Name))
		if bucket == nil {
			return errors.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		for _, item := range prop.Items {
			err := b.shard.tryDeleteFromInvertedIndicesProp(bucket, item, []uint64{docID}, false)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *referencesBatcher) analyzeRefCount(obj *storobj.Object,
	ref objects.BatchReference) ([]inverted.Property, error) {
	props := obj.Properties()
	if props == nil {
		fmt.Printf("exiting because props nil\n")
		return nil, nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		fmt.Printf("exiting because props not a map\n")
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

	items, err := inverted.NewAnalyzer().RefCount(refs)
	if err != nil {
		return nil, err
	}

	return []inverted.Property{{
		Name:         helpers.MetaCountProp(ref.From.Property.String()),
		Items:        items,
		HasFrequency: false,
	}}, nil
}

func (b *referencesBatcher) setErrorsForIndices(err error, affectedIndices []int) {
	b.Lock()
	defer b.Unlock()

	err = errors.Wrap(err, "bolt batch tx")
	for _, affected := range affectedIndices {
		b.errs[affected] = err
	}
}

func mergeDocFromBatchReference(ref objects.BatchReference) objects.MergeDocument {
	return objects.MergeDocument{
		Class:      ref.From.Class.String(),
		ID:         ref.From.TargetID,
		UpdateTime: time.Now().UnixNano(),
		References: objects.BatchReferences{ref},
	}
}

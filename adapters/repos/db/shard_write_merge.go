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

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

func (s *Shard) mergeObject(ctx context.Context, merge objects.MergeDocument) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	idBytes, err := uuid.MustParse(merge.ID.String()).MarshalBinary()
	if err != nil {
		return err
	}

	next, status, err := s.mergeObjectInStorage(merge, idBytes)
	if err != nil {
		return err
	}

	if err := s.updateVectorIndex(next.Vector, status); err != nil {
		return errors.Wrap(err, "update vector index")
	}

	if err := s.updatePropertySpecificIndices(next, status); err != nil {
		return errors.Wrap(err, "update property-specific indices")
	}

	if err := s.store.WriteWALs(); err != nil {
		return errors.Wrap(err, "flush all buffered WALs")
	}

	if err := s.vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush all vector index buffered WALs")
	}

	return nil
}

func (s *Shard) mergeObjectInStorage(merge objects.MergeDocument,
	idBytes []byte,
) (*storobj.Object, objectInsertStatus, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	previous, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return nil, objectInsertStatus{}, errors.Wrap(err, "get bucket")
	}

	nextObj, _, err := s.mergeObjectData(previous, merge)
	if err != nil {
		return nil, objectInsertStatus{}, errors.Wrap(err, "merge object data")
	}

	status, err := s.determineInsertStatus(previous, nextObj)
	if err != nil {
		return nil, status, errors.Wrap(err, "check insert/update status")
	}

	nextObj.SetDocID(status.docID)
	nextBytes, err := nextObj.MarshalBinary()
	if err != nil {
		return nil, status, errors.Wrapf(err, "marshal object %s to binary", nextObj.ID())
	}

	if err := s.upsertObjectDataLSM(bucket, idBytes, nextBytes, status.docID); err != nil {
		return nil, status, errors.Wrap(err, "upsert object data")
	}

	if err := s.updateInvertedIndexLSM(nextObj, status, previous); err != nil {
		return nil, status, errors.Wrap(err, "update inverted indices")
	}

	return nextObj, status, nil
}

// mutableMergeObjectLSM is a special version of mergeObjectInTx where no doc
// id increases will be made, but instead the old doc ID will be re-used. This
// is only possible if the following two conditions are met:
//
//  1. We only add to the inverted index, but there is nothing which requires
//     cleaning up. Example `name: "John"` is updated to `name: "John Doe"`,
//     this is valid because we only add new entry for "Doe", but do not alter
//     the existing entry for "John"
//     An invalid update would be `name:"John"` is updated to `name:"Diane"`,
//     this would require a cleanup for the existing link from "John" to this
//     doc id, which is not possible. The only way to clean up is to increase
//     the doc id and delete all entries for the old one
//
//  2. The vector position is not altered. Vector Indices cannot be mutated
//     therefore a vector update would not be reflected
//
// The above makes this a perfect candidate for a batch reference update as
// this alters neither the vector position, nor does it remove anything from
// the inverted index
func (s *Shard) mutableMergeObjectLSM(merge objects.MergeDocument,
	idBytes []byte,
) (mutableMergeResult, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	out := mutableMergeResult{}

	previous, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return out, err
	}

	nextObj, previousObj, err := s.mergeObjectData(previous, merge)
	if err != nil {
		return out, errors.Wrap(err, "merge object data")
	}

	out.next = nextObj
	out.previous = previousObj

	status, err := s.determineMutableInsertStatus(previous, nextObj)
	if err != nil {
		return out, errors.Wrap(err, "check insert/update status")
	}
	out.status = status

	nextObj.SetDocID(status.docID) // is not changed
	nextBytes, err := nextObj.MarshalBinary()
	if err != nil {
		return out, errors.Wrapf(err, "marshal object %s to binary", nextObj.ID())
	}

	if err := s.upsertObjectDataLSM(bucket, idBytes, nextBytes, status.docID); err != nil {
		return out, errors.Wrap(err, "upsert object data")
	}

	// do not updated inverted index, since this requires delta analysis, which
	// must be done by the caller!

	return out, nil
}

type mutableMergeResult struct {
	next     *storobj.Object
	previous *storobj.Object
	status   objectInsertStatus
}

func (s *Shard) mergeObjectData(previous []byte,
	merge objects.MergeDocument,
) (*storobj.Object, *storobj.Object, error) {
	var previousObj *storobj.Object
	if len(previous) == 0 {
		// DocID must be overwritten after status check, simply set to initial
		// value
		previousObj = storobj.New(0)
		previousObj.SetClass(merge.Class)
		previousObj.SetID(merge.ID)
	} else {
		p, err := storobj.FromBinary(previous)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unmarshal previous")
		}

		previousObj = p
	}

	return mergeProps(previousObj, merge), previousObj, nil
}

func mergeProps(previous *storobj.Object,
	merge objects.MergeDocument,
) *storobj.Object {
	next := previous.DeepCopyDangerous()
	properties, ok := next.Properties().(map[string]interface{})
	if !ok || properties == nil {
		properties = map[string]interface{}{}
	}

	for propName, value := range merge.PrimitiveSchema {
		// for primtive props, we simply need to overwrite
		properties[propName] = value
	}

	for _, ref := range merge.References {
		propName := ref.From.Property.String()
		prop := properties[propName]
		propParsed, ok := prop.(models.MultipleRef)
		if !ok {
			propParsed = models.MultipleRef{}
		}
		propParsed = append(propParsed, ref.To.SingleRef())
		properties[propName] = propParsed
	}

	if merge.Vector == nil {
		next.Vector = previous.Vector
	} else {
		next.Vector = merge.Vector
	}

	next.Object.LastUpdateTimeUnix = merge.UpdateTime
	next.SetProperties(properties)

	return next
}

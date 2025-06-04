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

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var errObjectNotFound = errors.New("object not found")

func (s *Shard) MergeObject(ctx context.Context, merge objects.MergeDocument) error {
	s.activityTrackerWrite.Add(1)
	if err := s.isReadOnly(); err != nil {
		return err
	}

	for targetVector, vector := range merge.Vectors {
		// validation needs to happen before any changes are done. Otherwise, insertion is aborted somewhere in-between.
		vectorIndex, ok := s.GetVectorIndex(targetVector)
		if !ok {
			return errors.Errorf("validate vector index for update of %v for target vector %s: vector index not found", merge.ID, targetVector)
		}
		switch v := vector.(type) {
		case []float32:
			err := vectorIndex.ValidateBeforeInsert(v)
			if err != nil {
				return errors.Wrapf(err, "validate vector index for update of %v for target vector %s", merge.ID, targetVector)
			}
		case [][]float32:
			err := vectorIndex.ValidateMultiBeforeInsert(v)
			if err != nil {
				return errors.Wrapf(err, "validate multi vector index for update of %v for target vector %s", merge.ID, targetVector)
			}
		default:
			return errors.Errorf("validate vector index for update of %v for target vector %s: unrecongnized vector type: %T", merge.ID, targetVector, vector)
		}
	}

	if len(merge.Vector) > 0 {
		vectorIndex, ok := s.GetVectorIndex("")
		if !ok {
			return errors.Errorf("validate vector index for update of %v for vector: vector index not found", merge.ID)
		}

		// validation needs to happen before any changes are done. Otherwise, insertion is aborted somewhere in-between.
		err := vectorIndex.ValidateBeforeInsert(merge.Vector)
		if err != nil {
			return errors.Wrapf(err, "validate vector index for update of %v", merge.ID)
		}
	}

	idBytes, err := uuid.MustParse(merge.ID.String()).MarshalBinary()
	if err != nil {
		return err
	}

	return s.merge(ctx, idBytes, merge)
}

func (s *Shard) merge(ctx context.Context, idBytes []byte, doc objects.MergeDocument) error {
	obj, status, err := s.mergeObjectInStorage(doc, idBytes)
	if err != nil {
		return err
	}

	// object was not changed, no further updates are required
	// https://github.com/weaviate/weaviate/issues/3949
	if status.skipUpsert {
		return nil
	}

	for targetVector, vector := range obj.Vectors {
		if err = s.updateVectorIndex(ctx, vector, status, targetVector); err != nil {
			return errors.Wrapf(err, "update vector index for target vector %s", targetVector)
		}
	}
	for targetVector, vector := range obj.MultiVectors {
		if err = s.updateMultiVectorIndex(ctx, vector, status, targetVector); err != nil {
			return errors.Wrapf(err, "update multi vector index for target vector %s", targetVector)
		}
	}

	if s.hasLegacyVectorIndex() {
		if err = s.updateVectorIndex(ctx, obj.Vector, status, ""); err != nil {
			return errors.Wrap(err, "update vector index")
		}
	}

	if err := s.updatePropertySpecificIndices(ctx, obj, status); err != nil {
		return errors.Wrap(err, "update property-specific indices")
	}

	if err := s.store.WriteWALs(); err != nil {
		return errors.Wrap(err, "flush all buffered WALs")
	}

	if err := s.mayUpsertObjectHashTree(obj, idBytes, status); err != nil {
		return errors.Wrap(err, "object merge in hashtree")
	}

	return nil
}

func (s *Shard) mergeObjectInStorage(merge objects.MergeDocument,
	idBytes []byte,
) (*storobj.Object, objectInsertStatus, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	var prevObj, obj *storobj.Object
	var status objectInsertStatus

	// see comment in shard_write_put.go::putObjectLSM
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]

	// wrapped in function to handle lock/unlock
	if err := func() error {
		lock.Lock()
		defer lock.Unlock()

		var err error
		prevObj, err = fetchObject(bucket, idBytes)
		if err != nil {
			return errors.Wrap(err, "get bucket")
		}

		if prevObj == nil {
			return errObjectNotFound
		}

		obj, _, err = s.mergeObjectData(prevObj, merge)
		if err != nil {
			return errors.Wrap(err, "merge object data")
		}

		status, err = s.determineInsertStatus(prevObj, obj)
		if err != nil {
			return errors.Wrap(err, "check insert/update status")
		}

		obj.DocID = status.docID
		if status.skipUpsert {
			return nil
		}

		objBytes, err := obj.MarshalBinary()
		if err != nil {
			return errors.Wrapf(err, "marshal object %s to binary", obj.ID())
		}

		if err := s.upsertObjectDataLSM(bucket, idBytes, objBytes, status.docID); err != nil {
			return errors.Wrap(err, "upsert object data")
		}

		return nil
	}(); err != nil {
		return nil, objectInsertStatus{}, err
	} else if status.skipUpsert {
		return obj, status, nil
	}

	if err := s.updateInvertedIndexLSM(obj, status, prevObj); err != nil {
		return nil, status, errors.Wrap(err, "update inverted indices")
	}

	return obj, status, nil
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

	// see comment in shard_write_put.go::putObjectLSM
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]
	lock.Lock()
	defer lock.Unlock()

	prevObj, err := fetchObject(bucket, idBytes)
	if err != nil {
		return out, err
	}

	if prevObj == nil {
		uid := uuid.UUID{}
		uid.UnmarshalBinary(idBytes)
		return out, fmt.Errorf("object with id %s not found", uid)
	}

	obj, notEmptyPrevObj, err := s.mergeObjectData(prevObj, merge)
	if err != nil {
		return out, errors.Wrap(err, "merge object data")
	}

	out.next = obj
	out.previous = notEmptyPrevObj

	status, err := s.determineMutableInsertStatus(prevObj, obj)
	if err != nil {
		return out, errors.Wrap(err, "check insert/update status")
	}
	out.status = status

	obj.DocID = status.docID // is not changed
	objBytes, err := obj.MarshalBinary()
	if err != nil {
		return out, errors.Wrapf(err, "marshal object %s to binary", obj.ID())
	}

	if err := s.upsertObjectDataLSM(bucket, idBytes, objBytes, status.docID); err != nil {
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

func (s *Shard) mergeObjectData(prevObj *storobj.Object,
	merge objects.MergeDocument,
) (*storobj.Object, *storobj.Object, error) {
	if prevObj == nil {
		s.index.logger.WithField("id", merge.ID).Error("resurrecting a zombie object")
		// DocID must be overwritten after status check, simply set to initial
		// value
		prevObj = storobj.New(0)
		prevObj.SetClass(merge.Class)
		prevObj.SetID(merge.ID)
	}

	return mergeProps(prevObj, merge), prevObj, nil
}

func mergeProps(previous *storobj.Object,
	merge objects.MergeDocument,
) *storobj.Object {
	next := previous.DeepCopyDangerous()
	properties, ok := next.Properties().(map[string]interface{})
	if !ok || properties == nil {
		properties = map[string]interface{}{}
	}

	// remove properties from object that have been set to nil
	for _, propToDelete := range merge.PropertiesToDelete {
		delete(properties, propToDelete)
	}

	for propName, value := range merge.PrimitiveSchema {
		// for primitive props, we simply need to overwrite
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

	if len(merge.Vectors) == 0 {
		next.Vectors = previous.Vectors
		next.MultiVectors = previous.MultiVectors
	} else {
		next.Vectors = vectorsAsMap(merge.Vectors)
		next.MultiVectors = multiVectorsAsMap(merge.Vectors)
	}

	next.Object.LastUpdateTimeUnix = merge.UpdateTime
	next.SetProperties(properties)

	return next
}

func vectorsAsMap(in models.Vectors) map[string][]float32 {
	if len(in) > 0 {
		out := make(map[string][]float32)
		for targetVector, vector := range in {
			if v, ok := vector.([]float32); ok {
				out[targetVector] = v
			}
		}
		return out
	}
	return nil
}

func multiVectorsAsMap(in models.Vectors) map[string][][]float32 {
	if len(in) > 0 {
		out := make(map[string][][]float32)
		for targetVector, vector := range in {
			if v, ok := vector.([][]float32); ok {
				out[targetVector] = v
			}
		}
		return out
	}
	return nil
}

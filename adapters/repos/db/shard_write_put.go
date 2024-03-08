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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) PutObject(ctx context.Context, object *storobj.Object) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}
	uuid, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}
	return s.putOne(ctx, uuid, object)
}

func (s *Shard) putOne(ctx context.Context, uuid []byte, object *storobj.Object) error {
	if s.hasTargetVectors() {
		if len(object.Vectors) > 0 {
			for targetVector, vector := range object.Vectors {
				if vectorIndex := s.VectorIndexForName(targetVector); vectorIndex != nil {
					if err := vectorIndex.ValidateBeforeInsert(vector); err != nil {
						return errors.Wrapf(err, "Validate vector index %s for target vector %s", targetVector, object.ID())
					}
				}
			}
		}
	} else {
		if object.Vector != nil {
			// validation needs to happen before any changes are done. Otherwise, insertion is aborted somewhere in-between.
			err := s.vectorIndex.ValidateBeforeInsert(object.Vector)
			if err != nil {
				return errors.Wrapf(err, "Validate vector index for %s", object.ID())
			}
		}
	}

	status, err := s.putObjectLSM(object, uuid)
	if err != nil {
		return errors.Wrap(err, "store object in LSM store")
	}

	// object was not changed, no further updates are required
	// https://github.com/weaviate/weaviate/issues/3949
	if status.skipUpsert {
		return nil
	}

	if s.hasTargetVectors() {
		for targetVector, vector := range object.Vectors {
			if err := s.updateVectorIndexForName(vector, status, targetVector); err != nil {
				return errors.Wrapf(err, "update vector index for target vector %s", targetVector)
			}
		}
	} else {
		if err := s.updateVectorIndex(object.Vector, status); err != nil {
			return errors.Wrap(err, "update vector index")
		}
	}

	if err := s.updatePropertySpecificIndices(object, status); err != nil {
		return errors.Wrap(err, "update property-specific indices")
	}

	if err := s.store.WriteWALs(); err != nil {
		return errors.Wrap(err, "flush all buffered WALs")
	}

	if err := s.GetPropertyLengthTracker().Flush(false); err != nil {
		return errors.Wrap(err, "flush prop length tracker to disk")
	}

	return nil
}

// as the name implies this method only performs the insertions, but completely
// ignores any deletes. It thus assumes that the caller has already taken care
// of all the deletes in another way
func (s *Shard) updateVectorIndexIgnoreDelete(vector []float32,
	status objectInsertStatus,
) error {
	// vector was not changed, object was not changed or changed without changing vector
	// https://github.com/weaviate/weaviate/issues/3948
	// https://github.com/weaviate/weaviate/issues/3949
	if status.docIDPreserved || status.skipUpsert {
		return nil
	}

	// vector is now optional as of
	// https://github.com/weaviate/weaviate/issues/1800
	if len(vector) == 0 {
		return nil
	}

	if err := s.vectorIndex.Add(status.docID, vector); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	return nil
}

// as the name implies this method only performs the insertions, but completely
// ignores any deletes. It thus assumes that the caller has already taken care
// of all the deletes in another way
func (s *Shard) updateVectorIndexesIgnoreDelete(vectors map[string][]float32,
	status objectInsertStatus,
) error {
	// vector was not changed, object was not changed or changed without changing vector
	// https://github.com/weaviate/weaviate/issues/3948
	// https://github.com/weaviate/weaviate/issues/3949
	if status.docIDPreserved || status.skipUpsert {
		return nil
	}

	// vector is now optional as of
	// https://github.com/weaviate/weaviate/issues/1800
	if len(vectors) == 0 {
		return nil
	}

	for targetVector, vector := range vectors {
		if vectorIndex := s.VectorIndexForName(targetVector); vectorIndex != nil {
			if err := vectorIndex.Add(status.docID, vector); err != nil {
				return errors.Wrapf(err, "insert doc id %d to vector index for target vector %s", status.docID, targetVector)
			}
		}
	}

	return nil
}

func (s *Shard) updateVectorIndex(vector []float32,
	status objectInsertStatus,
) error {
	return s.updateVectorInVectorIndex(vector, status, s.queue, s.vectorIndex)
}

func (s *Shard) updateVectorIndexForName(vector []float32,
	status objectInsertStatus, targetVector string,
) error {
	queue, ok := s.queues[targetVector]
	if !ok {
		return fmt.Errorf("vector queue not found for target vector %s", targetVector)
	}
	vectorIndex := s.VectorIndexForName(targetVector)
	if vectorIndex == nil {
		return fmt.Errorf("vector index not found for target vector %s", targetVector)
	}
	return s.updateVectorInVectorIndex(vector, status, queue, vectorIndex)
}

func (s *Shard) updateVectorInVectorIndex(vector []float32,
	status objectInsertStatus, queue *IndexQueue, vectorIndex VectorIndex,
) error {
	// even if no vector is provided in an update, we still need
	// to delete the previous vector from the index, if it
	// exists. otherwise, the associated doc id is left dangling,
	// resulting in failed attempts to merge an object on restarts.
	if status.docIDChanged {
		if err := queue.Delete(status.oldDocID); err != nil {
			return errors.Wrapf(err, "delete doc id %d from vector index", status.oldDocID)
		}
	}

	// vector was not changed, object was updated without changing docID
	// https://github.com/weaviate/weaviate/issues/3948
	if status.docIDPreserved {
		return nil
	}

	// vector is now optional as of
	// https://github.com/weaviate/weaviate/issues/1800
	if len(vector) == 0 {
		return nil
	}

	if err := vectorIndex.Add(status.docID, vector); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	if err := vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush all vector index buffered WALs")
	}

	return nil
}

func fetchObject(bucket *lsmkv.Bucket, idBytes []byte) (*storobj.Object, error) {
	objBytes, err := bucket.Get(idBytes)
	if err != nil {
		return nil, err
	}
	if len(objBytes) == 0 {
		return nil, nil
	}

	obj, err := storobj.FromBinary(objBytes)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (s *Shard) putObjectLSM(obj *storobj.Object, idBytes []byte,
) (objectInsertStatus, error) {
	before := time.Now()
	defer s.metrics.PutObject(before)

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	var prevObj *storobj.Object
	var status objectInsertStatus

	// First the object bucket is checked if an object with the same uuid is alreadypresent,
	// to determine if it is insert or an update.
	// Afterwards the bucket is updated. To avoid races, only one goroutine can do this at once.
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]

	// wrapped in function to handle lock/unlock
	if err := func() error {
		lock.Lock()
		defer lock.Unlock()

		var err error

		before = time.Now()
		prevObj, err = fetchObject(bucket, idBytes)
		if err != nil {
			return err
		}

		status, err = s.determineInsertStatus(prevObj, obj)
		if err != nil {
			return err
		}
		s.metrics.PutObjectDetermineStatus(before)

		obj.DocID = status.docID
		if status.skipUpsert {
			return nil
		}

		objBinary, err := obj.MarshalBinary()
		if err != nil {
			return errors.Wrapf(err, "marshal object %s to binary", obj.ID())
		}

		before = time.Now()
		if err := s.upsertObjectDataLSM(bucket, idBytes, objBinary, status.docID); err != nil {
			return errors.Wrap(err, "upsert object data")
		}
		s.metrics.PutObjectUpsertObject(before)

		return nil
	}(); err != nil {
		return objectInsertStatus{}, err
	} else if status.skipUpsert {
		return status, nil
	}

	before = time.Now()
	if err := s.updateInvertedIndexLSM(obj, status, prevObj); err != nil {
		return objectInsertStatus{}, errors.Wrap(err, "update inverted indices")
	}
	s.metrics.PutObjectUpdateInverted(before)

	return status, nil
}

type objectInsertStatus struct {
	docID        uint64
	docIDChanged bool
	oldDocID     uint64
	// docID was not changed, although object itself did. DocID can be preserved if
	// object's vector remain the same, allowing to omit vector index update which is time
	// consuming operation. New object is saved and inverted indexes updated if required.
	docIDPreserved bool
	// object was not changed, all properties and additional properties are the same as in
	// the one already stored. No object update, inverted indexes update and vector index
	// update is required.
	skipUpsert bool
}

// to be called with the current contents of a row, if the row is empty (i.e.
// didn't exist before), we will get a new docID from the central counter.
// Otherwise, we will reuse the previous docID and mark this as an update
func (s *Shard) determineInsertStatus(prevObj, nextObj *storobj.Object) (objectInsertStatus, error) {
	var out objectInsertStatus

	if prevObj == nil {
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "initial doc id: get new doc id from counter")
		}
		out.docID = docID
		return out, nil
	}

	out.oldDocID = prevObj.DocID

	// If object was not changed (props and additional props of prev and next objects are the same)
	// skip updates of object, inverted indexes and vector index.
	// https://github.com/weaviate/weaviate/issues/3949
	//
	// If object was changed (props or additional props of prev and next objects differ)
	// update objects and inverted indexes, skip update of vector index.
	// https://github.com/weaviate/weaviate/issues/3948
	//
	// Due to geo index's (using HNSW vector index) requirement new docID for delete+insert
	// (delete initially adds tombstone, which "overwrite" following insert of the same docID)
	// any update of geo property needs new docID for updating geo index.
	if preserve, skip := compareObjsForInsertStatus(prevObj, nextObj); preserve || skip {
		out.docID = prevObj.DocID
		out.docIDPreserved = preserve
		out.skipUpsert = skip
		return out, nil
	}

	docID, err := s.counter.GetAndInc()
	if err != nil {
		return out, errors.Wrap(err, "doc id update: get new doc id from counter")
	}
	out.docID = docID
	out.docIDChanged = true

	return out, nil
}

// determineMutableInsertStatus is a special version of determineInsertStatus
// where it does not alter the doc id if one already exists. Calling this
// method only makes sense under very special conditions, such as those
// outlined in mutableMergeObjectInTx
func (s *Shard) determineMutableInsertStatus(previous, next *storobj.Object) (objectInsertStatus, error) {
	var out objectInsertStatus

	if previous == nil {
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "initial doc id: get new doc id from counter")
		}
		out.docID = docID
		return out, nil
	}

	out.docID = previous.DocID

	// we are planning on mutating and thus not altering the doc id
	return out, nil
}

func (s *Shard) upsertObjectDataLSM(bucket *lsmkv.Bucket, id []byte, data []byte,
	docID uint64,
) error {
	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	docIDBytes := keyBuf.Bytes()

	return bucket.Put(id, data, lsmkv.WithSecondaryKey(0, docIDBytes))
}

func (s *Shard) updateInvertedIndexLSM(object *storobj.Object,
	status objectInsertStatus, prevObject *storobj.Object,
) error {
	props, nilprops, err := s.AnalyzeObject(object)
	if err != nil {
		return errors.Wrap(err, "analyze next object")
	}

	var prevProps []inverted.Property
	var prevNilprops []inverted.NilProperty

	if prevObject != nil {
		prevProps, prevNilprops, err = s.AnalyzeObject(prevObject)
		if err != nil {
			return fmt.Errorf("analyze previous object: %w", err)
		}
	}

	// if object updated (with or without docID changed)
	if status.docIDChanged || status.docIDPreserved {
		if err := s.subtractPropLengths(prevProps); err != nil {
			s.index.logger.WithField("action", "subtractPropLengths").WithError(err).Error("could not subtract prop lengths")
		}
	}

	if err := s.SetPropertyLengths(props); err != nil {
		return errors.Wrap(err, "store field length values for props")
	}

	var propsToAdd []inverted.Property
	var propsToDel []inverted.Property
	var nilpropsToAdd []inverted.NilProperty
	var nilpropsToDel []inverted.NilProperty

	// determine only changed properties to avoid unnecessary updates of inverted indexes
	if status.docIDPreserved {
		delta := inverted.Delta(prevProps, props)
		propsToAdd = delta.ToAdd
		propsToDel = delta.ToDelete
		deltaNil := inverted.DeltaNil(prevNilprops, nilprops)
		nilpropsToAdd = deltaNil.ToAdd
		nilpropsToDel = deltaNil.ToDelete
	} else {
		propsToAdd = inverted.DedupItems(props)
		propsToDel = inverted.DedupItems(prevProps)
		nilpropsToAdd = nilprops
		nilpropsToDel = prevNilprops
	}

	if prevObject != nil {
		// TODO: metrics
		if err := s.deleteFromInvertedIndicesLSM(propsToDel, nilpropsToDel, status.oldDocID); err != nil {
			return fmt.Errorf("delete inverted indices props: %w", err)
		}
		if s.index.Config.TrackVectorDimensions {
			if s.hasTargetVectors() {
				for vecName, vec := range prevObject.Vectors {
					if err := s.removeDimensionsForVecLSM(len(vec), status.oldDocID, vecName); err != nil {
						return fmt.Errorf("track dimensions of '%s' (delete): %w", vecName, err)
					}
				}
			} else {
				if err := s.removeDimensionsLSM(len(prevObject.Vector), status.oldDocID); err != nil {
					return fmt.Errorf("track dimensions (delete): %w", err)
				}
			}
		}
	}

	before := time.Now()
	if err := s.extendInvertedIndicesLSM(propsToAdd, nilpropsToAdd, status.docID); err != nil {
		return fmt.Errorf("put inverted indices props: %w", err)
	}
	s.metrics.InvertedExtend(before, len(propsToAdd))

	if s.index.Config.TrackVectorDimensions {
		if s.hasTargetVectors() {
			for vecName, vec := range object.Vectors {
				if err := s.extendDimensionTrackerForVecLSM(len(vec), status.docID, vecName); err != nil {
					return fmt.Errorf("track dimensions of '%s': %w", vecName, err)
				}
			}
		} else {
			if err := s.extendDimensionTrackerLSM(len(object.Vector), status.docID); err != nil {
				return fmt.Errorf("track dimensions: %w", err)
			}
		}
	}

	return nil
}

func compareObjsForInsertStatus(prevObj, nextObj *storobj.Object) (preserve, skip bool) {
	prevProps, ok := prevObj.Object.Properties.(map[string]interface{})
	if !ok {
		return false, false
	}
	nextProps, ok := nextObj.Object.Properties.(map[string]interface{})
	if !ok {
		return false, false
	}
	if !geoPropsEqual(prevProps, nextProps) {
		return false, false
	}
	if !common.VectorsEqual(prevObj.Vector, nextObj.Vector) {
		return false, false
	}
	if !targetVectorsEqual(prevObj.Vectors, nextObj.Vectors) {
		return false, false
	}
	if !addPropsEqual(prevObj.Object.Additional, nextObj.Object.Additional) {
		return true, false
	}
	if !propsEqual(prevProps, nextProps) {
		return true, false
	}
	return false, true
}

func geoPropsEqual(prevProps, nextProps map[string]interface{}) bool {
	geoPropsCompared := map[string]struct{}{}

	for name, prevVal := range prevProps {
		switch prevGeoVal := prevVal.(type) {
		case *models.GeoCoordinates:
			nextVal, ok := nextProps[name]
			if !ok {
				// matching prop does not exist in next
				return false
			}

			switch nextGeoVal := nextVal.(type) {
			case *models.GeoCoordinates:
				if !reflect.DeepEqual(prevGeoVal, nextGeoVal) {
					// matching geo props in prev and next differ
					return false
				}
			default:
				// matching prop in next is not geo
				return false
			}
			geoPropsCompared[name] = struct{}{}
		}
	}

	for name, nextVal := range nextProps {
		switch nextVal.(type) {
		case *models.GeoCoordinates:
			if _, ok := geoPropsCompared[name]; !ok {
				// matching geo prop does not exist in prev
				return false
			}
		}
	}

	return true
}

func timeToString(t time.Time) string {
	if b, err := t.MarshalText(); err == nil {
		return string(b)
	}
	return ""
}

func uuidToString(u uuid.UUID) string {
	if b, err := u.MarshalText(); err == nil {
		return string(b)
	}
	return ""
}

func targetVectorsEqual(prevTargetVectors, nextTargetVectors map[string][]float32) bool {
	if len(prevTargetVectors) == 0 && len(nextTargetVectors) == 0 {
		return true
	}

	visited := map[string]struct{}{}
	for vecName, vec := range prevTargetVectors {
		if !common.VectorsEqual(vec, nextTargetVectors[vecName]) {
			return false
		}
		visited[vecName] = struct{}{}
	}
	for vecName, vec := range nextTargetVectors {
		if _, ok := visited[vecName]; !ok {
			if !common.VectorsEqual(vec, prevTargetVectors[vecName]) {
				return false
			}
		}
	}

	return true
}

func addPropsEqual(prevAddProps, nextAddProps models.AdditionalProperties) bool {
	return reflect.DeepEqual(prevAddProps, nextAddProps)
}

func propsEqual(prevProps, nextProps map[string]interface{}) bool {
	if len(prevProps) != len(nextProps) {
		return false
	}

	for name := range nextProps {
		if _, ok := prevProps[name]; !ok {
			return false
		}

		switch nextVal := nextProps[name].(type) {
		case time.Time:
			if timeToString(nextVal) != prevProps[name] {
				return false
			}

		case []time.Time:
			prevVal, ok := prevProps[name].([]string)
			if !ok {
				return false
			}
			if len(nextVal) != len(prevVal) {
				return false
			}
			for i := range nextVal {
				if timeToString(nextVal[i]) != prevVal[i] {
					return false
				}
			}

		case uuid.UUID:
			if uuidToString(nextVal) != prevProps[name] {
				return false
			}

		case []uuid.UUID:
			prevVal, ok := prevProps[name].([]string)
			if !ok {
				return false
			}
			if len(nextVal) != len(prevVal) {
				return false
			}
			for i := range nextVal {
				if uuidToString(nextVal[i]) != prevVal[i] {
					return false
				}
			}

		case map[string]interface{}: // data type "object"
			prevVal, ok := prevProps[name].(map[string]interface{})
			if !ok {
				return false
			}
			if !propsEqual(prevVal, nextVal) {
				return false
			}

		case []interface{}: // data type "objects"
			prevVal, ok := prevProps[name].([]interface{})
			if !ok {
				return false
			}
			if len(nextVal) != len(prevVal) {
				return false
			}
			for i := range nextVal {
				nextValI, ok := nextVal[i].(map[string]interface{})
				if !ok {
					return false
				}
				prevValI, ok := prevVal[i].(map[string]interface{})
				if !ok {
					return false
				}
				if !propsEqual(prevValI, nextValI) {
					return false
				}
			}

		default:
			if !reflect.DeepEqual(nextProps[name], prevProps[name]) {
				return false
			}
		}
	}

	return true
}

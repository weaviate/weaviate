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
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) PutObject(ctx context.Context, object *storobj.Object) error {
	s.activityTrackerWrite.Add(1)
	if err := s.isReadOnly(); err != nil {
		return err
	}
	uid, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}
	return s.putOne(ctx, uid, object)
}

func (s *Shard) putOne(ctx context.Context, uuid []byte, object *storobj.Object) error {
	status, err := s.putObjectLSM(object, uuid)
	if err != nil {
		return errors.Wrap(err, "store object in LSM store")
	}

	// object was not changed, no further updates are required
	// https://github.com/weaviate/weaviate/issues/3949
	if status.skipUpsert {
		return nil
	}

	for targetVector, vector := range object.Vectors {
		if err := s.updateVectorIndex(ctx, vector, status, targetVector); err != nil {
			return errors.Wrapf(err, "update vector index for target vector %s", targetVector)
		}
	}
	for targetVector, multiVector := range object.MultiVectors {
		if err := s.updateMultiVectorIndex(ctx, multiVector, status, targetVector); err != nil {
			return errors.Wrapf(err, "update multi vector index for target vector %s", targetVector)
		}
	}

	if s.hasLegacyVectorIndex() {
		if err := s.updateVectorIndex(ctx, object.Vector, status, ""); err != nil {
			return errors.Wrap(err, "update vector index")
		}
	}

	if err := s.updatePropertySpecificIndices(ctx, object, status); err != nil {
		return errors.Wrap(err, "update property-specific indices")
	}

	if err := s.store.WriteWALs(); err != nil {
		return errors.Wrap(err, "flush all buffered WALs")
	}

	if err := s.GetPropertyLengthTracker().Flush(); err != nil {
		return errors.Wrap(err, "flush prop length tracker to disk")
	}

	if err := s.mayUpsertObjectHashTree(object, uuid, status); err != nil {
		return errors.Wrap(err, "object creation in hashtree")
	}

	return nil
}

// as the name implies this method only performs the insertions, but completely
// ignores any deletes. It thus assumes that the caller has already taken care
// of all the deletes in another way
func (s *Shard) updateVectorIndexIgnoreDelete(ctx context.Context, vector []float32,
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

	if queue, ok := s.GetVectorIndexQueue(""); ok {
		if err := queue.Insert(ctx, &common.Vector[[]float32]{ID: status.docID, Vector: vector}); err != nil {
			return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
		}
	}

	return nil
}

// as the name implies this method only performs the insertions, but completely
// ignores any deletes. It thus assumes that the caller has already taken care
// of all the deletes in another way
func (s *Shard) updateVectorIndexesIgnoreDelete(ctx context.Context,
	vectors map[string][]float32, status objectInsertStatus,
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
		if q, ok := s.GetVectorIndexQueue(targetVector); ok {
			if err := q.Insert(ctx, &common.Vector[[]float32]{ID: status.docID, Vector: vector}); err != nil {
				return errors.Wrapf(err, "insert doc id %d to vector index for target vector %s", status.docID, targetVector)
			}
		}
	}

	return nil
}

// this method implements the same logic as updateVectorIndexesIgnoreDelete but
// supports multi vectors
func (s *Shard) updateMultiVectorIndexesIgnoreDelete(ctx context.Context,
	multiVectors map[string][][]float32, status objectInsertStatus,
) error {
	// vector was not changed, object was not changed or changed without changing vector
	// https://github.com/weaviate/weaviate/issues/3948
	// https://github.com/weaviate/weaviate/issues/3949
	if status.docIDPreserved || status.skipUpsert {
		return nil
	}

	// vector is now optional as of
	// https://github.com/weaviate/weaviate/issues/1800
	if len(multiVectors) == 0 {
		return nil
	}

	for targetVector, vector := range multiVectors {
		if q, ok := s.GetVectorIndexQueue(targetVector); ok {
			if err := q.Insert(ctx, &common.Vector[[][]float32]{ID: status.docID, Vector: vector}); err != nil {
				return errors.Wrapf(err, "insert doc id %d to multi vector index for target vector %s", status.docID, targetVector)
			}
		}
	}

	return nil
}

func (s *Shard) updateVectorIndex(ctx context.Context, vector []float32,
	status objectInsertStatus, targetVector string,
) error {
	return updateVectorInVectorIndex(ctx, s, targetVector, vector, status)
}

func (s *Shard) updateMultiVectorIndex(ctx context.Context, vector [][]float32,
	status objectInsertStatus, targetVector string,
) error {
	return updateVectorInVectorIndex(ctx, s, targetVector, vector, status)
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
) (status objectInsertStatus, err error) {
	before := time.Now()
	defer s.metrics.PutObject(before)

	for targetVector, vector := range obj.Vectors {
		if vectorIndex, ok := s.GetVectorIndex(targetVector); ok {
			if err := vectorIndex.ValidateBeforeInsert(vector); err != nil {
				return status, errors.Wrapf(err, "Validate vector index %s for target vector %s", targetVector, obj.ID())
			}
		}
	}

	for targetVector, vector := range obj.MultiVectors {
		if vectorIndex, ok := s.GetVectorIndex(targetVector); ok {
			if err := vectorIndex.ValidateMultiBeforeInsert(vector); err != nil {
				return status, errors.Wrapf(err, "Validate vector index %s for target multi vector %s", targetVector, obj.ID())
			}
		}
	}

	if len(obj.Vector) > 0 && s.hasLegacyVectorIndex() {
		// validation needs to happen before any changes are done. Otherwise, insertion is aborted somewhere in-between.
		if index, ok := s.GetVectorIndex(""); ok {
			if err = index.ValidateBeforeInsert(obj.Vector); err != nil {
				return status, errors.Wrapf(err, "Validate vector index for %s", obj.ID())
			}
		}
	}

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	var prevObj *storobj.Object

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

func (s *Shard) mayUpsertObjectHashTree(object *storobj.Object, uuidBytes []byte, status objectInsertStatus) error {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()

	if s.hashtree == nil {
		return nil
	}

	return s.upsertObjectHashTree(object, uuidBytes, status)
}

func (s *Shard) upsertObjectHashTree(object *storobj.Object, uuidBytes []byte, status objectInsertStatus) error {
	if len(uuidBytes) != 16 {
		return fmt.Errorf("invalid object uuid")
	}

	if object.Object.LastUpdateTimeUnix < 1 {
		return fmt.Errorf("invalid object last update time")
	}

	leaf := s.hashtreeLeafFor(uuidBytes)

	var objectDigest [16 + 8]byte
	copy(objectDigest[:], uuidBytes)

	if status.oldUpdateTime > 0 {
		// Given only latest object version is maintained, previous registration is erased
		binary.BigEndian.PutUint64(objectDigest[16:], uint64(status.oldUpdateTime))
		s.hashtree.AggregateLeafWith(leaf, objectDigest[:])
	}

	binary.BigEndian.PutUint64(objectDigest[16:], uint64(object.Object.LastUpdateTimeUnix))
	s.hashtree.AggregateLeafWith(leaf, objectDigest[:])

	return nil
}

func (s *Shard) hashtreeLeafFor(uuidBytes []byte) uint64 {
	hashtreeHeight := s.asyncReplicationConfig.hashtreeHeight

	if hashtreeHeight == 0 {
		return 0
	}

	return binary.BigEndian.Uint64(uuidBytes[:8]) >> (64 - hashtreeHeight)
}

type objectInsertStatus struct {
	docID         uint64
	docIDChanged  bool
	oldDocID      uint64
	oldUpdateTime int64
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
	out.oldUpdateTime = prevObj.LastUpdateTimeUnix()

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
	out.oldUpdateTime = previous.LastUpdateTimeUnix()

	// we are planning on mutating and thus not altering the doc id
	return out, nil
}

func (s *Shard) upsertObjectDataLSM(bucket *lsmkv.Bucket, id []byte, data []byte,
	docID uint64,
) error {
	keyBuf := bytes.NewBuffer(nil)
	err := binary.Write(keyBuf, binary.LittleEndian, &docID)
	if err != nil {
		return fmt.Errorf("write doc id to buffer: %w", err)
	}
	docIDBytes := keyBuf.Bytes()

	return bucket.Put(id, data,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes),
	)
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
		delta := inverted.DeltaSkipSearchable(prevProps, props, s.getSearchableBlockmaxProperties())

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
			err = prevObject.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
				if err = s.removeDimensionsLSM(dims, status.oldDocID, targetVector); err != nil {
					return fmt.Errorf("remove dimension tracking for vector %q: %w", targetVector, err)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	before := time.Now()
	if err := s.extendInvertedIndicesLSM(propsToAdd, nilpropsToAdd, status.docID); err != nil {
		return fmt.Errorf("put inverted indices props: %w", err)
	}
	s.metrics.InvertedExtend(before, len(propsToAdd))

	if s.index.Config.TrackVectorDimensions {
		err = object.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
			if err = s.extendDimensionTrackerLSM(dims, status.docID, targetVector); err != nil {
				return fmt.Errorf("add dimension tracking for vector %q: %w", targetVector, err)
			}
			return nil
		})
		if err != nil {
			return err
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
	if !targetMultiVectorsEqual(prevObj.MultiVectors, nextObj.MultiVectors) {
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
	return targetVectorsEqualCheck(prevTargetVectors, nextTargetVectors, common.VectorsEqual)
}

func targetMultiVectorsEqual(prevTargetVectors, nextTargetVectors map[string][][]float32) bool {
	return targetVectorsEqualCheck(prevTargetVectors, nextTargetVectors, common.MultiVectorsEqual)
}

func targetVectorsEqualCheck[T []float32 | [][]float32](prevTargetVectors, nextTargetVectors map[string]T,
	vectorsEqual func(vecA, vecB T) bool,
) bool {
	if len(prevTargetVectors) == 0 && len(nextTargetVectors) == 0 {
		return true
	}

	visited := map[string]struct{}{}
	for vecName, vec := range prevTargetVectors {
		if !vectorsEqual(vec, nextTargetVectors[vecName]) {
			return false
		}
		visited[vecName] = struct{}{}
	}
	for vecName, vec := range nextTargetVectors {
		if _, ok := visited[vecName]; !ok {
			if !vectorsEqual(vec, prevTargetVectors[vecName]) {
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

func updateVectorInVectorIndex[T dto.Embedding](ctx context.Context, shard *Shard, targetVector string, vector T,
	status objectInsertStatus,
) error {
	queue, ok := shard.GetVectorIndexQueue(targetVector)
	if !ok {
		return fmt.Errorf("vector index not found for %s", targetVector)
	}

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

	if err := queue.Insert(ctx, &common.Vector[T]{ID: status.docID, Vector: vector}); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	if err := queue.Flush(); err != nil {
		return errors.Wrap(err, "flush all vector index buffered WALs")
	}

	return nil
}

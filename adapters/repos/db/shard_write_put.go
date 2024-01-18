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
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
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
	if object.Vector != nil {
		// validation needs to happen before any changes are done. Otherwise, insertion is aborted somewhere in-between.
		err := s.VectorIndex().ValidateBeforeInsert(object.Vector)
		if err != nil {
			return errors.Wrapf(err, "Validate vector index for %s", object.ID())
		}
	}

	status, err := s.putObjectLSM(object, uuid)
	if err != nil {
		return errors.Wrap(err, "store object in LSM store")
	}

	if err := s.updateVectorIndex(object.Vector, status); err != nil {
		return errors.Wrap(err, "update vector index")
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

	if err := s.VectorIndex().Add(status.docID, vector); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	return nil
}

func (s *Shard) updateVectorIndex(vector []float32,
	status objectInsertStatus,
) error {
	// even if no vector is provided in an update, we still need
	// to delete the previous vector from the index, if it
	// exists. otherwise, the associated doc id is left dangling,
	// resulting in failed attempts to merge an object on restarts.
	if status.docIDChanged {
		if err := s.queue.Delete(status.oldDocID); err != nil {
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

	if err := s.VectorIndex().Add(status.docID, vector); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	if err := s.VectorIndex().Flush(); err != nil {
		return errors.Wrap(err, "flush all vector index buffered WALs")
	}

	return nil
}

// TODO AL_skip_vector_reindex: adjust to batch?
func (s *Shard) putObjectLSM(object *storobj.Object, idBytes []byte,
) (objectInsertStatus, error) {
	before := time.Now()
	defer s.metrics.PutObject(before)

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	// First the object bucket is checked if already an object with the same uuid is present, to determine if it is new
	// or an update. Afterwards the bucket is updates. To avoid races, only one goroutine can do this at once.
	lock := &s.docIdLock[s.uuidToIdLockPoolId(idBytes)]
	lock.Lock()
	previous_object_bytes, err := bucket.Get(idBytes)
	if err != nil {
		lock.Unlock()
		return objectInsertStatus{}, err
	}

	status, err := s.determineInsertStatus(previous_object_bytes, object)
	if err != nil {
		lock.Unlock()
		return status, errors.Wrap(err, "check insert/update status")
	}
	s.metrics.PutObjectDetermineStatus(before)

	object.DocID = status.docID
	data, err := object.MarshalBinary()
	if err != nil {
		lock.Unlock()
		return status, errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	before = time.Now()
	if err := s.upsertObjectDataLSM(bucket, idBytes, data, status.docID); err != nil {
		lock.Unlock()
		return status, errors.Wrap(err, "upsert object data")
	}
	lock.Unlock()
	s.metrics.PutObjectUpsertObject(before)

	before = time.Now()
	if err := s.updateInvertedIndexLSM(object, status, previous_object_bytes); err != nil {
		return status, errors.Wrap(err, "update inverted indices")
	}

	s.metrics.PutObjectUpdateInverted(before)

	return status, nil
}

type objectInsertStatus struct {
	docID          uint64
	docIDChanged   bool
	oldDocID       uint64
	docIDPreserved bool // docID preserved (docID was not changed, although object did change)
}

// to be called with the current contents of a row, if the row is empty (i.e.
// didn't exist before), we will get a new docID from the central counter.
// Otherwise, we will reuse the previous docID and mark this as an update
func (s *Shard) determineInsertStatus(previous []byte,
	next *storobj.Object,
) (objectInsertStatus, error) {
	var out objectInsertStatus

	if previous == nil {
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "initial doc id: get new doc id from counter")
		}
		out.docID = docID
		return out, nil
	}

	docID, err := storobj.DocIDFromBinary(previous)
	if err != nil {
		return out, errors.Wrap(err, "get previous doc id from object binary")
	}
	out.oldDocID = docID

	// if vector was not changed, replace object and update indexed properties
	// without changing docID
	// https://github.com/weaviate/weaviate/issues/3948
	//
	// Due to geo index does not support delete+insert of geo value for the same docID
	// (as tombstones are used for hnsw geo index),
	// preserving docID will not be supported for classes with geo properties for now.
	// Feature can be improved in the future, by preserving docIDs for objects
	// not updating values of geo properties.
	if !s.hasGeoIndex() {
		if l := len(next.Vector); l > 0 {
			buffer := make([]float32, l)
			prevVector, err := storobj.VectorFromBinary(previous, buffer)
			if err != nil {
				return out, fmt.Errorf("get previous vector from object binary: %w", err)
			}
			if common.VectorsEqual(prevVector, next.Vector) {
				out.docID = docID
				out.docIDPreserved = true
				return out, nil
			}
		}
	}

	// with docIDs now being immutable (see
	// https://github.com/weaviate/weaviate/issues/1282) there is no
	// more check if we need to increase a docID. Any update will mean a doc ID
	// needs to be updated.
	docID, err = s.counter.GetAndInc()
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
func (s *Shard) determineMutableInsertStatus(previous []byte,
	next *storobj.Object,
) (objectInsertStatus, error) {
	var out objectInsertStatus

	if previous == nil {
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "initial doc id: get new doc id from counter")
		}
		out.docID = docID
		return out, nil
	}

	docID, err := storobj.DocIDFromBinary(previous)
	if err != nil {
		return out, errors.Wrap(err, "get previous doc id from object binary")
	}
	out.docID = docID

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
	status objectInsertStatus, previous []byte,
) error {
	props, nilprops, err := s.AnalyzeObject(object)
	if err != nil {
		return errors.Wrap(err, "analyze next object")
	}

	var prevObject *storobj.Object
	var prevProps []inverted.Property
	var prevNilprops []inverted.NilProperty

	if previous != nil {
		prevObject, err = storobj.FromBinary(previous)
		if err != nil {
			return fmt.Errorf("unmarshal previous object: %w", err)
		}

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
	} else {
		if err := s.ChangeObjectCountBy(1); err != nil {
			return fmt.Errorf("increment object count: %w", err)
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

	if previous != nil {
		// TODO: metrics
		if err := s.deleteFromInvertedIndicesLSM(propsToDel, nilpropsToDel, status.oldDocID); err != nil {
			return fmt.Errorf("delete inverted indices props: %w", err)
		}
		if s.index.Config.TrackVectorDimensions {
			if err := s.removeDimensionsLSM(len(prevObject.Vector), status.oldDocID); err != nil {
				return fmt.Errorf("track dimensions (delete): %w", err)
			}
		}
	}

	before := time.Now()
	if err := s.extendInvertedIndicesLSM(propsToAdd, nilpropsToAdd, status.docID); err != nil {
		return fmt.Errorf("put inverted indices props: %w", err)
	}
	s.metrics.InvertedExtend(before, len(propsToAdd))

	if s.index.Config.TrackVectorDimensions {
		if err := s.extendDimensionTrackerLSM(len(object.Vector), status.docID); err != nil {
			return fmt.Errorf("track dimensions: %w", err)
		}
	}

	return nil
}

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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

func (s *Shard) putObject(ctx context.Context, object *storobj.Object) error {
	idBytes, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	var status objectInsertStatus

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		s, err := s.putObjectInTx(tx, object, idBytes)
		if err != nil {
			return err
		}

		status = s
		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	if err := s.updateVectorIndex(object.Vector, status); err != nil {
		return errors.Wrap(err, "update vector index")
	}

	return nil
}

func (s *Shard) updateVectorIndex(vector []float32,
	status objectInsertStatus) error {

	if status.isUpdate && !status.docIDChanged {
		// nothing has changed, nothing to do for us
		return nil

	}

	if status.docIDChanged {
		if err := s.vectorIndex.Delete(int(status.oldDocID)); err != nil {
			return errors.Wrapf(err, "delete doc id %q from vector index", status.oldDocID)
		}

	}

	if err := s.vectorIndex.Add(int(status.docID), vector); err != nil {
		return errors.Wrapf(err, "insert doc id %q to vector index", status.docID)
	}

	return nil
}

func (s *Shard) putObjectInTx(tx *bolt.Tx, object *storobj.Object,
	idBytes []byte) (objectInsertStatus, error) {
	before := time.Now()
	defer s.metrics.PutObject(before)

	bucket := tx.Bucket(helpers.ObjectsBucket)
	previous := bucket.Get([]byte(idBytes))

	status, err := s.determineInsertStatus(previous, object)
	if err != nil {
		return status, errors.Wrap(err, "check insert/update status")
	}

	object.SetIndexID(status.docID)
	data, err := object.MarshalBinary()
	if err != nil {
		return status, errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	before = time.Now()
	if err := s.upsertObjectData(bucket, idBytes, data); err != nil {
		return status, errors.Wrap(err, "upsert object data")
	}
	s.metrics.PutObjectUpsertObject(before)

	before = time.Now()
	if err := s.updateIndexIDLookup(tx, idBytes, status); err != nil {
		return status, errors.Wrap(err, "add/update docID->UUID index")
	}
	s.metrics.PutObjectUpdateIndexID(before)

	before = time.Now()
	if err := s.updateInvertedIndex(tx, object, status, previous); err != nil {
		return status, errors.Wrap(err, "udpate inverted indices")
	}
	s.metrics.PutObjectUpdateInverted(before)

	return status, nil
}

type objectInsertStatus struct {
	docID        uint32
	isUpdate     bool
	docIDChanged bool
	oldDocID     uint32
}

// to be called with the current contents of a row, if the row is empty (i.e.
// didn't exist before, we will get a new docID from the central counter.
// Otherwise, we will will reuse the previous docID and mark this as an update
func (s Shard) determineInsertStatus(previous []byte,
	next *storobj.Object) (objectInsertStatus, error) {
	var out objectInsertStatus

	if previous == nil {
		out.isUpdate = false
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "initial doc id: get new doc id from counter")
		}
		out.docID = docID
		return out, nil
	}

	out.isUpdate = true
	docID, err := storobj.DocIDFromBinary(previous)
	if err != nil {
		return out, errors.Wrap(err, "get previous doc id from object binary")
	}
	out.docID = docID
	out.oldDocID = docID

	// we need to check if assignment of a new doc ID is required
	if s.newDocIDRequired(previous, next) {
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "doc id update: get new doc id from counter")
		}
		out.docID = docID
		out.docIDChanged = true
	}

	return out, nil
}

// check if we need to update the doc ID. This might have various reasons in
// the future. As of now, the only reason we need to update the docID is if the
// vector position has changed, as vector indices are considered immutable
func (s Shard) newDocIDRequired(previous []byte, next *storobj.Object) bool {
	old, err := storobj.FromBinary(previous)
	if err != nil {
		return true
	}

	return !vectorsEqual(old.Vector, next.Vector)
}

func (s Shard) upsertObjectData(bucket *bolt.Bucket, id []byte, data []byte) error {
	return bucket.Put(id, data)
}

func (s Shard) updateInvertedIndex(tx *bolt.Tx, object *storobj.Object,
	status objectInsertStatus, previous []byte) error {
	// if this is a new object, we simply have to add those. If this is an update
	// (see below), we have to calculate the delta and then only add the new ones
	nextInvertProps, err := s.analyzeObject(object)
	if err != nil {
		return errors.Wrap(err, "analyze next object")
	}

	var invertPropsToAdd = nextInvertProps
	if status.isUpdate {
		// since this is an update, we need to analyze the old object, calculate
		// the delta, then delete the "toBeDeleted" and overwrite the "toAdds" with
		// the result of the delta

		previousObject, err := storobj.FromBinary(previous)
		if err != nil {
			return errors.Wrap(err, "unmarshal previous object")
		}

		previousInvertProps, err := s.analyzeObject(previousObject)
		if err != nil {
			return errors.Wrap(err, "analyze previous object")
		}

		// there are two possible update cases:
		// Case A: We have the same docID, so we only need to remove/add the deltas
		// Case B: The doc ID has changed, so we need to delete anything pointing
		// to the old doc ID and add everything pointing to the new one

		if status.docIDChanged {
			// doc ID has changed, delete all old, add all new
			before := time.Now()
			err = s.deleteFromInvertedIndices(tx, previousInvertProps, status.oldDocID)
			if err != nil {
				return errors.Wrap(err, "delete obsolete inverted pointers")
			}
			s.metrics.InvertedDeleteOld(before)

		} else {
			// doc ID has not changed, only handle the delta
			before := time.Now()
			delta := inverted.Delta(previousInvertProps, nextInvertProps)
			err = s.deleteFromInvertedIndices(tx, delta.ToDelete, status.docID)
			if err != nil {
				return errors.Wrap(err, "delete obsolete inverted pointers")
			}
			s.metrics.InvertedDeleteDelta(before)

			invertPropsToAdd = delta.ToAdd
		}
	}

	before := time.Now()
	err = s.extendInvertedIndices(tx, invertPropsToAdd, status.docID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
	}
	s.metrics.InvertedExtend(before)

	return nil
}

func (s *Shard) updateIndexIDLookup(tx *bolt.Tx, id []byte, status objectInsertStatus) error {
	if status.docIDChanged {
		// clean up old docId first
		if err := s.removeIndexIDLookup(tx, status.oldDocID); err != nil {
			return errors.Wrap(err, "remove docID->UUID index")
		}
	}

	if err := s.addIndexIDLookup(tx, id, status.docID); err != nil {
		return errors.Wrap(err, "add docID->UUID index")
	}

	return nil
}

func (s *Shard) addIndexIDLookup(tx *bolt.Tx, id []byte, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(helpers.IndexIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	if err := b.Put(key, id); err != nil {
		return errors.Wrap(err, "store uuid for index id")
	}

	return nil
}

func (s *Shard) removeIndexIDLookup(tx *bolt.Tx, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(helpers.IndexIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	if err := b.Delete(key); err != nil {
		return errors.Wrap(err, "delete uuid for index id")
	}

	return nil
}

func vectorsEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

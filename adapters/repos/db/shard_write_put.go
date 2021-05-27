//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	bolt "go.etcd.io/bbolt"
)

func (s *Shard) putObject(ctx context.Context, object *storobj.Object) error {
	idBytes, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	var status objectInsertStatus

	status, err = s.putObjectLSM(object, idBytes, false)
	if err != nil {
		return errors.Wrap(err, "store object in LSM store")
	}

	if err := s.updateVectorIndex(object.Vector, status); err != nil {
		return errors.Wrap(err, "update vector index")
	}

	if err := s.updatePropertySpecificIndices(object, status); err != nil {
		return errors.Wrap(err, "update property-specific indices")
	}

	return nil
}

func (s *Shard) updateVectorIndex(vector []float32,
	status objectInsertStatus) error {
	if status.docIDChanged {
		if err := s.vectorIndex.Delete(status.oldDocID); err != nil {
			return errors.Wrapf(err, "delete doc id %d from vector index", status.oldDocID)
		}
	}

	if err := s.vectorIndex.Add(status.docID, vector); err != nil {
		return errors.Wrapf(err, "insert doc id %d to vector index", status.docID)
	}

	return nil
}

// func (s *Shard) putObjectInTx(tx *bolt.Tx, object *storobj.Object,
// 	idBytes []byte, skipInverted bool) (objectInsertStatus, error) {
// 	before := time.Now()
// 	defer s.metrics.PutObject(before)

// 	bucket := tx.Bucket(helpers.ObjectsBucket)
// 	previous := bucket.Get([]byte(idBytes))

// 	status, err := s.determineInsertStatus(previous, object)
// 	if err != nil {
// 		return status, errors.Wrap(err, "check insert/update status")
// 	}

// 	object.SetDocID(status.docID)
// 	data, err := object.MarshalBinary()
// 	if err != nil {
// 		return status, errors.Wrapf(err, "marshal object %s to binary", object.ID())
// 	}

// 	before = time.Now()
// 	if err := s.upsertObjectData(bucket, idBytes, data); err != nil {
// 		return status, errors.Wrap(err, "upsert object data")
// 	}
// 	s.metrics.PutObjectUpsertObject(before)

// 	before = time.Now()
// 	if err := s.updateDocIDLookup(tx, idBytes, status); err != nil {
// 		return status, errors.Wrap(err, "add/update docID->UUID index")
// 	}
// 	s.metrics.PutObjectUpdateDocID(before)

// 	if !skipInverted {
// 		before = time.Now()
// 		if err := s.updateInvertedIndex(tx, object, status.docID); err != nil {
// 			return status, errors.Wrap(err, "update inverted indices")
// 		}
// 		s.metrics.PutObjectUpdateInverted(before)
// 	}

// 	return status, nil
// }

func (s *Shard) putObjectLSM(object *storobj.Object,
	idBytes []byte, skipInverted bool) (objectInsertStatus, error) {
	before := time.Now()
	defer s.metrics.PutObject(before)

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	previous, err := bucket.Get([]byte(idBytes))
	if err != nil {
		return objectInsertStatus{}, err
	}

	status, err := s.determineInsertStatus(previous, object)
	if err != nil {
		return status, errors.Wrap(err, "check insert/update status")
	}

	object.SetDocID(status.docID)
	data, err := object.MarshalBinary()
	if err != nil {
		return status, errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	before = time.Now()
	if err := s.upsertObjectDataLSM(bucket, idBytes, data); err != nil {
		return status, errors.Wrap(err, "upsert object data")
	}
	s.metrics.PutObjectUpsertObject(before)

	before = time.Now()
	if err := s.updateDocIDLookupLSM(idBytes, status); err != nil {
		return status, errors.Wrap(err, "add/update docID->UUID index")
	}
	s.metrics.PutObjectUpdateDocID(before)

	if !skipInverted {
		before = time.Now()
		if err := s.updateInvertedIndexLSM(object, status, previous); err != nil {
			return status, errors.Wrap(err, "update inverted indices")
		}
		s.metrics.PutObjectUpdateInverted(before)
	}

	return status, nil
}

type objectInsertStatus struct {
	docID        uint64
	docIDChanged bool
	oldDocID     uint64
}

// to be called with the current contents of a row, if the row is empty (i.e.
// didn't exist before, we will get a new docID from the central counter.
// Otherwise, we will will reuse the previous docID and mark this as an update
func (s Shard) determineInsertStatus(previous []byte,
	next *storobj.Object) (objectInsertStatus, error) {
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

	// with docIDs now being immutable (see
	// https://github.com/semi-technologies/weaviate/issues/1282) there is no
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
func (s Shard) determineMutableInsertStatus(previous []byte,
	next *storobj.Object) (objectInsertStatus, error) {
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

func (s Shard) upsertObjectData(bucket *bolt.Bucket, id []byte, data []byte) error {
	return bucket.Put(id, data)
}

func (s Shard) upsertObjectDataLSM(bucket *lsmkv.Bucket, id []byte, data []byte) error {
	return bucket.Put(id, data)
}

func (s Shard) updateInvertedIndexLSM(object *storobj.Object,
	status objectInsertStatus, previous []byte) error {
	props, err := s.analyzeObject(object)
	if err != nil {
		return errors.Wrap(err, "analyze next object")
	}

	// TODO: metrics
	if err := s.updateInvertedIndexCleanupOldLSM(status, previous); err != nil {
		return errors.Wrap(err, "analyze and cleanup previous")
	}

	before := time.Now()
	err = s.extendInvertedIndicesLSM(props, status.docID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
	}
	s.metrics.InvertedExtend(before, len(props))

	return nil
}

func (s Shard) updateInvertedIndexCleanupOldLSM(status objectInsertStatus,
	previous []byte) error {
	if !status.docIDChanged {
		// nothing to do
		return nil
	}

	// The doc id changed, so we need to analyze the previous and delete all
	// entries (immediately - since the async part is now handled inside the
	// LSMKV store, as part of merging/compaction)
	//
	// NOTE: Since Doc IDs are immutable, there is no need to use a
	// DeltaAnalyzer. docIDChanged==true, therefore the old docID is
	// "worthless" and can be cleaned up in the inverted index fully.
	previousObject, err := storobj.FromBinary(previous)
	if err != nil {
		return errors.Wrap(err, "unmarshal previous object")
	}

	previousInvertProps, err := s.analyzeObject(previousObject)
	if err != nil {
		return errors.Wrap(err, "analyze previous object")
	}

	err = s.deleteFromInvertedIndicesLSM(previousInvertProps, status.oldDocID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
	}

	return nil
}

func (s *Shard) updateDocIDLookup(tx *bolt.Tx, newID []byte,
	status objectInsertStatus) error {
	if status.docIDChanged {
		// clean up old docId first

		// in-mem
		s.deletedDocIDs.Add(status.oldDocID)

		// on-disk
		if err := docid.MarkDeletedInTx(tx, status.oldDocID); err != nil {
			return errors.Wrap(err, "remove docID->UUID index")
		}
	}

	if err := docid.AddLookupInTx(tx, docid.Lookup{
		PointsTo: newID,
		DocID:    status.docID,
	}); err != nil {
		return errors.Wrap(err, "add docID->UUID index")
	}

	return nil
}

func (s *Shard) updateDocIDLookupLSM(newID []byte,
	status objectInsertStatus) error {
	if status.docIDChanged {
		// clean up old docId first

		// in-mem
		s.deletedDocIDs.Add(status.oldDocID)

		// on-disk
		if err := docid.MarkDeletedLSM(s.store, status.oldDocID); err != nil {
			return errors.Wrap(err, "remove docID->UUID index")
		}
	}

	if err := docid.AddLookupInLSM(s.store, docid.Lookup{
		PointsTo: newID,
		DocID:    status.docID,
	}); err != nil {
		return errors.Wrap(err, "add docID->UUID index")
	}

	return nil
}

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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

func (s *Shard) putObject(ctx context.Context, object *storobj.Object) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	idBytes, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	var status objectInsertStatus

	status, err = s.putObjectLSM(object, idBytes, false)
	if err != nil {
		return errors.Wrap(err, "store object in LSM store")
	}

	// vector is now optional as of
	// https://github.com/semi-technologies/weaviate/issues/1800
	if object.Vector != nil {
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

	if err := s.propLengths.Flush(); err != nil {
		return errors.Wrap(err, "flush prop length tracker to disk")
	}

	if err := s.vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush all vector index buffered WALs")
	}

	return nil
}

func (s *Shard) updateVectorIndex(vector []float32,
	status objectInsertStatus) error {
	// on occasion, objects are updated which
	// do not have vector embeddings. in this
	// case, there is nothing to update here.
	if len(vector) == 0 {
		return nil
	}

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
	if err := s.upsertObjectDataLSM(bucket, idBytes, data, status.docID); err != nil {
		return status, errors.Wrap(err, "upsert object data")
	}
	s.metrics.PutObjectUpsertObject(before)

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
// didn't exist before), we will get a new docID from the central counter.
// Otherwise, we will reuse the previous docID and mark this as an update
func (s *Shard) determineInsertStatus(previous []byte,
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
func (s *Shard) determineMutableInsertStatus(previous []byte,
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

func (s *Shard) upsertObjectDataLSM(bucket *lsmkv.Bucket, id []byte, data []byte,
	docID uint64) error {
	keyBuf := bytes.NewBuffer(nil)
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	docIDBytes := keyBuf.Bytes()

	return bucket.Put(id, data, lsmkv.WithSecondaryKey(0, docIDBytes))
}

func (s *Shard) updateInvertedIndexLSM(object *storobj.Object,
	status objectInsertStatus, previous []byte) error {
	props, err := s.analyzeObject(object)
	if err != nil {
		return errors.Wrap(err, "analyze next object")
	}

	// TODO: metrics
	if err := s.updateInvertedIndexCleanupOldLSM(status, previous); err != nil {
		return errors.Wrap(err, "analyze and cleanup previous")
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		// update the inverted timestamp indices as well
		err = s.addIndexedTimestampsToProps(object, &props)
		if err != nil {
			return errors.Wrap(err, "add indexed timestamps to props")
		}
	}

	before := time.Now()

	err = s.extendInvertedIndicesLSM(props, status.docID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
	}
	s.metrics.InvertedExtend(before, len(props))

	if err := s.addPropLengths(props); err != nil {
		return errors.Wrap(err, "store field length values for props")
	}

	return nil
}

// addIndexedTimestampsToProps ensures that writes are indexed
// by internal timestamps
func (s *Shard) addIndexedTimestampsToProps(object *storobj.Object, props *[]inverted.Property) error {
	createTime, err := json.Marshal(object.CreationTimeUnix())
	if err != nil {
		return errors.Wrap(err, "failed to marshal _creationTimeUnix")
	}

	updateTime, err := json.Marshal(object.LastUpdateTimeUnix())
	if err != nil {
		return errors.Wrap(err, "failed to marshal _lastUpdateTimeUnix")
	}

	*props = append(*props,
		inverted.Property{
			Name:  filters.InternalPropCreationTimeUnix,
			Items: []inverted.Countable{{Data: createTime}},
		},
		inverted.Property{
			Name:  filters.InternalPropLastUpdateTimeUnix,
			Items: []inverted.Countable{{Data: updateTime}},
		},
	)

	return nil
}

func (s *Shard) updateInvertedIndexCleanupOldLSM(status objectInsertStatus,
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

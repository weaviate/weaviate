package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

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

	if !status.isUpdate {
		if err := s.vectorIndex.Add(int(status.docID), object.Vector); err != nil {
			return errors.Wrap(err, "insert to vector index")
		}
	} else {
		// fmt.Printf("skipping vector update because its an update. TODO: handle correctly\n")
	}

	return nil
}

func (s *Shard) putObjectInTx(tx *bolt.Tx, object *storobj.Object,
	idBytes []byte) (objectInsertStatus, error) {
	bucket := tx.Bucket(helpers.ObjectsBucket)
	previous := bucket.Get([]byte(idBytes))

	status, err := s.determineInsertStatus(previous)
	if err != nil {
		return status, errors.Wrap(err, "check insert/update status")
	}

	object.SetIndexID(status.docID)
	data, err := object.MarshalBinary()
	if err != nil {
		return status, errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	if err := s.upsertObjectData(bucket, idBytes, data); err != nil {
		return status, errors.Wrap(err, "upsert object data")
	}

	// build indexID->UUID lookup
	// uuid is immutable, so this does not need to be altered, cleaned on
	// updates, but is simply idempotent
	if err := s.addIndexIDLookup(tx, idBytes, status.docID); err != nil {
		return status, errors.Wrap(err, "add docID->UUID index")
	}

	if err := s.updateInvertedIndex(tx, object, status, previous); err != nil {
		return status, errors.Wrap(err, "udpate inverted indices")
	}

	return status, nil
}

type objectInsertStatus struct {
	docID    uint32
	isUpdate bool
}

// to be called with the current contents of a row, if the row is empty (i.e.
// didn't exist before, we will get a new docID from the central counter.
// Otherwise, we will will reuse the previous docID and mark this as an update
func (s Shard) determineInsertStatus(previous []byte) (objectInsertStatus, error) {
	var out objectInsertStatus

	if previous == nil {
		out.isUpdate = false
		docID, err := s.counter.GetAndInc()
		if err != nil {
			return out, errors.Wrap(err, "get new doc id from counter")
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

	return out, nil
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

		delta := inverted.Delta(previousInvertProps, nextInvertProps)
		err = s.deleteFromInvertedIndices(tx, delta.ToDelete, status.docID)
		if err != nil {
			return errors.Wrap(err, "delete obsolete inverted pointers")
		}

		invertPropsToAdd = delta.ToAdd
	}

	err = s.extendInvertedIndices(tx, invertPropsToAdd, status.docID)
	if err != nil {
		return errors.Wrap(err, "put inverted indices props")
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

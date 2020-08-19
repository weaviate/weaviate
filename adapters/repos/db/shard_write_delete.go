package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

func (s *Shard) deleteObject(ctx context.Context, id strfmt.UUID) error {
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	if err != nil {
		return err
	}

	var docID uint32
	if err := s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(helpers.ObjectsBucket)
		existing := bucket.Get([]byte(idBytes))
		if existing == nil {
			// nothing to do
			return nil
		}

		// we need the doc ID so we can clean up inverted indices currently
		// pointing to this object
		docID, err = storobj.DocIDFromBinary(existing)
		if err != nil {
			return errors.Wrap(err, "get existing doc id from object binary")
		}

		oldObj, err := storobj.FromBinary(existing)
		if err != nil {
			return errors.Wrap(err, "unmarshal existing doc")
		}

		invertedPointersToDelete, err := s.analyzeObject(oldObj)
		if err != nil {
			return errors.Wrap(err, "analyze object")
		}

		err = s.deleteFromInvertedIndices(tx, invertedPointersToDelete, docID)
		if err != nil {
			return errors.Wrap(err, "delete pointers from inverted index")
		}

		err = bucket.Delete(idBytes)
		if err != nil {
			return errors.Wrap(err, "delete object from bucket")
		}

		err = s.deleteIndexIDLookup(tx, docID)
		if err != nil {
			return errors.Wrap(err, "delete indexID->uuid lookup")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	// TODO: Delete from vector index

	return nil
}

func (s *Shard) deleteIndexIDLookup(tx *bolt.Tx, docID uint32) error {
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

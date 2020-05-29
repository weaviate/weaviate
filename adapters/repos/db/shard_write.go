package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

func (s *Shard) putObject(ctx context.Context, object *storobj.Object) error {
	idBytes, err := uuid.MustParse(object.ID().String()).MarshalBinary()
	if err != nil {
		return err
	}

	docID, err := s.counter.GetAndInc()
	if err != nil {
		return errors.Wrap(err, "get new doc id from counter")
	}
	object.SetIndexID(docID)

	data, err := object.MarshalBinary()
	if err != nil {
		return errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	invertProps, err := s.analyzeObject(object)
	if err != nil {
		return err
	}

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		// insert data object
		if err := tx.Bucket(ObjectsBucket).Put([]byte(idBytes), data); err != nil {
			return errors.Wrap(err, "put object data")
		}

		// build indexID->UUID lookup
		if err := s.addIndexIDLookup(tx, idBytes, docID); err != nil {
			return errors.Wrap(err, "put inverted indices props")
		}

		// insert inverted index props
		if err := s.extendInvertedIndices(tx, invertProps, docID); err != nil {
			return errors.Wrap(err, "put inverted indices props")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	return nil
}

func (s *Shard) analyzeObject(object *storobj.Object) ([]inverted.Property, error) {
	if object.Schema() == nil {
		return nil, nil
	}

	var schemaModel *models.Schema
	if object.Kind == kind.Thing {
		schemaModel = s.index.getSchema.GetSchemaSkipAuth().Things
	} else {
		schemaModel = s.index.getSchema.GetSchemaSkipAuth().Actions
	}

	c, err := schema.GetClassByName(schemaModel, object.Class().String())
	if err != nil {
		return nil, err
	}

	schemaMap, ok := object.Schema().(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected schema to be map, but got %T", object.Schema())
	}

	return inverted.NewAnalyzer().Object(schemaMap, c.Properties)
}

func (s *Shard) extendInvertedIndices(tx *bolt.Tx, props []inverted.Property, docID uint32) error {
	for _, prop := range props {
		b := tx.Bucket(bucketFromPropName(prop.Name))
		if b == nil {
			return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		for _, item := range prop.Items {
			if err := s.extendInvertedIndexItem(b, item, docID, item.TermFrequency); err != nil {
				return errors.Wrapf(err, "extend index with item '%s'", string(item.Data))
			}
		}
	}

	return nil
}

// extendInvertedIndexItem maintains an inverted index row for one search term,
// the structure is as follows:
//
// Bytes | Meaning
// 0..4   | count of matching documents as uint32 (little endian)
// 5..7   | doc id of first matching doc as uint32 (little endian)
// 8..11   | term frequency in first doc as float32 (little endian)
// ...
// (n-7)..(n-4) | doc id of last doc
// (n-3)..n     | term frequency of last
func (s *Shard) extendInvertedIndexItem(b *bolt.Bucket, item inverted.Countable, docID uint32, freq float32) error {
	data := b.Get(item.Data)
	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initalize counter in
		// beginning as zero
		docCount := uint32(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	}

	// append current document
	binary.Write(updated, binary.LittleEndian, &docID)
	binary.Write(updated, binary.LittleEndian, &freq)
	extended := updated.Bytes()

	// read and increase doc count
	reader := bytes.NewReader(extended)
	var docCount uint32
	binary.Read(reader, binary.LittleEndian, &docCount)
	docCount++
	countBytes := bytes.NewBuffer(make([]byte, 0, 4))
	binary.Write(countBytes, binary.LittleEndian, &docCount)

	// combine back together and save
	combined := append(countBytes.Bytes(), extended[4:]...)
	err := b.Put(item.Data, combined)
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addIndexIDLookup(tx *bolt.Tx, id []byte, docID uint32) error {
	keyBuf := bytes.NewBuffer(make([]byte, 4))
	binary.Write(keyBuf, binary.LittleEndian, &docID)
	key := keyBuf.Bytes()

	b := tx.Bucket(IndexIDBucket)
	if b == nil {
		return fmt.Errorf("no index id bucket found")
	}

	if err := b.Put(key, id); err != nil {
		return errors.Wrap(err, "store uuid for index id")
	}

	return nil
}

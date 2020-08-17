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
	"hash/crc32"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
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

	invertProps, err := s.analyzeObject(object)
	if err != nil {
		return err
	}

	var docID uint32
	var isUpdate bool

	if err := s.db.Batch(func(tx *bolt.Tx) error {

		id, update, err := s.putObjectInTx(tx, object, idBytes, invertProps)
		if err != nil {
			return err
		}

		docID = id
		isUpdate = update
		return nil
	}); err != nil {
		return errors.Wrap(err, "bolt batch tx")
	}

	if !isUpdate {
		if err := s.vectorIndex.Add(int(docID), object.Vector); err != nil {
			return errors.Wrap(err, "insert to vector index")
		}
	} else {
		// fmt.Printf("skipping vector update because its an update. TODO: handle correctly\n")
	}

	return nil
}

// return value map[int]error gives the error for the index as it received it
func (s *Shard) putObjectBatch(ctx context.Context, objects []*storobj.Object) map[int]error {
	maxPerTransaction := 30

	m := &sync.Mutex{}
	docIDs := map[strfmt.UUID]uint32{}
	errs := map[int]error{} // int represents original index

	var wg = &sync.WaitGroup{}
	for i := 0; i < len(objects); i += maxPerTransaction {
		end := i + maxPerTransaction
		if end > len(objects) {
			end = len(objects)
		}

		batch := objects[i:end]
		wg.Add(1)
		go func(i int, batch []*storobj.Object) {
			defer wg.Done()
			var affectedIndices []int
			if err := s.db.Batch(func(tx *bolt.Tx) error {
				for j := range batch {
					// so we can reference potential errors
					affectedIndices = append(affectedIndices, i+j)
				}

				for _, object := range batch {
					uuidParsed, err := uuid.Parse(object.ID().String())
					if err != nil {
						return errors.Wrap(err, "invalid id")
					}

					idBytes, err := uuidParsed.MarshalBinary()
					if err != nil {
						return err
					}

					invertProps, err := s.analyzeObject(object)
					if err != nil {
						return err
					}

					id, _, err := s.putObjectInTx(tx, object, idBytes, invertProps)
					if err != nil {
						return err
					}

					m.Lock()
					docIDs[object.ID()] = id
					m.Unlock()
				}
				return nil
			}); err != nil {
				m.Lock()
				err = errors.Wrap(err, "bolt batch tx")
				for _, affected := range affectedIndices {
					errs[affected] = err
				}
				m.Unlock()
			}
		}(i, batch)

	}
	wg.Wait()

	// TODO: is it smart to let them all run in parallel? wouldn't it be better
	// to open no more threads than we have cpu cores?
	wg = &sync.WaitGroup{}
	for i, object := range objects {
		if _, ok := errs[i]; ok {
			// had an error prior, ignore
			continue
		}

		wg.Add(1)
		docID := int(docIDs[object.ID()])
		go func(object *storobj.Object, docID int, index int) {
			defer wg.Done()

			if err := s.vectorIndex.Add(docID, object.Vector); err != nil {
				m.Lock()
				errs[index] = errors.Wrap(err, "insert to vector index")
				m.Unlock()
			}
		}(object, docID, i)
	}
	wg.Wait()

	return errs
}

func (s *Shard) putObjectInTx(tx *bolt.Tx, object *storobj.Object, idBytes []byte,
	invertProps []inverted.Property) (uint32, bool, error) {
	var docID uint32
	var isUpdate bool
	var err error

	bucket := tx.Bucket(helpers.ObjectsBucket)

	existing := bucket.Get([]byte(idBytes))
	if existing == nil {
		isUpdate = false
		docID, err = s.counter.GetAndInc()
		if err != nil {
			return docID, isUpdate, errors.Wrap(err, "get new doc id from counter")
		}
	} else {
		isUpdate = true
		docID, err = storobj.DocIDFromBinary(existing)
		if err != nil {
			return docID, isUpdate, errors.Wrap(err, "get existing doc id from object binary")
		}
	}
	object.SetIndexID(docID)

	data, err := object.MarshalBinary()
	if err != nil {
		return docID, isUpdate, errors.Wrapf(err, "marshal object %s to binary", object.ID())
	}

	// insert data object
	if err := bucket.Put([]byte(idBytes), data); err != nil {
		return docID, isUpdate, errors.Wrap(err, "put object data")
	}

	// build indexID->UUID lookup
	if err := s.addIndexIDLookup(tx, idBytes, docID); err != nil {
		return docID, isUpdate, errors.Wrap(err, "put inverted indices props")
	}

	if !isUpdate {
		// TODO gh-1221: the above is an over-simplification to make sure that on
		// an update we don't add index id duplicates, so instaed we simply don't
		// touch the invertied index at all. This essentially means right now
		// updates aren't indexed. Instead we should (as outlined in #1221)
		// calculate the delta, then explicitly add/remove where necessary

		// insert inverted index props
		if err := s.extendInvertedIndices(tx, invertProps, docID); err != nil {
			return docID, isUpdate, errors.Wrap(err, "put inverted indices props")
		}
	}

	return docID, isUpdate, nil
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
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name))
		if b == nil {
			return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		if prop.HasFrequency {
			for _, item := range prop.Items {
				if err := s.extendInvertedIndexItemWithFrequency(b, item, docID, item.TermFrequency); err != nil {
					return errors.Wrapf(err, "extend index with item '%s'", string(item.Data))
				}
			}
		} else {
			if len(prop.Items) != 1 {
				return fmt.Errorf("prop %s has no frequency but %d items", prop.Name, len(prop.Items))
			}

			if err := s.extendInvertedIndexItem(b, prop.Items[0], docID); err != nil {
				return errors.Wrapf(err, "extend index with item '%s'", string(prop.Items[0].Data))
			}

		}

	}

	return nil
}

// extendInvertedIndexItemWithFrequency maintains an inverted index row for one search term,
// the structure is as follows:
//
// Bytes | Meaning
// 0..4   | count of matching documents as uint32 (little endian)
// 5..7   | doc id of first matching doc as uint32 (little endian)
// 8..11   | term frequency in first doc as float32 (little endian)
// ...
// (n-7)..(n-4) | doc id of last doc
// (n-3)..n     | term frequency of last
func (s *Shard) extendInvertedIndexItemWithFrequency(b *bolt.Bucket, item inverted.Countable, docID uint32, freq float32) error {
	data := b.Get(item.Data)
	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initalize counter in
		// beginning as zero
		docCount := uint32(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	} else {
		// remove the old checksum
		data = data[4:]
		updated = bytes.NewBuffer(data)
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

	// combine back together
	combined := append(countBytes.Bytes(), extended[4:]...)

	// finally calculate the checksum and prepend one more time.
	chksum, err := s.checksum(combined)
	if err != nil {
		return err
	}

	combined = append(chksum, combined...)

	err = b.Put(item.Data, combined)
	if err != nil {
		return err
	}

	return nil
}

// extendInvertedIndexItem maintains an inverted index row for one search term,
// the structure is as follows:
//
// Bytes | Meaning
// 0..4   | count of matching documents as uint32 (little endian)
// 5..7   | doc id of first matching doc as uint32 (little endian)
// ...
// (n-3)..n | doc id of last doc
func (s *Shard) extendInvertedIndexItem(b *bolt.Bucket, item inverted.Countable, docID uint32) error {
	data := b.Get(item.Data)
	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initalize counter in
		// beginning as zero
		docCount := uint32(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	} else {
		// remove the old checksum
		data = data[4:]
		updated = bytes.NewBuffer(data)
	}

	// append current document
	binary.Write(updated, binary.LittleEndian, &docID)
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

	// finally calculate the checksum and prepend one more time.
	chksum, err := s.checksum(combined)
	if err != nil {
		return err
	}

	combined = append(chksum, combined...)
	err = b.Put(item.Data, combined)
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) checksum(in []byte) ([]byte, error) {
	checksum := crc32.ChecksumIEEE(in)
	buf := bytes.NewBuffer(make([]byte, 0, 4))
	err := binary.Write(buf, binary.LittleEndian, &checksum)
	return buf.Bytes(), err
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

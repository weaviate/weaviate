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
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

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

	return inverted.NewAnalyzer().Object(schemaMap, c.Properties, object.ID())
}

func (s *Shard) extendInvertedIndices(tx *bolt.Tx, props []inverted.Property,
	docID uint64) error {
	for _, prop := range props {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name))
		if b == nil {
			return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		if prop.HasFrequency {
			for _, item := range prop.Items {
				if err := s.extendInvertedIndexItemWithFrequency(b, item,
					docID, item.TermFrequency); err != nil {
					return errors.Wrapf(err, "extend index with item '%s'",
						string(item.Data))
				}
			}
		} else {
			for _, item := range prop.Items {
				if err := s.extendInvertedIndexItem(b, item, docID); err != nil {
					return errors.Wrapf(err, "extend index with item '%s'",
						string(item.Data))
				}
			}
		}
	}

	return nil
}

func (s *Shard) sliceToMap(in []uint64) map[uint64]struct{} {
	out := map[uint64]struct{}{}
	for i := range in {
		out[in[i]] = struct{}{}
	}
	return out
}

func (s *Shard) tryDeleteFromInvertedIndicesProp(b *bolt.Bucket,
	item inverted.Countable, docIDs []uint64, hasFrequency bool) error {
	data := b.Get(item.Data)
	if len(data) == 0 {
		// we want to delete from an empty row. Nothing to do
		return nil
	}
	deletedDocIDs := s.sliceToMap(docIDs)

	performDelete := false
	if len(data) > 23 {
		propDocIDs := data[16:]
		divider := 8
		if hasFrequency {
			divider = 16
		}
		numberOfPropDocIDs := len(propDocIDs) / divider
		for i := 0; i < numberOfPropDocIDs; i++ {
			indx := i * divider
			propDocID := binary.LittleEndian.Uint64(propDocIDs[indx : indx+8])
			if _, foundDeleted := deletedDocIDs[propDocID]; foundDeleted {
				performDelete = true
				break
			}
		}
	}

	if performDelete {
		return s.deleteFromInvertedIndicesProp(b, item, deletedDocIDs, hasFrequency)
	}

	return nil
}

func (s *Shard) deleteFromInvertedIndicesProp(b *bolt.Bucket,
	item inverted.Countable, docIDs map[uint64]struct{}, hasFrequency bool) error {
	data := b.Get(item.Data)
	if len(data) == 0 {
		// we want to delete from an empty row. Nothing to do
		return nil
	}

	// remove the old checksum and doc count (0-8 = checksum, 9-16=docCount)
	data = data[16:]
	r := bytes.NewReader(data)

	newDocCount := uint64(0)
	newRow := bytes.NewBuffer(nil)
	for {
		nextDocIDBytes := make([]byte, 8)
		_, err := r.Read(nextDocIDBytes)
		if err != nil {
			if err == io.EOF {
				break
			}

			return errors.Wrap(err, "read doc id")
		}

		var nextDocID uint64
		if err := binary.Read(bytes.NewReader(nextDocIDBytes), binary.LittleEndian,
			&nextDocID); err != nil {
			return errors.Wrap(err, "read doc id from binary")
		}

		frequencyBytes := make([]byte, 8)
		if hasFrequency {
			// always read frequency if the property has one, so the reader offset is
			// correct for the next round., i.e.only skip the loop after reading all
			// contents
			if n, err := r.Read(frequencyBytes); err != nil {
				return errors.Wrapf(err, "read frequency (%d bytes)", n)
			}
		}

		_, isDeleted := docIDs[nextDocID]
		if isDeleted {
			// we have found the one we want to delete, i.e. not copy into the
			// updated list
			continue
		}

		newDocCount++
		if _, err := newRow.Write(nextDocIDBytes); err != nil {
			return errors.Wrap(err, "write doc")
		}

		if hasFrequency {
			if _, err := newRow.Write(frequencyBytes); err != nil {
				return errors.Wrap(err, "write frequency")
			}
		}
	}

	countBytes := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(countBytes, binary.LittleEndian, &newDocCount)

	// combine back together
	combined := append(countBytes.Bytes(), newRow.Bytes()...)

	// finally calculate the checksum and prepend one more time.
	chksum, err := s.checksum(combined)
	if err != nil {
		return err
	}

	combined = append(chksum, combined...)
	if len(combined) != 0 && len(combined) > 0 {
		// -16 to remove the checksum and doc count
		// module 8 for 8 bytes of docID if no frequency
		// module 16 for 16 bytes of docID if frequency
		if hasFrequency && (len(combined)-16)%16 != 0 {
			return fmt.Errorf("sanity check: invert row has invalid updated length %d"+
				"with original length %d", len(combined), len(data))
		}
		if !hasFrequency && (len(combined)-16)%8 != 0 {
			return fmt.Errorf("sanity check: invert row has invalid updated length %d"+
				"with original length %d", len(combined), len(data))
		}
	}

	err = b.Put(item.Data, combined)
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) extendInvertedIndexItemWithOptionalFrequency(b *bolt.Bucket,
	item inverted.Countable, docID uint64, freq *float64) error {
	data := b.Get(item.Data)

	updated := bytes.NewBuffer(data)
	if len(data) == 0 {
		// this is the first time someones writing this row, initialize counter in
		// beginning as zero, and a dummy checksum
		updated.Write([]uint8{0, 0, 0, 0, 0, 0, 0, 0}) // dummy checksum
		docCount := uint64(0)
		binary.Write(updated, binary.LittleEndian, &docCount)
	}
	// append current document
	if err := binary.Write(updated, binary.LittleEndian, &docID); err != nil {
		return errors.Wrap(err, "write doc id")
	}
	if freq != nil {
		if err := binary.Write(updated, binary.LittleEndian, freq); err != nil {
			return errors.Wrap(err, "write doc frequency")
		}
	}

	extended := updated.Bytes()

	// read and increase doc count
	reader := bytes.NewReader(extended[8:])
	var docCount uint64
	binary.Read(reader, binary.LittleEndian, &docCount)
	docCount++
	countBuf := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(countBuf, binary.LittleEndian, &docCount)

	// overwrite old doc count

	startPos := 8 // first 8 bytes are checksum, so 8-15 is count
	countBytes := countBuf.Bytes()
	for i := 0; i < 8; i++ {
		extended[startPos+i] = countBytes[i]
	}

	// finally calculate the checksum and prepend one more time.
	chksum, err := s.checksum(extended[8:])
	if err != nil {
		return err
	}

	// overwrite first eight bytes with checksum
	startPos = 0 // first 8 bytes are checksum
	for i := 0; i < 8; i++ {
		extended[startPos+i] = chksum[i]
	}

	lengthOfOneEntry := 8
	if freq != nil {
		lengthOfOneEntry = 16
	}
	if len(extended) != 0 && len(extended) > 16 && (len(extended)-16)%lengthOfOneEntry != 0 {
		// -16 to remove the checksum and doc count
		// module 16 for 8 bytes of docID + frequency or alternatively 8 without the
		// frequency
		return fmt.Errorf("sanity check: invert row has invalid updated length %d"+
			"with original length %d", len(extended), len(data))
	}

	err = b.Put(item.Data, extended)
	if err != nil {
		return err
	}

	return nil
}

// Bytes  | Meaning
//  0..8  | checksum
//  9..16 | count of matching documents as uint64 (little endian)
// 17..24 | doc id of first matching doc as uint64 (little endian)
// ...
// (n-7)..n | doc id of last doc
func (s *Shard) extendInvertedIndexItem(b *bolt.Bucket, item inverted.Countable,
	docID uint64) error {
	return s.extendInvertedIndexItemWithOptionalFrequency(b, item, docID, nil)
}

// extendInvertedIndexItemWithFrequency maintains an inverted index row for one
// search term,
// the structure is as follows:
//
// Bytes  | Meaning
//  0..8  | checksum
//  9..16 | count of matching documents as uint64 (little endian)
// 17..24 | doc id of first matching doc as uint64 (little endian)
// 25..32 | term frequency in first doc as float64 (little endian)
// ...
// (n-15)..(n-8) | doc id of last doc
// (n-7)..n      | term frequency of last
func (s *Shard) extendInvertedIndexItemWithFrequency(b *bolt.Bucket,
	item inverted.Countable, docID uint64, freq float64) error {
	return s.extendInvertedIndexItemWithOptionalFrequency(b, item, docID, &freq)
}

func (s *Shard) checksum(in []byte) ([]byte, error) {
	checksum := crc64.Checksum(in, crc64.MakeTable(crc64.ISO))
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	err := binary.Write(buf, binary.LittleEndian, &checksum)
	return buf.Bytes(), err
}

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

package inverted

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/notimplemented"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (fs *Searcher) docPointers(id []byte, operator filters.Operator,
	b *bolt.Bucket, value []byte, limit int,
	hasFrequency bool) (docPointers, error) {
	switch operator {
	case filters.OperatorEqual:
		return fs.docPointersEqual(id, b, value, limit, hasFrequency)
	case filters.OperatorNotEqual:
		return fs.docPointersNotEqual(id, b, value, limit, hasFrequency)
	case filters.OperatorGreaterThan:
		return fs.docPointersGreaterThan(id, b, value, limit, hasFrequency, false)
	case filters.OperatorGreaterThanEqual:
		return fs.docPointersGreaterThan(id, b, value, limit, hasFrequency, true)
	case filters.OperatorLessThan:
		return fs.docPointersLessThan(id, b, value, limit, hasFrequency, false)
	case filters.OperatorLessThanEqual:
		return fs.docPointersLessThan(id, b, value, limit, hasFrequency, true)
	default:
		return docPointers{}, fmt.Errorf("operator not supported (yet) in standalone "+
			"mode, see %s for details", notimplemented.Link)
	}
}

func (fs *Searcher) docPointersEqual(prop []byte, b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	return fs.parseInvertedIndexRow(rowID(prop, value), b.Get(value),
		limit, hasFrequency)
}

func (fs *Searcher) docPointersGreaterThan(prop []byte, b *bolt.Bucket,
	value []byte, limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	var hashes [][]byte

	for k, v := c.Seek(value); k != nil; k, v = c.Next() {
		if bytes.Equal(k, value) && !allowEqual {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(rowID(prop, k), v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "greater than: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		hashes = append(hashes, curr.checksum)
		if limit > 0 && pointers.count >= uint32(limit) {
			break
		}
	}
	newChecksum, err := fs.combineChecksums(hashes)
	if err != nil {
		return pointers, errors.Wrap(err, "greater than: calculate new checksum")
	}

	pointers.checksum = newChecksum
	return pointers, nil
}

func (fs *Searcher) docPointersLessThan(prop []byte, b *bolt.Bucket,
	value []byte, limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	var hashes [][]byte

	for k, v := c.First(); k != nil && bytes.Compare(k, value) != 1; k, v = c.Next() {
		if bytes.Equal(k, value) && !allowEqual {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(rowID(prop, k), v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "less than: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		hashes = append(hashes, curr.checksum)
		if pointers.count >= uint32(limit) {
			break
		}
	}

	newChecksum, err := fs.combineChecksums(hashes)
	if err != nil {
		return pointers, errors.Wrap(err, "less than: calculate new checksum")
	}

	pointers.checksum = newChecksum
	return pointers, nil
}

func (fs *Searcher) docPointersNotEqual(prop []byte, b *bolt.Bucket,
	value []byte, limit int, hasFrequency bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	var hashes [][]byte

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if bytes.Equal(k, value) {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(rowID(prop, k), v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "not equal: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		hashes = append(hashes, curr.checksum)
		if pointers.count >= uint32(limit) {
			break
		}
	}

	newChecksum, err := fs.combineChecksums(hashes)
	if err != nil {
		return pointers, errors.Wrap(err, "not equal: calculate new checksum")
	}

	pointers.checksum = newChecksum

	return pointers, nil
}

func rowID(prop, value []byte) []byte {
	return append(prop, value...)
}

// why is there a need to combine checksums prior to merging?
// on Operators GreaterThan (Equal) & LessThan (Equal), we don't just read a
// single row in the inverted index, but several (e.g. for greater than 5, we
// right read the row containing 5, 6, 7 and so on. Since a field contains just
// one value the docIDs are guarnateed to be unique, we can simply append them.
// But to be able to recognize this read operation further than the line (e.g.
// when merging independent filter) we need a new checksum describing exactly
// this request. Thus we simply treat the existing checksums as an input string
// (appended) and caculate a new one
func (fs *Searcher) combineChecksums(checksums [][]byte) ([]byte, error) {
	if len(checksums) == 1 {
		return checksums[0], nil
	}

	var total []byte
	for _, chksum := range checksums {
		total = append(total, chksum...)
	}

	newChecksum := crc32.ChecksumIEEE(total)
	buf := bytes.NewBuffer(make([]byte, 0, 4))
	err := binary.Write(buf, binary.LittleEndian, &newChecksum)
	return buf.Bytes(), err
}

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
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (fs *Searcher) docPointers(prop []byte, operator filters.Operator,
	b *bolt.Bucket, value []byte, limit int,
	hasFrequency bool) (docPointers, error) {
	rr := NewRowReader(b, value, operator)

	var pointers docPointers
	var hashes [][]byte

	if err := rr.Read(context.TODO(), func(k, v []byte) (bool, error) {
		curr, err := fs.parseInvertedIndexRow(rowID(prop, k), v, limit, hasFrequency)
		if err != nil {
			return false, errors.Wrap(err, "parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		hashes = append(hashes, curr.checksum)
		if limit > 0 && pointers.count >= uint32(limit) {
			return false, nil
		}

		return true, nil
	}); err != nil {
		return pointers, errors.Wrap(err, "read row")
	}

	newChecksum, err := fs.combineChecksums(hashes)
	if err != nil {
		return pointers, errors.Wrap(err, "greater than: calculate new checksum")
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
// one value the docIDs are guaranteed to be unique, we can simply append them.
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

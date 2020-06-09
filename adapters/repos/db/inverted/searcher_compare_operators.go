//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (fs *Searcher) docPointers(operator filters.Operator, b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	switch operator {
	case filters.OperatorEqual:
		return fs.docPointersEqual(b, value, limit, hasFrequency)
	case filters.OperatorNotEqual:
		return fs.docPointersNotEqual(b, value, limit, hasFrequency)
	case filters.OperatorGreaterThan:
		return fs.docPointersGreaterThan(b, value, limit, hasFrequency, false)
	case filters.OperatorGreaterThanEqual:
		return fs.docPointersGreaterThan(b, value, limit, hasFrequency, true)
	case filters.OperatorLessThan:
		return fs.docPointersLessThan(b, value, limit, hasFrequency, false)
	case filters.OperatorLessThanEqual:
		return fs.docPointersLessThan(b, value, limit, hasFrequency, true)
	default:
		return docPointers{}, fmt.Errorf("operator not supported (yet)")
	}
}

func (fs *Searcher) docPointersEqual(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	return fs.parseInvertedIndexRow(b.Get(value), limit, hasFrequency)
}

func (fs *Searcher) docPointersGreaterThan(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	for k, v := c.Seek(value); k != nil; k, v = c.Next() {
		if bytes.Equal(k, value) && !allowEqual {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "greater than: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		if pointers.count >= uint32(limit) {
			break
		}
	}

	return pointers, nil
}

func (fs *Searcher) docPointersLessThan(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	for k, v := c.First(); k != nil && bytes.Compare(k, value) != 1; k, v = c.Next() {
		if bytes.Equal(k, value) && !allowEqual {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "less than: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		if pointers.count >= uint32(limit) {
			break
		}
	}

	return pointers, nil
}

func (fs *Searcher) docPointersNotEqual(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	c := b.Cursor()
	var pointers docPointers
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if bytes.Equal(k, value) {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "not equal: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
		if pointers.count >= uint32(limit) {
			break
		}
	}

	return pointers, nil
}

package filtersearcher

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func (fs *FilterSearcher) docPointers(operator filters.Operator, b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	switch operator {
	case filters.OperatorEqual:
		return fs.docPointersEqual(b, value, limit, hasFrequency)
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

func (fs *FilterSearcher) docPointersEqual(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool) (docPointers, error) {
	return fs.parseInvertedIndexRow(b.Get(value), limit, hasFrequency)
}
func (fs *FilterSearcher) docPointersGreaterThan(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	return fs.docPointersCompare(b, c, c.Next, value, limit, hasFrequency, allowEqual)
}

func (fs *FilterSearcher) docPointersLessThan(b *bolt.Bucket, value []byte,
	limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {
	c := b.Cursor()
	return fs.docPointersCompare(b, c, c.Prev, value, limit, hasFrequency, allowEqual)
}

func (fs *FilterSearcher) docPointersCompare(b *bolt.Bucket, c *bolt.Cursor, cursorStep func() ([]byte, []byte),
	value []byte, limit int, hasFrequency bool, allowEqual bool) (docPointers, error) {

	var pointers docPointers
	for k, v := c.Seek(value); k != nil; k, v = cursorStep() {
		if bytes.Equal(k, value) && !allowEqual {
			continue
		}

		curr, err := fs.parseInvertedIndexRow(v, limit, hasFrequency)
		if err != nil {
			return pointers, errors.Wrap(err, "greater than: parse inverted index row")
		}

		pointers.count += curr.count
		pointers.docIDs = append(pointers.docIDs, curr.docIDs...)
	}

	return pointers, nil
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

func TestRowReaderRoaringSet(t *testing.T) {
	data := []kvData{
		{"aaa", []uint64{1, 2, 3}},
		{"bbb", []uint64{11, 22, 33}},
		{"ccc", []uint64{111, 222, 333}},
		{"ddd", []uint64{1111, 2222, 3333}},
		{"eee", []uint64{11111, 22222, 33333}},
		{"fff", []uint64{111111, 222222, 333333}},
		{"ggg", []uint64{1111111, 2222222, 3333333}},
		{"hhh", []uint64{11111111, 2222222, 33333333}},
	}
	ctx := context.Background()

	testcases := []struct {
		name     string
		value    string
		operator filters.Operator
		expected []kvData
	}{
		{
			name:     "equal 'ggg' value",
			value:    "ggg",
			operator: filters.OperatorEqual,
			expected: []kvData{
				{"ggg", []uint64{1111111, 2222222, 3333333}},
			},
		},
		{
			name:     "not equal 'ccc' value",
			value:    "ccc",
			operator: filters.OperatorNotEqual,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "not equal non-matching value",
			value:    "fgh",
			operator: filters.OperatorNotEqual,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ccc", []uint64{111, 222, 333}},
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "greater than 'ddd' value",
			value:    "ddd",
			operator: filters.OperatorGreaterThan,
			expected: []kvData{
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "greater than equal 'ddd' value",
			value:    "ddd",
			operator: filters.OperatorGreaterThanEqual,
			expected: []kvData{
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "greater than non-matching value",
			value:    "fgh",
			operator: filters.OperatorGreaterThan,
			expected: []kvData{
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "greater than equal non-matching value",
			value:    "fgh",
			operator: filters.OperatorGreaterThanEqual,
			expected: []kvData{
				{"ggg", []uint64{1111111, 2222222, 3333333}},
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
		{
			name:     "less than 'eee' value",
			value:    "eee",
			operator: filters.OperatorLessThan,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ccc", []uint64{111, 222, 333}},
				{"ddd", []uint64{1111, 2222, 3333}},
			},
		},
		{
			name:     "less than equal 'eee' value",
			value:    "eee",
			operator: filters.OperatorLessThanEqual,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ccc", []uint64{111, 222, 333}},
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
			},
		},
		{
			name:     "less than non-matching value",
			value:    "fgh",
			operator: filters.OperatorLessThan,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ccc", []uint64{111, 222, 333}},
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
			},
		},
		{
			name:     "less than equal non-matching value",
			value:    "fgh",
			operator: filters.OperatorLessThanEqual,
			expected: []kvData{
				{"aaa", []uint64{1, 2, 3}},
				{"bbb", []uint64{11, 22, 33}},
				{"ccc", []uint64{111, 222, 333}},
				{"ddd", []uint64{1111, 2222, 3333}},
				{"eee", []uint64{11111, 22222, 33333}},
				{"fff", []uint64{111111, 222222, 333333}},
			},
		},
		{
			name:     "like '*b' value",
			value:    "*b",
			operator: filters.OperatorLike,
			expected: []kvData{
				{"bbb", []uint64{11, 22, 33}},
			},
		},
		{
			name:     "like 'h*' value",
			value:    "h*",
			operator: filters.OperatorLike,
			expected: []kvData{
				{"hhh", []uint64{11111111, 2222222, 33333333}},
			},
		},
	}

	for _, tc := range testcases {
		type readResult struct {
			k []byte
			v *sroar.Bitmap
		}

		t.Run(tc.name, func(t *testing.T) {
			result := []readResult{}
			rowReader := createRowReaderRoaringSet([]byte(tc.value), tc.operator, data)
			rowReader.Read(ctx, func(k []byte, v *sroar.Bitmap) (bool, error) {
				result = append(result, readResult{k, v})
				return true, nil
			})

			assert.Len(t, result, len(tc.expected))
			for i, expectedKV := range tc.expected {
				assert.Equal(t, []byte(expectedKV.k), result[i].k)
				assert.Equal(t, len(expectedKV.v), result[i].v.GetCardinality())
				for _, expectedV := range expectedKV.v {
					assert.True(t, result[i].v.Contains(expectedV))
				}
			}
		})

		t.Run(tc.name+" with 3 results limit", func(t *testing.T) {
			limit := 3
			expected := tc.expected
			if len(tc.expected) > limit {
				expected = tc.expected[:limit]
			}

			result := []readResult{}
			rowReader := createRowReaderRoaringSet([]byte(tc.value), tc.operator, data)
			rowReader.Read(ctx, func(k []byte, v *sroar.Bitmap) (bool, error) {
				result = append(result, readResult{k, v})
				if len(result) >= limit {
					return false, nil
				}
				return true, nil
			})

			assert.Len(t, result, len(expected))
			for i, expectedKV := range expected {
				assert.Equal(t, []byte(expectedKV.k), result[i].k)
				assert.Equal(t, len(expectedKV.v), result[i].v.GetCardinality())
				for _, expectedV := range expectedKV.v {
					assert.True(t, result[i].v.Contains(expectedV))
				}
			}
		})
	}
}

type kvData struct {
	k string
	v []uint64
}

type dummyCursorRoaringSet struct {
	data   []kvData
	pos    int
	closed bool
}

func (c *dummyCursorRoaringSet) First() ([]byte, *sroar.Bitmap) {
	c.pos = 0
	return c.Next()
}

func (c *dummyCursorRoaringSet) Next() ([]byte, *sroar.Bitmap) {
	bm := sroar.NewBitmap()
	if c.pos >= len(c.data) {
		return nil, bm
	}
	pos := c.pos
	c.pos++
	bm.SetMany(c.data[pos].v)
	return []byte(c.data[pos].k), bm
}

func (c *dummyCursorRoaringSet) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	pos := -1
	for i := 0; i < len(c.data); i++ {
		if bytes.Compare([]byte(c.data[i].k), key) >= 0 {
			pos = i
			break
		}
	}
	if pos < 0 {
		return nil, sroar.NewBitmap()
	}
	c.pos = pos
	return c.Next()
}

func (c *dummyCursorRoaringSet) Close() {
	c.closed = true
}

func createRowReaderRoaringSet(value []byte, operator filters.Operator, data []kvData) *RowReaderRoaringSet {
	return &RowReaderRoaringSet{
		value:     value,
		operator:  operator,
		newCursor: func() lsmkv.CursorRoaringSet { return &dummyCursorRoaringSet{data: data} },
		getter: func(key []byte) (*sroar.Bitmap, error) {
			for i := 0; i < len(data); i++ {
				if bytes.Equal([]byte(data[i].k), key) {
					return roaringset.NewBitmap(data[i].v...), nil
				}
			}
			return nil, entlsmkv.NotFound
		},
	}
}

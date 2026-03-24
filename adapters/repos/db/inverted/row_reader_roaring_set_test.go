//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

const maxDocID = 33333333

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
				{"ccc", func() []uint64 {
					bm := sroar.Prefill(maxDocID)
					for _, x := range []uint64{111, 222, 333} {
						bm.Remove(x)
					}
					return bm.ToArray()
				}()},
			},
		},
		{
			name:     "not equal non-matching value",
			value:    "fgh",
			operator: filters.OperatorNotEqual,
			expected: []kvData{},
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
			r func()
		}

		t.Run(tc.name, func(t *testing.T) {
			result := []readResult{}
			rowReader := createRowReaderRoaringSet([]byte(tc.value), tc.operator, nil, data)
			rowReader.Read(ctx, func(k []byte, v *sroar.Bitmap, release func()) (bool, error) {
				result = append(result, readResult{k, v, release})
				return true, nil
			})

			assert.Len(t, result, len(tc.expected))
			for i, expectedKV := range tc.expected {
				assert.Equal(t, []byte(expectedKV.k), result[i].k)
				for _, expectedV := range expectedKV.v {
					assert.True(t, result[i].v.Contains(expectedV) != rowReader.isDenyList)
				}
				result[i].r()
			}
		})

		t.Run(tc.name+" with 3 results limit", func(t *testing.T) {
			limit := 3
			expected := tc.expected
			if len(tc.expected) > limit {
				expected = tc.expected[:limit]
			}

			result := []readResult{}
			rowReader := createRowReaderRoaringSet([]byte(tc.value), tc.operator, nil, data)
			rowReader.Read(ctx, func(k []byte, v *sroar.Bitmap, release func()) (bool, error) {
				result = append(result, readResult{k, v, release})
				if len(result) >= limit {
					return false, nil
				}
				return true, nil
			})

			assert.Len(t, result, len(expected))
			for i, expectedKV := range expected {
				assert.Equal(t, []byte(expectedKV.k), result[i].k)
				for _, expectedV := range expectedKV.v {
					assert.True(t, result[i].v.Contains(expectedV) != rowReader.isDenyList)
				}
				result[i].r()
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

// createRowReaderRoaringSet is the single base constructor for all row reader
// tests. keyPrefix may be nil for flat-property (no prefix) tests.
func createRowReaderRoaringSet(value []byte, operator filters.Operator,
	keyPrefix []byte, data []kvData,
) *RowReaderRoaringSet {
	return &RowReaderRoaringSet{
		value:     value,
		operator:  operator,
		keyPrefix: keyPrefix,
		newCursor: func() lsmkv.CursorRoaringSet { return &dummyCursorRoaringSet{data: data} },
		getter: func(key []byte) (*sroar.Bitmap, func(), error) {
			for i := range data {
				if bytes.Equal([]byte(data[i].k), key) {
					return roaringset.NewBitmap(data[i].v...), noopRelease, nil
				}
			}
			return nil, noopRelease, entlsmkv.NotFound
		},
	}
}


// TestRowReaderRoaringSetWithPrefix verifies that cursor-based reads stay
// within the bounds of the supplied key prefix and do not bleed into entries
// belonging to other prefixes stored in the same bucket.
//
// The bucket contains three groups of entries (sorted by full key):
//
//	prefix0 (0x00): "aaa", "bbb"                  ← sorts BEFORE prefixA
//	prefixA (0x01): "aaa", "bbb", "ccc", "ddd"    ← middle prefix
//	prefixB (0x02): "aaa", "bbb"                  ← sorts AFTER prefixA
//
// All three prefixes are tested to cover edge cases at the start, middle,
// and end of the bucket. Both returned keys and docIDs are validated.
func TestRowReaderRoaringSetWithPrefix(t *testing.T) {
	prefix0 := []byte{0x00}
	prefixA := []byte{0x01}
	prefixB := []byte{0x02}

	pk := func(prefix []byte, val string) string {
		return string(append(append([]byte{}, prefix...), val...))
	}

	// Bucket data — must be in ascending key order for the dummy cursor.
	data := []kvData{
		{pk(prefix0, "aaa"), []uint64{901, 902}},
		{pk(prefix0, "bbb"), []uint64{903, 904}},
		{pk(prefixA, "aaa"), []uint64{1, 2, 3}},
		{pk(prefixA, "bbb"), []uint64{11, 22, 33}},
		{pk(prefixA, "ccc"), []uint64{111, 222, 333}},
		{pk(prefixA, "ddd"), []uint64{1111, 2222, 3333}},
		{pk(prefixB, "aaa"), []uint64{801, 802}},
		{pk(prefixB, "bbb"), []uint64{803, 804}},
	}

	ctx := context.Background()

	tests := []struct {
		name          string
		prefix        []byte
		value         []byte
		operator      filters.Operator
		expected      []kvData // bare key (no prefix) + expected docIDs
		wantDenyList  bool
	}{
		// prefix0 — first prefix in bucket
		// greaterThan must not bleed into prefixA; lessThan has nothing before it.
		{
			name: "prefix0/equal",
			prefix: prefix0, value: []byte("aaa"), operator: filters.OperatorEqual,
			expected: []kvData{{"aaa", []uint64{901, 902}}},
		},
		{
			name: "prefix0/greaterThan no bleed into prefixA",
			prefix: prefix0, value: []byte("bbb"), operator: filters.OperatorGreaterThan,
			expected: []kvData{},
		},
		{
			name: "prefix0/greaterThanEqual",
			prefix: prefix0, value: []byte("bbb"), operator: filters.OperatorGreaterThanEqual,
			expected: []kvData{{"bbb", []uint64{903, 904}}},
		},
		{
			name: "prefix0/lessThan nothing before first entry",
			prefix: prefix0, value: []byte("aaa"), operator: filters.OperatorLessThan,
			expected: []kvData{},
		},
		{
			name: "prefix0/lessThan",
			prefix: prefix0, value: []byte("bbb"), operator: filters.OperatorLessThan,
			expected: []kvData{{"aaa", []uint64{901, 902}}},
		},
		{
			name: "prefix0/like full wildcard",
			prefix: prefix0, value: []byte("*"), operator: filters.OperatorLike,
			expected: []kvData{{"aaa", []uint64{901, 902}}, {"bbb", []uint64{903, 904}}},
		},
		{
			name: "prefix0/notEqual",
			prefix: prefix0, value: []byte("aaa"), operator: filters.OperatorNotEqual,
			expected:     []kvData{{"aaa", []uint64{901, 902}}},
			wantDenyList: true,
		},

		// prefixA — middle prefix, bleed possible in both directions
		{
			name: "prefixA/equal",
			prefix: prefixA, value: []byte("bbb"), operator: filters.OperatorEqual,
			expected: []kvData{{"bbb", []uint64{11, 22, 33}}},
		},
		{
			name: "prefixA/greaterThan stops at upper boundary",
			prefix: prefixA, value: []byte("bbb"), operator: filters.OperatorGreaterThan,
			expected: []kvData{{"ccc", []uint64{111, 222, 333}}, {"ddd", []uint64{1111, 2222, 3333}}},
		},
		{
			name: "prefixA/greaterThanEqual",
			prefix: prefixA, value: []byte("bbb"), operator: filters.OperatorGreaterThanEqual,
			expected: []kvData{{"bbb", []uint64{11, 22, 33}}, {"ccc", []uint64{111, 222, 333}}, {"ddd", []uint64{1111, 2222, 3333}}},
		},
		{
			name: "prefixA/greaterThan last entry no bleed into prefixB",
			prefix: prefixA, value: []byte("ddd"), operator: filters.OperatorGreaterThan,
			expected: []kvData{},
		},
		{
			name: "prefixA/lessThan starts at lower boundary no bleed into prefix0",
			prefix: prefixA, value: []byte("ccc"), operator: filters.OperatorLessThan,
			expected: []kvData{{"aaa", []uint64{1, 2, 3}}, {"bbb", []uint64{11, 22, 33}}},
		},
		{
			name: "prefixA/lessThanEqual",
			prefix: prefixA, value: []byte("ccc"), operator: filters.OperatorLessThanEqual,
			expected: []kvData{{"aaa", []uint64{1, 2, 3}}, {"bbb", []uint64{11, 22, 33}}, {"ccc", []uint64{111, 222, 333}}},
		},
		{
			name: "prefixA/lessThan first entry no bleed into prefix0",
			prefix: prefixA, value: []byte("aaa"), operator: filters.OperatorLessThan,
			expected: []kvData{},
		},
		{
			name: "prefixA/like wildcard-suffix stays within prefix",
			prefix: prefixA, value: []byte("b*"), operator: filters.OperatorLike,
			expected: []kvData{{"bbb", []uint64{11, 22, 33}}},
		},
		{
			name: "prefixA/like wildcard-prefix stays within prefix",
			prefix: prefixA, value: []byte("*b"), operator: filters.OperatorLike,
			expected: []kvData{{"bbb", []uint64{11, 22, 33}}},
		},
		{
			name: "prefixA/like full wildcard returns only prefixA entries",
			prefix: prefixA, value: []byte("*"), operator: filters.OperatorLike,
			expected: []kvData{{"aaa", []uint64{1, 2, 3}}, {"bbb", []uint64{11, 22, 33}}, {"ccc", []uint64{111, 222, 333}}, {"ddd", []uint64{1111, 2222, 3333}}},
		},
		{
			name: "prefixA/notEqual",
			prefix: prefixA, value: []byte("bbb"), operator: filters.OperatorNotEqual,
			expected:     []kvData{{"bbb", []uint64{11, 22, 33}}},
			wantDenyList: true,
		},

		// prefixB — last prefix in bucket
		// greaterThan past last entry must return nothing; lessThan must not bleed into prefixA.
		{
			name: "prefixB/equal",
			prefix: prefixB, value: []byte("aaa"), operator: filters.OperatorEqual,
			expected: []kvData{{"aaa", []uint64{801, 802}}},
		},
		{
			name: "prefixB/greaterThan nothing past last entry",
			prefix: prefixB, value: []byte("bbb"), operator: filters.OperatorGreaterThan,
			expected: []kvData{},
		},
		{
			name: "prefixB/greaterThan",
			prefix: prefixB, value: []byte("aaa"), operator: filters.OperatorGreaterThan,
			expected: []kvData{{"bbb", []uint64{803, 804}}},
		},
		{
			name: "prefixB/lessThan no bleed into prefixA",
			prefix: prefixB, value: []byte("aaa"), operator: filters.OperatorLessThan,
			expected: []kvData{},
		},
		{
			name: "prefixB/lessThanEqual",
			prefix: prefixB, value: []byte("bbb"), operator: filters.OperatorLessThanEqual,
			expected: []kvData{{"aaa", []uint64{801, 802}}, {"bbb", []uint64{803, 804}}},
		},
		{
			name: "prefixB/like full wildcard returns only prefixB entries",
			prefix: prefixB, value: []byte("*"), operator: filters.OperatorLike,
			expected: []kvData{{"aaa", []uint64{801, 802}}, {"bbb", []uint64{803, 804}}},
		},
		{
			name: "prefixB/notEqual",
			prefix: prefixB, value: []byte("aaa"), operator: filters.OperatorNotEqual,
			expected:     []kvData{{"aaa", []uint64{801, 802}}},
			wantDenyList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := createRowReaderRoaringSet(tt.value, tt.operator, tt.prefix, data)

			got := []kvData{}
			err := rr.Read(ctx, func(k []byte, v *sroar.Bitmap, release func()) (bool, error) {
				got = append(got, kvData{string(k), v.ToArray()})
				return true, nil
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
			assert.Equal(t, tt.wantDenyList, rr.isDenyList)
		})
	}
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestPropValuePairs_Merging(t *testing.T) {
	t.Run("uses checksums", func(t *testing.T) {
		type testCase struct {
			name string

			bitmaps   []*sroar.Bitmap
			checksums [][]byte
			operator  filters.Operator

			expectedIds         []uint64
			expectedChecksum    []byte
			expectedFirstBitmap bool
		}

		testCases := []testCase{
			{
				name: "AND; merging; new bitmap and calculated checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				checksums: [][]byte{
					{0x01}, {0x02}, {0x03},
				},
				operator: filters.OperatorAnd,

				expectedIds:         []uint64{7, 9},
				expectedChecksum:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xd1, 0xc0, 0xfa},
				expectedFirstBitmap: false,
			},
			{
				name: "OR; merging; new bitmap and calculated checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				checksums: [][]byte{
					{0x01}, {0x02}, {0x03},
				},
				operator: filters.OperatorOr,

				expectedIds:         []uint64{1, 3, 5, 7, 8, 9, 10, 11},
				expectedChecksum:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xd1, 0x50, 0xf3},
				expectedFirstBitmap: false,
			},
			{
				name: "AND; optimization; first bitmap and checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
				},
				checksums: [][]byte{
					{0x01}, {0x01}, {0x01},
				},
				operator: filters.OperatorAnd,

				expectedIds:         []uint64{7, 8, 9, 10, 11},
				expectedChecksum:    []byte{0x01},
				expectedFirstBitmap: true,
			},
			{
				name: "OR; optimization; first bitmap and checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
				},
				checksums: [][]byte{
					{0x01}, {0x01}, {0x01},
				},
				operator: filters.OperatorOr,

				expectedIds:         []uint64{7, 8, 9, 10, 11},
				expectedChecksum:    []byte{0x01},
				expectedFirstBitmap: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pv := &propValuePair{
					operator: tc.operator,
					children: make([]*propValuePair, len(tc.bitmaps)),
				}
				for i := range tc.bitmaps {
					pv.children[i] = &propValuePair{
						operator: filters.OperatorEqual,
						docIDs: docBitmap{
							docIDs:   tc.bitmaps[i],
							checksum: tc.checksums[i],
						},
					}
				}

				dbm, err := pv.mergeDocIDs(true)

				require.Nil(t, err)
				assert.ElementsMatch(t, tc.expectedIds, dbm.IDs())
				assert.Equal(t, tc.expectedChecksum, dbm.checksum)
				assert.Equal(t, tc.expectedFirstBitmap, tc.bitmaps[0] == dbm.docIDs)
				assert.False(t, tc.bitmaps[1] == dbm.docIDs)
				assert.False(t, tc.bitmaps[2] == dbm.docIDs)
			})
		}
	})

	t.Run("does not use checksums", func(t *testing.T) {
		type testCase struct {
			name string

			bitmaps  []*sroar.Bitmap
			operator filters.Operator

			expectedIds []uint64
		}

		testCases := []testCase{
			{
				name: "AND; merging; new bitmap without checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				operator: filters.OperatorAnd,

				expectedIds: []uint64{7, 9},
			},
			{
				name: "OR; merging; new bitmap without checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				operator: filters.OperatorOr,

				expectedIds: []uint64{1, 3, 5, 7, 8, 9, 10, 11},
			},
			{
				name: "AND; no optimization; new bitmap without checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
				},
				operator: filters.OperatorAnd,

				expectedIds: []uint64{7, 8, 9, 10, 11},
			},
			{
				name: "OR; no optimization; new bitmap without checksum",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
				},
				operator: filters.OperatorOr,

				expectedIds: []uint64{7, 8, 9, 10, 11},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pv := &propValuePair{
					operator: tc.operator,
					children: make([]*propValuePair, len(tc.bitmaps)),
				}
				for i := range tc.bitmaps {
					pv.children[i] = &propValuePair{
						operator: filters.OperatorEqual,
						docIDs: docBitmap{
							docIDs: tc.bitmaps[i],
						},
					}
				}

				dbm, err := pv.mergeDocIDs(false)

				require.Nil(t, err)
				assert.ElementsMatch(t, tc.expectedIds, dbm.IDs())
				assert.Nil(t, dbm.checksum)
				assert.False(t, tc.bitmaps[0] == dbm.docIDs)
				assert.False(t, tc.bitmaps[1] == dbm.docIDs)
				assert.False(t, tc.bitmaps[2] == dbm.docIDs)
			})
		}
	})
}

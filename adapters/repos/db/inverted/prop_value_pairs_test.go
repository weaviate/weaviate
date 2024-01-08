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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestPropValuePairs_Merging(t *testing.T) {
	t.Run("always creates new underlying bitmap", func(t *testing.T) {
		type testCase struct {
			name string

			bitmaps  []*sroar.Bitmap
			operator filters.Operator

			expectedIds []uint64
		}

		testCases := []testCase{
			{
				name: "AND; different sets",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				operator: filters.OperatorAnd,

				expectedIds: []uint64{7, 9},
			},
			{
				name: "OR; different sets",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9, 11),
					roaringset.NewBitmap(1, 3, 5, 7, 9),
				},
				operator: filters.OperatorOr,

				expectedIds: []uint64{1, 3, 5, 7, 8, 9, 10, 11},
			},
			{
				name: "AND; same sets",

				bitmaps: []*sroar.Bitmap{
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
					roaringset.NewBitmap(7, 8, 9, 10, 11),
				},
				operator: filters.OperatorAnd,

				expectedIds: []uint64{7, 8, 9, 10, 11},
			},
			{
				name: "OR; same sets",

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

				dbm, err := pv.mergeDocIDs()

				require.Nil(t, err)
				assert.ElementsMatch(t, tc.expectedIds, dbm.IDs())
				assert.False(t, tc.bitmaps[0] == dbm.docIDs)
				assert.False(t, tc.bitmaps[1] == dbm.docIDs)
				assert.False(t, tc.bitmaps[2] == dbm.docIDs)
			})
		}
	})
}

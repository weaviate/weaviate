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

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestPropValuePairs_MergeAnd(t *testing.T) {
	pv := &propValuePair{
		operator: filters.OperatorAnd,
		children: []*propValuePair{
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 7, 8, 9, 10, 11),
					checksum: []byte{0x01},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7, 9, 11),
					checksum: []byte{0x02},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7, 9),
					checksum: []byte{0x03},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7),
					checksum: []byte{0x04},
				},
				operator: filters.OperatorEqual,
			},
		},
	}

	expectedIds := []uint64{7}

	dbm, err := pv.mergeDocIDs()

	require.Nil(t, err)
	assert.ElementsMatch(t, expectedIds, dbm.IDs())
}

func TestPropValuePairs_MergeOr(t *testing.T) {
	pv := &propValuePair{
		operator: filters.OperatorOr,
		children: []*propValuePair{
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 7, 8, 9, 10, 11),
					checksum: []byte{0x01},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7, 9, 11),
					checksum: []byte{0x02},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7, 9),
					checksum: []byte{0x03},
				},
				operator: filters.OperatorEqual,
			},
			{
				docIDsBM: docBitmap{
					docIDs:   createBitmap(t, 1, 3, 5, 7),
					checksum: []byte{0x04},
				},
				operator: filters.OperatorEqual,
			},
		},
	}

	expectedPointers := []uint64{1, 3, 5, 7, 8, 9, 10, 11}

	dbm, err := pv.mergeDocIDs()
	
	require.Nil(t, err)
	assert.ElementsMatch(t, expectedPointers, dbm.IDs())
}

func createBitmap(t *testing.T, ids ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	bm.SetMany(ids)
	return bm
}

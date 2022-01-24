//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeAnd_Old(t *testing.T) {
	list1 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 7},
				{id: 8},
				{id: 9},
				{id: 10},
				{id: 11},
			},
			checksum: []byte{0x01},
		},
		operator: filters.OperatorEqual,
	}

	list2 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
				{id: 9},
				{id: 11},
			},
			checksum: []byte{0x02},
		},
		operator: filters.OperatorEqual,
	}

	list3 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
				{id: 9},
			},
			checksum: []byte{0x03},
		},
		operator: filters.OperatorEqual,
	}

	list4 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
			},
			checksum: []byte{0x04},
		},
		operator: filters.OperatorEqual,
	}

	res, err := mergeAnd([]*propValuePair{&list1, &list2, &list3, &list4}, false)
	require.Nil(t, err)

	expectedPointers := []docPointer{
		{id: 7},
	}

	assert.ElementsMatch(t, expectedPointers, res.docIDs)
}

func TestMergeAnd_Optimized(t *testing.T) {
	list1 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 7},
				{id: 8},
				{id: 9},
				{id: 10},
				{id: 11},
			},
			checksum: []byte{0x01},
		},
		operator: filters.OperatorEqual,
	}

	list2 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
				{id: 9},
				{id: 11},
			},
			checksum: []byte{0x02},
		},
		operator: filters.OperatorEqual,
	}

	list3 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
				{id: 9},
			},
			checksum: []byte{0x03},
		},
		operator: filters.OperatorEqual,
	}

	list4 := propValuePair{
		docIDs: docPointers{
			docIDs: []docPointer{
				{id: 1},
				{id: 3},
				{id: 5},
				{id: 7},
			},
			checksum: []byte{0x04},
		},
		operator: filters.OperatorEqual,
	}

	res, err := mergeAndOptimized([]*propValuePair{&list1, &list2, &list3, &list4}, false)
	require.Nil(t, err)

	expectedPointers := []docPointer{
		{id: 7},
	}

	assert.ElementsMatch(t, expectedPointers, res.docIDs)
}

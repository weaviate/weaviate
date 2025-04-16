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

package roaringsetrange

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
	"golang.org/x/net/context"
)

func TestMemtableReader(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mem := NewMemtable(logger)
	mem.Insert(13, []uint64{113, 213}) // ...1101
	mem.Insert(5, []uint64{15, 25})    // ...0101
	mem.Insert(0, []uint64{10, 20})    // ...0000
	mem.Delete(20, []uint64{120, 220})

	reader := NewMemtableReader(mem)

	type testCase struct {
		value       uint64
		operator    filters.Operator
		expectedAdd []uint64
		expectedDel []uint64
	}

	testCases := []testCase{
		{
			value:       0,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       0,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		{
			value:       5,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{10, 20},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       5,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},

		{
			value:       13,
			operator:    filters.OperatorGreaterThanEqual,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorGreaterThan,
			expectedAdd: []uint64{},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorLessThanEqual,
			expectedAdd: []uint64{10, 20, 15, 25, 113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorLessThan,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorEqual,
			expectedAdd: []uint64{113, 213},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
		{
			value:       13,
			operator:    filters.OperatorNotEqual,
			expectedAdd: []uint64{10, 20, 15, 25},
			expectedDel: []uint64{10, 20, 15, 25, 113, 213, 120, 220},
		},
	}

	for _, tc := range testCases {
		t.Run("read", func(t *testing.T) {
			bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
			assert.NoError(t, err)
			defer release()
			assert.ElementsMatch(t, bm.Additions.ToArray(), tc.expectedAdd)
			assert.ElementsMatch(t, bm.Deletions.ToArray(), tc.expectedDel)
		})
	}
}

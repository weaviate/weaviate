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

package test_utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func AssertPropsMatch(t *testing.T, propsA, propsB []*models.Property) {
	require.Len(t, propsB, len(propsA), "props: different length")

	pMap := map[string]int{}
	for idx, p := range propsA {
		pMap[p.Name] = idx
	}

	for _, pB := range propsB {
		require.Contains(t, pMap, pB.Name)
		pA := propsA[pMap[pB.Name]]

		assert.Equal(t, pA.DataType, pB.DataType)
		assert.Equal(t, pA.IndexFilterable, pB.IndexFilterable)
		assert.Equal(t, pA.IndexSearchable, pB.IndexSearchable)
		assert.Equal(t, pA.IndexRangeFilters, pB.IndexRangeFilters)
		assert.Equal(t, pA.Tokenization, pB.Tokenization)

		if _, isNested := schema.AsNested(pA.DataType); isNested {
			AssertNestedPropsMatch(t, pA.NestedProperties, pB.NestedProperties)
		}
	}
}

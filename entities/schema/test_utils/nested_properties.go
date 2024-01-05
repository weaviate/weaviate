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

func AssertNestedPropsMatch(t *testing.T, nestedPropsA, nestedPropsB []*models.NestedProperty) {
	require.Len(t, nestedPropsB, len(nestedPropsA), "nestedProps: different length")

	npMap := map[string]int{}
	for index, np := range nestedPropsA {
		npMap[np.Name] = index
	}

	for _, npB := range nestedPropsB {
		require.Contains(t, npMap, npB.Name)
		npA := nestedPropsA[npMap[npB.Name]]

		assert.Equal(t, npA.DataType, npB.DataType)
		assert.Equal(t, npA.IndexFilterable, npB.IndexFilterable)
		assert.Equal(t, npA.IndexSearchable, npB.IndexSearchable)
		assert.Equal(t, npA.Tokenization, npB.Tokenization)

		if _, isNested := schema.AsNested(npA.DataType); isNested {
			AssertNestedPropsMatch(t, npA.NestedProperties, npB.NestedProperties)
		}
	}
}

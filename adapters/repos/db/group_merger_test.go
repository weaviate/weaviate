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

package db

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func makeTestGroupedObject(objID string, propVal string, hitDistances []float32) *storobj.Object {
	hits := make([]map[string]interface{}, len(hitDistances))
	for i, d := range hitDistances {
		hits[i] = map[string]interface{}{
			"prop": propVal,
			"_additional": &additional.GroupHitAdditional{
				ID:       strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)),
				Distance: d,
			},
		}
	}
	var minD, maxD float32
	if len(hitDistances) > 0 {
		minD = hitDistances[0]
		maxD = hitDistances[len(hitDistances)-1]
	}
	return storobj.FromObject(&models.Object{
		ID:    strfmt.UUID(objID),
		Class: "TestClass",
		Additional: models.AdditionalProperties{
			"group": &additional.Group{
				GroupedBy: &additional.GroupedBy{
					Value: propVal,
					Path:  []string{"prop"},
				},
				Count:       len(hits),
				Hits:        hits,
				MinDistance: minD,
				MaxDistance: maxD,
			},
		},
	}, []float32{1, 0, 0}, nil, nil)
}

func TestGroupMerger_MinMaxDistance(t *testing.T) {
	t.Run("multi-shard merge correctly assigns MinDistance and MaxDistance", func(t *testing.T) {
		// Shard 1 produced group "cityA" with hits [0.1, 0.5]
		// Shard 2 produced group "cityA" with hits [0.2, 0.9]
		obj1 := makeTestGroupedObject("00000000-0000-0000-0000-000000000001", "cityA", []float32{0.1, 0.5})
		obj2 := makeTestGroupedObject("00000000-0000-0000-0000-000000000002", "cityA", []float32{0.2, 0.9})

		gm := newGroupMerger(
			[]*storobj.Object{obj1, obj2},
			[]float32{0.1, 0.2},
			&searchparams.GroupBy{
				Property:        "prop",
				Groups:          5,
				ObjectsPerGroup: 10,
			},
		)

		mergedObjs, mergedDists, err := gm.Do()
		require.NoError(t, err)
		require.Len(t, mergedObjs, 1)
		require.Len(t, mergedDists, 1)

		group := mergedObjs[0].AdditionalProperties()["group"].(*additional.Group)
		assert.Equal(t, "cityA", group.GroupedBy.Value)
		assert.Equal(t, 4, group.Count)
		assert.Len(t, group.Hits, 4)

		// Hits should be sorted ascending by distance
		expectedDists := []float32{0.1, 0.2, 0.5, 0.9}
		for i, h := range group.Hits {
			actualDist := h["_additional"].(*additional.GroupHitAdditional).Distance
			assert.Equal(t, expectedDists[i], actualDist)
		}

		// MinDistance must be the lowest distance (0.1) and MaxDistance the highest (0.9)
		assert.Equal(t, float32(0.1), group.MinDistance, "MinDistance should be the minimum hit distance")
		assert.Equal(t, float32(0.9), group.MaxDistance, "MaxDistance should be the maximum hit distance")
	})

	t.Run("multi-shard merge with ObjectsPerGroup capping", func(t *testing.T) {
		// Shard 1 has hits [0.1, 0.7]
		// Shard 2 has hits [0.3, 0.8]
		// With ObjectsPerGroup = 2, retained hits are [0.1, 0.3]
		obj1 := makeTestGroupedObject("00000000-0000-0000-0000-000000000001", "group1", []float32{0.1, 0.7})
		obj2 := makeTestGroupedObject("00000000-0000-0000-0000-000000000002", "group1", []float32{0.3, 0.8})

		gm := newGroupMerger(
			[]*storobj.Object{obj1, obj2},
			[]float32{0.1, 0.3},
			&searchparams.GroupBy{
				Property:        "prop",
				Groups:          5,
				ObjectsPerGroup: 2,
			},
		)

		mergedObjs, _, err := gm.Do()
		require.NoError(t, err)
		require.Len(t, mergedObjs, 1)

		group := mergedObjs[0].AdditionalProperties()["group"].(*additional.Group)
		assert.Equal(t, 2, group.Count)
		assert.Len(t, group.Hits, 2)
		assert.Equal(t, float32(0.1), group.MinDistance)
		assert.Equal(t, float32(0.3), group.MaxDistance)
	})

	t.Run("multiple groups ordered by minimum distance", func(t *testing.T) {
		objB := makeTestGroupedObject("00000000-0000-0000-0000-000000000001", "groupB", []float32{0.4, 0.7})
		objA := makeTestGroupedObject("00000000-0000-0000-0000-000000000002", "groupA", []float32{0.05, 0.8})

		gm := newGroupMerger(
			[]*storobj.Object{objB, objA},
			[]float32{0.4, 0.05},
			&searchparams.GroupBy{
				Property:        "prop",
				Groups:          2,
				ObjectsPerGroup: 5,
			},
		)

		mergedObjs, _, err := gm.Do()
		require.NoError(t, err)
		require.Len(t, mergedObjs, 2)

		group0 := mergedObjs[0].AdditionalProperties()["group"].(*additional.Group)
		assert.Equal(t, "groupA", group0.GroupedBy.Value)
		assert.Equal(t, float32(0.05), group0.MinDistance)
		assert.Equal(t, float32(0.8), group0.MaxDistance)

		group1 := mergedObjs[1].AdditionalProperties()["group"].(*additional.Group)
		assert.Equal(t, "groupB", group1.GroupedBy.Value)
		assert.Equal(t, float32(0.4), group1.MinDistance)
		assert.Equal(t, float32(0.7), group1.MaxDistance)
	})
}

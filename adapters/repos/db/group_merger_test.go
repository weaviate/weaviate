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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestGroupMergerMinMaxDistance(t *testing.T) {
	hit := func(id string, dist float32) map[string]interface{} {
		return map[string]interface{}{
			"_additional": &additional.GroupHitAdditional{ID: strfmt.UUID(id), Distance: dist},
		}
	}
	shardObj := func(value string, hits ...map[string]interface{}) *storobj.Object {
		minDist := hits[0]["_additional"].(*additional.GroupHitAdditional).Distance
		maxDist := hits[len(hits)-1]["_additional"].(*additional.GroupHitAdditional).Distance
		return &storobj.Object{Object: models.Object{
			Additional: models.AdditionalProperties{
				"group": &additional.Group{
					GroupedBy:   &additional.GroupedBy{Value: value, Path: []string{"prop"}},
					Count:       len(hits),
					Hits:        hits,
					MinDistance: minDist,
					MaxDistance: maxDist,
				},
			},
		}}
	}

	tests := []struct {
		name            string
		objects         []*storobj.Object
		objectsPerGroup int
		expectedMin     float32
		expectedMax     float32
		expectedCount   int
	}{
		{
			name: "single shard",
			objects: []*storobj.Object{
				shardObj("a", hit("1", 0.1), hit("2", 0.3)),
			},
			objectsPerGroup: 10,
			expectedMin:     0.1,
			expectedMax:     0.3,
			expectedCount:   2,
		},
		{
			name: "multiple shards interleaved",
			objects: []*storobj.Object{
				shardObj("a", hit("1", 0.2), hit("2", 0.5)),
				shardObj("a", hit("3", 0.1), hit("4", 0.4)),
			},
			objectsPerGroup: 10,
			expectedMin:     0.1,
			expectedMax:     0.5,
			expectedCount:   4,
		},
		{
			name: "multiple shards truncated by objectsPerGroup",
			objects: []*storobj.Object{
				shardObj("a", hit("1", 0.2), hit("2", 0.5)),
				shardObj("a", hit("3", 0.1), hit("4", 0.4)),
			},
			objectsPerGroup: 3,
			expectedMin:     0.1,
			expectedMax:     0.4,
			expectedCount:   3,
		},
		{
			name: "single hit",
			objects: []*storobj.Object{
				shardObj("a", hit("1", 0.7)),
			},
			objectsPerGroup: 10,
			expectedMin:     0.7,
			expectedMax:     0.7,
			expectedCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dists := make([]float32, len(tt.objects))
			groupBy := &searchparams.GroupBy{Property: "prop", Groups: 10, ObjectsPerGroup: tt.objectsPerGroup}

			objs, _, err := newGroupMerger(tt.objects, dists, groupBy).Do()
			require.NoError(t, err)
			require.Len(t, objs, 1)

			group := objs[0].AdditionalProperties()["group"].(*additional.Group)
			assert.Equal(t, tt.expectedMin, group.MinDistance)
			assert.Equal(t, tt.expectedMax, group.MaxDistance)
			assert.LessOrEqual(t, group.MinDistance, group.MaxDistance)
			assert.Equal(t, tt.expectedCount, group.Count)
		})
	}
}

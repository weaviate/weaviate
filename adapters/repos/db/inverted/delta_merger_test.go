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
)

func TestDeltaMerger(t *testing.T) {
	dm := NewDeltaMerger()

	t.Run("a simple add and delete with one prop and one doc id", func(t *testing.T) {
		dm.AddAdditions([]Property{{
			Name: "field1", Items: []Countable{
				{Data: []byte("a")},
				{Data: []byte("b")},
			},
			HasFilterableIndex: false,
			HasSearchableIndex: true,
		}}, 0)

		dm.AddDeletions([]Property{{
			Name: "field1", Items: []Countable{
				{Data: []byte("a")},
			},
		}}, 0)

		expected := DeltaMergeResult{
			Additions: []MergeProperty{
				{
					Name:               "field1",
					HasFilterableIndex: false,
					HasSearchableIndex: true,
					MergeItems: []MergeItem{
						{
							Data: []byte("b"),
							DocIDs: []MergeDocIDWithFrequency{
								{
									DocID: 0,
								},
							},
						},
					},
				},
			},
		}

		actual := dm.Merge()
		assert.Equal(t, expected, actual)
	})
}

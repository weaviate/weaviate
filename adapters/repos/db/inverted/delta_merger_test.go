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
		}}, 0)

		dm.AddDeletions([]Property{{
			Name: "field1", Items: []Countable{
				{Data: []byte("a")},
			},
		}}, 0)

		expected := DeltaMergeResult{
			Additions: []MergeProperty{
				{
					Name:         "field1",
					HasFrequency: false,
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

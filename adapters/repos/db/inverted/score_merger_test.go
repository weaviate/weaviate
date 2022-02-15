package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ScoreMerger_OR_List(t *testing.T) {
	t.Run("with three pre-sorted lists", func(t *testing.T) {
		lists := []docPointersWithScore{
			{
				docIDs: []docPointerWithScore{
					{
						id:    17,
						score: 0.9,
					},
					{
						id:    23,
						score: 0.8,
					},
					{
						id:    7,
						score: 0.4,
					},
					{
						id:    18,
						score: 0.2,
					},
				},
			},
			{
				docIDs: []docPointerWithScore{
					{
						id:    6,
						score: 0.4,
					},
					{
						id:    5,
						score: 0.38,
					},
					{
						id:    7,
						score: 0.35,
					},
					{
						id:    23,
						score: 0.3,
					},
					{
						id:    100,
						score: 0.2,
					},
				},
			},
			{
				docIDs: []docPointerWithScore{
					{
						id:    7,
						score: 0.3,
					},
					{
						id:    19,
						score: 0.2,
					},
				},
			},
		}

		actual := newScoreMerger(lists).do()
		expected := []docPointerWithScore{
			{
				id:    7,
				score: 0.3,
			},
			{
				id:    19,
				score: 0.2,
			},
		}

		assert.Equal(t, expected, actual)
	})
}

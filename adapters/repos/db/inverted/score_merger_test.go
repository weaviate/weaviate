package inverted

import (
	"sort"
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

		// make sure all lists are sorted by doc ID as that's a requirement for the
		// merger
		for listid := range lists {
			sort.Slice(lists[listid].docIDs, func(a, b int) bool {
				return lists[listid].docIDs[a].id < lists[listid].docIDs[b].id
			})
		}

		actual := newScoreMerger(lists).do()
		expected := docPointersWithScore{
			count: 8,
			docIDs: []docPointerWithScore{
				{
					id:    5,
					score: 0.38,
				},
				{
					id:    6,
					score: 0.4,
				},
				{
					id:    7,
					score: 1.05,
				},
				{
					id:    17,
					score: 0.9,
				},
				{
					id:    18,
					score: 0.2,
				},
				{
					id:    19,
					score: 0.2,
				},
				{
					id:    23,
					score: 1.1,
				},
				{
					id:    100,
					score: 0.2,
				},
			},
		}

		assert.Equal(t, expected, actual)
	})
}

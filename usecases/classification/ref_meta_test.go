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

package classification

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_RefMeta(t *testing.T) {
	t.Run("without a losing group", func(t *testing.T) {
		source := NeighborRef{
			WinningCount: 3,
			OverallCount: 3,
			LosingCount:  0,
			Distances: NeighborRefDistances{
				ClosestWinningDistance: 0.1,
				ClosestOverallDistance: 0.1,
				MeanWinningDistance:    0.2,
			},
		}

		expected := &models.ReferenceMetaClassification{
			ClosestWinningDistance: 0.1,
			ClosestOverallDistance: 0.1,
			MeanWinningDistance:    0.2,
			WinningDistance:        0.2, // deprecated, must be removed in 0.23.0
			OverallCount:           3,
			WinningCount:           3,
			LosingCount:            0,
		}

		actual := source.Meta()
		assert.InDelta(t, expected.ClosestWinningDistance, actual.ClosestWinningDistance, 0.001)
		assert.InDelta(t, expected.ClosestOverallDistance, actual.ClosestOverallDistance, 0.001)
		assert.InDelta(t, expected.MeanWinningDistance, actual.MeanWinningDistance, 0.001)
		assert.InDelta(t, expected.WinningDistance, actual.WinningDistance, 0.001)
		assert.Equal(t, expected.OverallCount, actual.OverallCount)
		assert.Equal(t, expected.WinningCount, actual.WinningCount)
		assert.Equal(t, expected.LosingCount, actual.LosingCount)
	})

	t.Run("with a losing group", func(t *testing.T) {
		source := NeighborRef{
			WinningCount: 3,
			OverallCount: 5,
			LosingCount:  2,
			Distances: NeighborRefDistances{
				ClosestWinningDistance: 0.1,
				ClosestOverallDistance: 0.1,
				MeanWinningDistance:    0.2,
				ClosestLosingDistance:  ptFloat32(0.15),
				MeanLosingDistance:     ptFloat32(0.25),
			},
		}

		expected := &models.ReferenceMetaClassification{
			ClosestOverallDistance: 0.1,
			ClosestWinningDistance: 0.1,
			MeanWinningDistance:    0.2,
			WinningDistance:        0.2, // deprecated, must be removed in 0.23.0
			ClosestLosingDistance:  ptFloat64(0.15),
			MeanLosingDistance:     ptFloat64(0.25),
			LosingDistance:         ptFloat64(0.25), // deprecated, must be removed in 0.23.0
			OverallCount:           5,
			WinningCount:           3,
			LosingCount:            2,
		}

		actual := source.Meta()
		assert.InDelta(t, expected.ClosestOverallDistance, actual.ClosestOverallDistance, 0.001)
		assert.InDelta(t, expected.ClosestWinningDistance, actual.ClosestWinningDistance, 0.001)
		assert.InDelta(t, expected.MeanWinningDistance, actual.MeanWinningDistance, 0.001)
		assert.InDelta(t, expected.WinningDistance, actual.WinningDistance, 0.001)
		assert.InDelta(t, *expected.ClosestLosingDistance, *actual.ClosestLosingDistance, 0.001)
		assert.InDelta(t, *expected.MeanLosingDistance, *actual.MeanLosingDistance, 0.001)
		assert.InDelta(t, *expected.LosingDistance, *actual.LosingDistance, 0.001)
		assert.Equal(t, expected.OverallCount, actual.OverallCount)
		assert.Equal(t, expected.OverallCount, actual.OverallCount)
		assert.Equal(t, expected.WinningCount, actual.WinningCount)
		assert.Equal(t, expected.LosingCount, actual.LosingCount)
	})
}

func ptFloat32(in float32) *float32 {
	return &in
}

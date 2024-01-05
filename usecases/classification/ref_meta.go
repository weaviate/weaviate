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

import "github.com/weaviate/weaviate/entities/models"

// NeighborRefDistances include various distances about the winning and losing
// groups (knn)
type NeighborRefDistances struct {
	ClosestOverallDistance float32

	// Winning
	ClosestWinningDistance float32
	MeanWinningDistance    float32

	// Losing (optional)
	MeanLosingDistance    *float32
	ClosestLosingDistance *float32
}

func (r NeighborRef) Meta() *models.ReferenceMetaClassification {
	out := &models.ReferenceMetaClassification{
		OverallCount:           int64(r.OverallCount),
		WinningCount:           int64(r.WinningCount),
		LosingCount:            int64(r.LosingCount),
		ClosestOverallDistance: float64(r.Distances.ClosestOverallDistance),
		WinningDistance:        float64(r.Distances.MeanWinningDistance), // deprecated, remove in 0.23.0
		MeanWinningDistance:    float64(r.Distances.MeanWinningDistance),
		ClosestWinningDistance: float64(r.Distances.ClosestWinningDistance),
	}

	if r.Distances.MeanLosingDistance != nil {
		out.MeanLosingDistance = ptFloat64(float64(*r.Distances.MeanLosingDistance))
		out.LosingDistance = ptFloat64(float64(*r.Distances.MeanLosingDistance)) // deprecated
	}

	if r.Distances.ClosestLosingDistance != nil {
		out.ClosestLosingDistance = ptFloat64(float64(*r.Distances.ClosestLosingDistance))
	}

	return out
}

func ptFloat64(in float64) *float64 {
	return &in
}

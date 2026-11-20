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

package traverser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

const (
	boostBerlinLat, boostBerlinLon = float32(52.52), float32(13.405)
)

func geoDecay(scale, offset float64, curve filters.DecayCurveType, decayValue float32) *filters.Decay {
	return &filters.Decay{
		Path:          &filters.Path{Property: "location"},
		IsGeo:         true,
		OriginGeo:     filters.GeoOrigin{Latitude: boostBerlinLat, Longitude: boostBerlinLon},
		ScaleNumeric:  scale,
		OffsetNumeric: offset,
		Curve:         curve,
		DecayValue:    decayValue,
	}
}

func coords(lat, lon float32) *models.GeoCoordinates {
	return &models.GeoCoordinates{Latitude: &lat, Longitude: &lon}
}

func TestComputeDecayForResult_Geo(t *testing.T) {
	// Potsdam, about 26 km from the origin
	potsdamLat, potsdamLon := float32(52.4), float32(13.066)
	potsdam := float64(geopoints.Distance(boostBerlinLat, boostBerlinLon, potsdamLat, potsdamLon))
	require.InDelta(t, 26_500, potsdam, 1_000)

	tests := []struct {
		name  string
		decay *filters.Decay
		value any
		want  float64
	}{
		{name: "at the origin", decay: geoDecay(10_000, 0, filters.DecayCurveExp, 0.5), value: coords(boostBerlinLat, boostBerlinLon), want: 1},
		{name: "at the scale distance scores the decay value", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: coords(potsdamLat, potsdamLon), want: 0.5},
		{name: "at twice the scale, exponential", decay: geoDecay(potsdam/2, 0, filters.DecayCurveExp, 0.5), value: coords(potsdamLat, potsdamLon), want: 0.25},
		{name: "gauss at the scale distance", decay: geoDecay(potsdam, 0, filters.DecayCurveGauss, 0.3), value: coords(potsdamLat, potsdamLon), want: 0.3},
		{name: "linear reaches zero", decay: geoDecay(potsdam/4, 0, filters.DecayCurveLinear, 0.5), value: coords(potsdamLat, potsdamLon), want: 0},
		{name: "within the offset", decay: geoDecay(1_000, potsdam+1, filters.DecayCurveExp, 0.5), value: coords(potsdamLat, potsdamLon), want: 1},
		{name: "the offset is subtracted", decay: geoDecay(potsdam/2, potsdam/2, filters.DecayCurveExp, 0.5), value: coords(potsdamLat, potsdamLon), want: 0.5},
		{name: "default curve and decay value", decay: geoDecay(potsdam, 0, "", 0), value: coords(potsdamLat, potsdamLon), want: 0.5},
		{name: "the other side of the globe", decay: geoDecay(1_000_000, 0, filters.DecayCurveLinear, 0.5), value: coords(-boostBerlinLat, boostBerlinLon-180), want: 0},
		{
			name: "coordinates as a map", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5),
			value: map[string]any{"latitude": float64(potsdamLat), "longitude": float64(potsdamLon)}, want: 0.5,
		},
		{name: "coordinates by value", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: *coords(potsdamLat, potsdamLon), want: 0.5},
		{name: "a nil pointer scores nothing", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: (*models.GeoCoordinates)(nil), want: 0},
		{name: "a missing latitude scores nothing", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: &models.GeoCoordinates{Longitude: &potsdamLon}, want: 0},
		{name: "a map without a longitude scores nothing", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: map[string]any{"latitude": 52.4}, want: 0},
		{name: "a number scores nothing", decay: geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5), value: float64(52.4), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := parseDecayParams(tt.decay, time.Now())
			require.True(t, parsed.valid)
			score := computeDecayForResult(tt.decay, parsed, map[string]any{"location": tt.value})
			assert.InDelta(t, tt.want, float64(score), 0.001)
		})
	}

	t.Run("a result without the property scores nothing", func(t *testing.T) {
		decay := geoDecay(potsdam, 0, filters.DecayCurveExp, 0.5)
		score := computeDecayForResult(decay, parseDecayParams(decay, time.Now()), map[string]any{"name": "nowhere"})
		assert.Zero(t, score)
	})
}

func TestApplyBoostScoring_GeoDecayPromotesNearbyResults(t *testing.T) {
	results := func() []search.Result {
		return []search.Result{
			makeResult("sydney", 0.9, map[string]any{"location": coords(-33.87, 151.21)}),
			makeResult("nowhere", 0.8, map[string]any{"name": "no location"}),
			makeResult("munich", 0.7, map[string]any{"location": coords(48.137, 11.575)}),
			makeResult("potsdam", 0.6, map[string]any{"location": coords(52.4, 13.066)}),
			makeResult("berlin", 0.5, map[string]any{"location": coords(boostBerlinLat, boostBerlinLon)}),
		}
	}
	ids := func(res []search.Result) []string {
		out := make([]string, len(res))
		for i := range res {
			out[i] = string(res[i].ID)
		}
		return out
	}
	boost := func(weight, condWeight float32) *filters.Boost {
		return &filters.Boost{
			Weight:     weight,
			Conditions: []filters.BoostCondition{{Decay: geoDecay(100_000, 0, filters.DecayCurveExp, 0.5), Weight: condWeight}},
		}
	}

	t.Run("proximity alone orders by distance, no location last", func(t *testing.T) {
		got := applyBoostScoring(results(), withOriginalLimit(boost(1, 1), 5))
		// sydney's decay is far below any float32 above zero, a tie with the result
		// without a location that the id breaks
		assert.Equal(t, []string{"berlin", "potsdam", "munich"}, ids(got)[:3])
		assert.ElementsMatch(t, []string{"sydney", "nowhere"}, ids(got)[3:])
	})

	t.Run("blended with the primary score", func(t *testing.T) {
		got := applyBoostScoring(results(), withOriginalLimit(boost(0.7, 1), 5))
		// 0.3*primary + 0.7*proximity, both in [0, 1]:
		// berlin 0+0.7, potsdam 0.075+0.58, sydney 0.3+0, nowhere 0.225+0, munich 0.15+0.02
		assert.Equal(t, []string{"berlin", "potsdam", "sydney", "nowhere", "munich"}, ids(got))
	})

	t.Run("a negative weight demotes nearby results", func(t *testing.T) {
		got := applyBoostScoring(results(), withOriginalLimit(boost(1, -1), 5))
		order := ids(got)
		assert.Equal(t, "berlin", order[4])
		assert.Equal(t, "potsdam", order[3])
	})

	t.Run("the limit applies after the reorder", func(t *testing.T) {
		got := applyBoostScoring(results(), withOriginalLimit(boost(1, 1), 2))
		assert.Equal(t, []string{"berlin", "potsdam"}, ids(got))
	})
}

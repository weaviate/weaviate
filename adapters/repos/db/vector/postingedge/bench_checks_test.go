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

//go:build postingedgebench

package postingedge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMedian(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{name: "one value", values: []float64{7}, want: 7},
		{name: "odd count, unsorted", values: []float64{9, 1, 5}, want: 5},
		{name: "even count is the mean of the two in the middle", values: []float64{10, 1, 3, 4}, want: 3.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := append([]float64(nil), tt.values...)
			require.InDelta(t, tt.want, median(values), 1e-12)
			require.Equal(t, tt.values, values, "the values are left in their order")
		})
	}
}

func TestOutworked(t *testing.T) {
	timed := func(threads, rescore, ef int, margin, qps float64) benchPoint {
		return benchPoint{Threads: threads, RescoreLimit: rescore, LoadBatch: DefaultLoadBatch, SourceMargin: margin, EF: ef, QPS: qps}
	}
	otherBatch := timed(8, 800, 640, 0.08, 900)
	otherBatch.LoadBatch = 1
	untimed := timed(8, 800, 640, 0.08, 900)
	untimed.Untimed = true

	// each wanted pair is the QPS of the slow point and of the busier one
	tests := []struct {
		name   string
		points []benchPoint
		want   [][2]float64
	}{
		{
			name:   "more rescoring gives less QPS",
			points: []benchPoint{timed(8, 400, 640, 0.08, 990), timed(8, 800, 640, 0.08, 914)},
		},
		{
			name:   "slower than the point that rescores more",
			points: []benchPoint{timed(8, 400, 640, 0.08, 894), timed(8, 800, 640, 0.08, 914)},
			want:   [][2]float64{{894, 914}},
		},
		{
			name:   "behind by less than the gap",
			points: []benchPoint{timed(8, 400, 640, 0.08, 900), timed(8, 800, 640, 0.08, 914)},
		},
		{
			name:   "set against the fastest of the busier points",
			points: []benchPoint{timed(1, 200, 80, 0.06, 600), timed(1, 400, 80, 0.06, 650), timed(1, 800, 80, 0.06, 700)},
			want:   [][2]float64{{600, 700}, {650, 700}},
		},
		{
			name: "another ef, margin, thread count or load batch is another search",
			points: []benchPoint{
				timed(8, 400, 640, 0.08, 800),
				timed(8, 800, 1024, 0.08, 900), timed(8, 800, 640, 0.06, 900), timed(1, 800, 640, 0.08, 900), otherBatch,
			},
		},
		{
			name:   "an untimed point has no QPS to compare",
			points: []benchPoint{timed(8, 400, 640, 0.08, 800), untimed},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got [][2]float64
			for _, o := range outworked(tt.points, 0.02) {
				require.Greater(t, o.busier.RescoreLimit, o.slow.RescoreLimit)
				got = append(got, [2]float64{o.slow.QPS, o.busier.QPS})
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRepeatedPoints(t *testing.T) {
	efCurve := func(qpsByEF ...float64) []benchPoint {
		var points []benchPoint
		for i := 0; i < len(qpsByEF); i += 2 {
			points = append(points, benchPoint{EF: int(qpsByEF[i]), QPS: qpsByEF[i+1]})
		}
		return points
	}

	tests := []struct {
		name         string
		curve, other []benchPoint
		wantEFs      []int
		wantChanges  []float64
	}{
		{
			name:        "the same efs in both runs",
			curve:       efCurve(10, 1100, 20, 900),
			other:       efCurve(10, 1000, 20, 1000),
			wantEFs:     []int{10, 20},
			wantChanges: []float64{0.10, -0.10},
		},
		{
			name:        "an ef only one of the runs has is left out",
			curve:       efCurve(10, 1000, 20, 900, 2048, 50),
			other:       efCurve(20, 1000, 40, 700),
			wantEFs:     []int{20},
			wantChanges: []float64{-0.10},
		},
		{
			name:  "no other run of the curve",
			curve: efCurve(10, 1000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs := repeatedPoints(tt.curve, tt.other)
			require.Len(t, pairs, len(tt.wantEFs))
			for i, pair := range pairs {
				require.Equal(t, tt.wantEFs[i], pair.point.EF)
				require.Equal(t, tt.wantEFs[i], pair.other.EF)
				require.InDelta(t, tt.wantChanges[i], pair.change(), 1e-9)
			}
		})
	}
}

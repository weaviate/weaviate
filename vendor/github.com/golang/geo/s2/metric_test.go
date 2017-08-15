/*
Copyright 2015 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s2

import (
	"math"
	"testing"
)

func TestMetric(t *testing.T) {
	if got := MinWidthMetric.MaxLevel(0.001256); got != 9 {
		t.Errorf("MinWidthMetric.MaxLevel(0.001256) = %d, want 9", got)
	}

	// Check that the maximum aspect ratio of an individual cell is consistent
	// with the global minimums and maximums.
	if MaxEdgeAspect < 1 {
		t.Errorf("MaxEdgeAspect = %v, want >= 1", MaxEdgeAspect)
	}
	if got := MaxEdgeMetric.Deriv / MinEdgeMetric.Deriv; MaxEdgeAspect > got {
		t.Errorf("Edge Aspect: %v/%v = %v, want <= %v", MaxEdgeMetric.Deriv, MinEdgeMetric.Deriv, got, MaxDiagAspect)
	}
	if MaxDiagAspect < 1 {
		t.Errorf("MaxDiagAspect = %v, want >= 1", MaxDiagAspect)
	}
	if got := MaxDiagMetric.Deriv / MinDiagMetric.Deriv; MaxDiagAspect > got {
		t.Errorf("Diag Aspect: %v/%v = %v, want <= %v", MaxDiagMetric.Deriv, MinDiagMetric.Deriv, got, MaxDiagAspect)
	}

	// Check that area is consistent with edge and width.
	if got := MinWidthMetric.Deriv*MinEdgeMetric.Deriv - 1e-15; MinAreaMetric.Deriv < got {
		t.Errorf("Min Area: %v*%v = %v, want >= %v", MinWidthMetric.Deriv, MinEdgeMetric.Deriv, got, MinAreaMetric.Deriv)
	}
	if got := MaxWidthMetric.Deriv*MaxEdgeMetric.Deriv + 1e-15; MaxAreaMetric.Deriv > got {
		t.Errorf("Max Area: %v*%v = %v, want <= %v", MaxWidthMetric.Deriv, MaxEdgeMetric.Deriv, got, MaxAreaMetric.Deriv)
	}

	for level := -2; level <= maxLevel+3; level++ {
		width := MinWidthMetric.Deriv * math.Pow(2, float64(-level))
		if level >= maxLevel+3 {
			width = 0
		}

		// Check boundary cases (exactly equal to a threshold value).
		expected := int(math.Max(0, math.Min(maxLevel, float64(level))))

		if MinWidthMetric.MinLevel(width) != expected {
			t.Errorf("MinWidthMetric.MinLevel(%v) = %v, want %v", width, MinWidthMetric.MinLevel(width), expected)
		}
		if MinWidthMetric.MaxLevel(width) != expected {
			t.Errorf("MinWidthMetric.MaxLevel(%v) = %v, want %v", width, MinWidthMetric.MaxLevel(width), expected)
		}
		if MinWidthMetric.ClosestLevel(width) != expected {
			t.Errorf("MinWidthMetric.ClosestLevel(%v) = %v, want %v", width, MinWidthMetric.ClosestLevel(width), expected)
		}

		// Also check non-boundary cases.
		if got := MinWidthMetric.MinLevel(1.2 * width); got != expected {
			t.Errorf("non-boundary MinWidthMetric.MinLevel(%v) = %v, want %v", 1.2*width, got, expected)
		}
		if got := MinWidthMetric.MaxLevel(0.8 * width); got != expected {
			t.Errorf("non-boundary MinWidthMetric.MaxLevel(%v) = %v, want %v", 0.8*width, got, expected)
		}
		if got := MinWidthMetric.ClosestLevel(1.2 * width); got != expected {
			t.Errorf("non-boundary larger MinWidthMetric.ClosestLevel(%v) = %v, want %v", 1.2*width, got, expected)
		}
		if got := MinWidthMetric.ClosestLevel(0.8 * width); got != expected {
			t.Errorf("non-boundary smaller MinWidthMetric.ClosestLevel(%v) = %v, want %v", 0.8*width, got, expected)
		}
	}
}

func TestMetricSizeRelations(t *testing.T) {
	// check that min <= avg <= max for each metric.
	tests := []struct {
		min Metric
		avg Metric
		max Metric
	}{
		{MinAngleSpanMetric, AvgAngleSpanMetric, MaxAngleSpanMetric},
		{MinWidthMetric, AvgWidthMetric, MaxWidthMetric},
		{MinEdgeMetric, AvgEdgeMetric, MaxEdgeMetric},
		{MinDiagMetric, AvgDiagMetric, MaxDiagMetric},
		{MinAreaMetric, AvgAreaMetric, MaxAreaMetric},
	}

	for _, test := range tests {
		if test.min.Deriv > test.avg.Deriv {
			t.Errorf("Min %v > Avg %v", test.min.Deriv, test.avg.Deriv)
		}
		if test.avg.Deriv > test.max.Deriv {
			t.Errorf("Avg %v > Max %v", test.avg.Deriv, test.max.Deriv)
		}
	}
}

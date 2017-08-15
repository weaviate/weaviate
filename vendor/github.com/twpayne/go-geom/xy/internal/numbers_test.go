package internal_test

import (
	"math"
	"testing"

	"github.com/twpayne/go-geom/xy/internal"
)

func TestIsSameSignAndNonZero(t *testing.T) {
	for i, tc := range []struct {
		i, j     float64
		expected bool
	}{
		{
			i: 0, j: 0,
			expected: false,
		},
		{
			i: 0, j: 1,
			expected: false,
		},
		{
			i: 1, j: 1,
			expected: true,
		},
		{
			i: math.Inf(1), j: 1,
			expected: true,
		},
		{
			i: math.Inf(1), j: math.Inf(1),
			expected: true,
		},
		{
			i: math.Inf(-1), j: math.Inf(1),
			expected: false,
		},
		{
			i: math.Inf(1), j: -1,
			expected: false,
		},
		{
			i: 1, j: -1,
			expected: false,
		},
		{
			i: -1, j: -1,
			expected: true,
		},
		{
			i: math.Inf(-1), j: math.Inf(-1),
			expected: true,
		},
	} {
		actual := internal.IsSameSignAndNonZero(tc.i, tc.j)

		if actual != tc.expected {
			t.Errorf("Test %d failed.", i+1)
		}

	}
}

func TestMin(t *testing.T) {
	for i, tc := range []struct {
		v1, v2, v3, v4 float64
		expected       float64
	}{
		{
			v1: 0, v2: 0, v3: 0, v4: 0,
			expected: 0,
		},
		{
			v1: -1, v2: 0, v3: 0, v4: 0,
			expected: -1,
		},
		{
			v1: -1, v2: 2, v3: 3, v4: math.Inf(-1),
			expected: math.Inf(-1),
		},
	} {
		actual := internal.Min(tc.v1, tc.v2, tc.v3, tc.v4)

		if actual != tc.expected {
			t.Errorf("Test %d failed.", i+1)
		}

	}
}

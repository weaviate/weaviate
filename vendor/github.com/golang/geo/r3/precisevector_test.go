/*
Copyright 2016 Google Inc. All rights reserved.

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

package r3

import (
	"math/big"
	"testing"
)

// preciseEq compares two big.Floats and checks if the are the same.
func preciseEq(a, b *big.Float) bool {
	return a.SetPrec(prec).Cmp(b.SetPrec(prec)) == 0
}

func TestPreciseRoundtrip(t *testing.T) {
	tests := []struct {
		v Vector
	}{
		{Vector{0, 0, 0}},
		{Vector{1, 2, 3}},
		{Vector{3, -4, 12}},
		{Vector{1, 1e-16, 1e-32}},
	}

	for _, test := range tests {
		if got, want := PreciseVectorFromVector(test.v).Vector(), test.v.Normalize(); !got.ApproxEqual(want) {
			t.Errorf("PreciseVectorFromVector(%v).Vector() = %v, want %v", test.v, got, want)
		}
	}
}

func TestPreciseIsUnit(t *testing.T) {
	const epsilon = 1e-14
	tests := []struct {
		v    PreciseVector
		want bool
	}{
		{
			v:    NewPreciseVector(0, 0, 0),
			want: false,
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			want: true,
		},
		{
			v:    NewPreciseVector(0, 1, 0),
			want: true,
		},
		{
			v:    NewPreciseVector(0, 0, 1),
			want: true,
		},
		{
			v:    NewPreciseVector(1+2*epsilon, 0, 0),
			want: false,
		},
		{
			v:    NewPreciseVector(0*(1+epsilon), 0, 0),
			want: false,
		},
		{
			v:    NewPreciseVector(1, 1, 1),
			want: false,
		},
	}

	for _, test := range tests {
		if got := test.v.IsUnit(); got != test.want {
			t.Errorf("%v.IsUnit() = %v, want %v", test.v, got, test.want)
		}
	}
}

func TestPreciseNorm2(t *testing.T) {
	tests := []struct {
		v    PreciseVector
		want *big.Float
	}{
		{
			v:    NewPreciseVector(0, 0, 0),
			want: precise0,
		},
		{
			v:    NewPreciseVector(0, 1, 0),
			want: precise1,
		},
		{
			v:    NewPreciseVector(1, 1, 1),
			want: precStr("3"),
		},
		{
			v:    NewPreciseVector(1, 2, 3),
			want: precStr("14"),
		},
		{
			v:    NewPreciseVector(3, -4, 12),
			want: precStr("169"),
		},
	}

	for _, test := range tests {
		if got := test.v.Norm2(); !preciseEq(got, test.want) {
			t.Errorf("%v.Norm2() = %v, want %v", test.v, test.v.Norm2(), test.want)
		}
	}
}

func TestPreciseAdd(t *testing.T) {
	tests := []struct {
		v1, v2, want PreciseVector
	}{
		{
			v1:   NewPreciseVector(0, 0, 0),
			v2:   NewPreciseVector(0, 0, 0),
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(0, 0, 0),
			want: NewPreciseVector(1, 0, 0),
		},
		{
			v1:   NewPreciseVector(1, 2, 3),
			v2:   NewPreciseVector(4, 5, 7),
			want: NewPreciseVector(5, 7, 10),
		},
		{
			v1:   NewPreciseVector(1, -3, 5),
			v2:   NewPreciseVector(1, -6, -6),
			want: NewPreciseVector(2, -9, -1),
		},
	}

	for _, test := range tests {
		if got := test.v1.Add(test.v2); !got.Equals(test.want) {
			t.Errorf("%v + %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestPreciseSub(t *testing.T) {
	tests := []struct {
		v1, v2, want PreciseVector
	}{
		{
			v1:   NewPreciseVector(0, 0, 0),
			v2:   NewPreciseVector(0, 0, 0),
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(0, 0, 0),
			want: NewPreciseVector(1, 0, 0),
		},
		{
			v1:   NewPreciseVector(1, 2, 3),
			v2:   NewPreciseVector(4, 5, 7),
			want: NewPreciseVector(-3, -3, -4),
		},
		{
			v1:   NewPreciseVector(1, -3, 5),
			v2:   NewPreciseVector(1, -6, -6),
			want: NewPreciseVector(0, 3, 11),
		},
	}

	for _, test := range tests {
		if got := test.v1.Sub(test.v2); !got.Equals(test.want) {
			t.Errorf("%v - %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestPreciseMul(t *testing.T) {
	tests := []struct {
		v    PreciseVector
		f    *big.Float
		want PreciseVector
	}{
		{
			v:    NewPreciseVector(0, 0, 0),
			f:    precFloat(3),
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    precFloat(1),
			want: NewPreciseVector(1, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    precFloat(0),
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    precFloat(3),
			want: NewPreciseVector(3, 0, 0),
		},
		{
			v:    NewPreciseVector(1, -3, 5),
			f:    precFloat(-1),
			want: NewPreciseVector(-1, 3, -5),
		},
		{
			v:    NewPreciseVector(1, -3, 5),
			f:    precFloat(2),
			want: NewPreciseVector(2, -6, 10),
		},
	}

	for _, test := range tests {
		if got := test.v.Mul(test.f); !got.Equals(test.want) {
			t.Errorf("%v.Mul(%v) = %v, want %v", test.v, test.f, got, test.want)
		}
	}
}

func TestPreciseMulByFloat64(t *testing.T) {
	tests := []struct {
		v    PreciseVector
		f    float64
		want PreciseVector
	}{
		{
			v:    NewPreciseVector(0, 0, 0),
			f:    3,
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    1,
			want: NewPreciseVector(1, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    0,
			want: NewPreciseVector(0, 0, 0),
		},
		{
			v:    NewPreciseVector(1, 0, 0),
			f:    3,
			want: NewPreciseVector(3, 0, 0),
		},
		{
			v:    NewPreciseVector(1, -3, 5),
			f:    -1,
			want: NewPreciseVector(-1, 3, -5),
		},
		{
			v:    NewPreciseVector(1, -3, 5),
			f:    2,
			want: NewPreciseVector(2, -6, 10),
		},
	}

	for _, test := range tests {
		if got := test.v.MulByFloat64(test.f); !got.Equals(test.want) {
			t.Errorf("%v.MulByFloat64(%v) = %v, want %v", test.v, test.f, got, test.want)
		}
	}
}

func TestPreciseDot(t *testing.T) {
	tests := []struct {
		v1, v2 PreciseVector
		want   *big.Float
	}{
		{
			// Dot with self should be 1.
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(1, 0, 0),
			want: precise1,
		},
		{
			// Dot with self should be 1.
			v1:   NewPreciseVector(0, 1, 0),
			v2:   NewPreciseVector(0, 1, 0),
			want: precise1,
		},
		{
			// Dot with self should be 1.
			v1:   NewPreciseVector(0, 0, 1),
			v2:   NewPreciseVector(0, 0, 1),
			want: precise1,
		},
		{
			// Perpendicular should be 0.
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(0, 1, 0),
			want: precise0,
		},
		{
			// Perpendicular should be 0.
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(0, 1, 1),
			want: precise0,
		},
		{
			v1:   NewPreciseVector(1, 1, 1),
			v2:   NewPreciseVector(-1, -1, -1),
			want: precStr("-3"),
		},
	}

	for _, test := range tests {
		if got := test.v1.Dot(test.v2); !preciseEq(got, test.want) {
			t.Errorf("%v · %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
		if got := test.v2.Dot(test.v1); !preciseEq(got, test.want) {
			t.Errorf("%v · %v = %v, want %v", test.v2, test.v1, got, test.want)
		}
	}
}

func TestPreciseCross(t *testing.T) {
	tests := []struct {
		v1, v2, want PreciseVector
	}{
		{
			// Cross with self should be 0.
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(1, 0, 0),
			want: NewPreciseVector(0, 0, 0),
		},
		{
			// Cross with perpendicular should give the remaining axis.
			v1:   NewPreciseVector(1, 0, 0),
			v2:   NewPreciseVector(0, 1, 0),
			want: NewPreciseVector(0, 0, 1),
		},
		{
			// Cross with perpendicular should give the remaining axis.
			v1:   NewPreciseVector(0, 1, 0),
			v2:   NewPreciseVector(0, 0, 1),
			want: NewPreciseVector(1, 0, 0),
		},
		{
			// Cross with perpendicular should give the remaining axis.
			v1:   NewPreciseVector(0, 0, 1),
			v2:   NewPreciseVector(1, 0, 0),
			want: NewPreciseVector(0, 1, 0),
		},
		{
			v1:   NewPreciseVector(0, 1, 0),
			v2:   NewPreciseVector(1, 0, 0),
			want: NewPreciseVector(0, 0, -1),
		},
		{
			v1:   NewPreciseVector(1, 2, 3),
			v2:   NewPreciseVector(-4, 5, -6),
			want: NewPreciseVector(-27, -6, 13),
		},
	}

	for _, test := range tests {
		if got := test.v1.Cross(test.v2); !got.Equals(test.want) {
			t.Errorf("%v ⨯ %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestPreciseIdentities(t *testing.T) {
	tests := []struct {
		v1, v2 PreciseVector
	}{
		{
			v1: NewPreciseVector(0, 0, 0),
			v2: NewPreciseVector(0, 0, 0),
		},
		{
			v1: NewPreciseVector(0, 0, 0),
			v2: NewPreciseVector(0, 1, 2),
		},
		{
			v1: NewPreciseVector(1, 0, 0),
			v2: NewPreciseVector(0, 1, 0),
		},
		{
			v1: NewPreciseVector(1, 0, 0),
			v2: NewPreciseVector(0, 1, 1),
		},
		{
			v1: NewPreciseVector(1, 1, 1),
			v2: NewPreciseVector(-1, -1, -1),
		},
		{
			v1: NewPreciseVector(1, 2, 2),
			v2: NewPreciseVector(-0.3, 0.4, -1.2),
		},
	}

	for _, test := range tests {
		c1 := test.v1.Cross(test.v2)
		c2 := test.v2.Cross(test.v1)
		d1 := test.v1.Dot(test.v2)
		d2 := test.v2.Dot(test.v1)

		// Dot commutes
		if !preciseEq(d1, d2) {
			t.Errorf("%v = %v · %v != %v · %v = %v", d1, test.v1, test.v2, test.v2, test.v1, d2)
		}
		// Cross anti-commutes
		if !c1.Equals(c2.MulByFloat64(-1.0)) {
			t.Errorf("%v = %v ⨯ %v != -(%v ⨯ %v) = -%v", c1, test.v1, test.v2, test.v2, test.v1, c2)
		}
		// Cross is orthogonal to original vectors
		if got := test.v1.Dot(c1); !preciseEq(got, precise0) {
			t.Errorf("%v · (%v ⨯ %v) = %v, want %v", test.v1, test.v1, test.v2, got, precise0)
		}
		if got := test.v2.Dot(c1); !preciseEq(got, precise0) {
			t.Errorf("%v · (%v ⨯ %v) = %v, want %v", test.v2, test.v1, test.v2, got, precise0)
		}
	}
}

func TestPreciseLargestSmallestComponents(t *testing.T) {
	tests := []struct {
		v                 PreciseVector
		largest, smallest Axis
	}{
		{
			v:        NewPreciseVector(0, 0, 0),
			largest:  ZAxis,
			smallest: ZAxis,
		},
		{
			v:        NewPreciseVector(1, 0, 0),
			largest:  XAxis,
			smallest: ZAxis,
		},
		{
			v:        NewPreciseVector(1, -1, 0),
			largest:  YAxis,
			smallest: ZAxis,
		},
		{
			v:        NewPreciseVector(-1, -1.1, -1.1),
			largest:  ZAxis,
			smallest: XAxis,
		},
		{
			v:        NewPreciseVector(0.5, -0.4, -0.5),
			largest:  ZAxis,
			smallest: YAxis,
		},
		{
			v:        NewPreciseVector(1e-15, 1e-14, 1e-13),
			largest:  ZAxis,
			smallest: XAxis,
		},
	}

	for _, test := range tests {
		if got := test.v.LargestComponent(); got != test.largest {
			t.Errorf("%v.LargestComponent() = %v, want %v", test.v, got, test.largest)
		}
		if got := test.v.SmallestComponent(); got != test.smallest {
			t.Errorf("%v.SmallestComponent() = %v, want %v", test.v, got, test.smallest)
		}
	}
}

/*
Copyright 2014 Google Inc. All rights reserved.

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
	"math"
	"testing"
)

func float64Eq(x, y float64) bool { return math.Abs(x-y) < 1e-14 }

func TestVectorNorm(t *testing.T) {
	tests := []struct {
		v    Vector
		want float64
	}{
		{Vector{0, 0, 0}, 0},
		{Vector{0, 1, 0}, 1},
		{Vector{3, -4, 12}, 13},
		{Vector{1, 1e-16, 1e-32}, 1},
	}
	for _, test := range tests {
		if !float64Eq(test.v.Norm(), test.want) {
			t.Errorf("%v.Norm() = %v, want %v", test.v, test.v.Norm(), test.want)
		}
	}
}

func TestVectorNorm2(t *testing.T) {
	tests := []struct {
		v    Vector
		want float64
	}{
		{Vector{0, 0, 0}, 0},
		{Vector{0, 1, 0}, 1},
		{Vector{1, 1, 1}, 3},
		{Vector{1, 2, 3}, 14},
		{Vector{3, -4, 12}, 169},
		{Vector{1, 1e-16, 1e-32}, 1},
	}
	for _, test := range tests {
		if !float64Eq(test.v.Norm2(), test.want) {
			t.Errorf("%v.Norm2() = %v, want %v", test.v, test.v.Norm2(), test.want)
		}
	}
}

func TestVectorNormalize(t *testing.T) {
	vectors := []Vector{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{1, 1, 1},
		{1, 1e-16, 1e-32},
		{12.34, 56.78, 91.01},
	}
	for _, v := range vectors {
		nv := v.Normalize()
		if !float64Eq(v.X*nv.Y, v.Y*nv.X) || !float64Eq(v.X*nv.Z, v.Z*nv.X) {
			t.Errorf("%v.Normalize() did not preserve direction", v)
		}
		if !float64Eq(nv.Norm(), 1.0) {
			t.Errorf("|%v| = %v, want 1", v, v.Norm())
		}
	}
}

func TestVectorIsUnit(t *testing.T) {
	const epsilon = 1e-14
	tests := []struct {
		v    Vector
		want bool
	}{
		{Vector{0, 0, 0}, false},
		{Vector{0, 1, 0}, true},
		{Vector{1 + 2*epsilon, 0, 0}, true},
		{Vector{1 * (1 + epsilon), 0, 0}, true},
		{Vector{1, 1, 1}, false},
		{Vector{1, 1e-16, 1e-32}, true},
	}
	for _, test := range tests {
		if got := test.v.IsUnit(); got != test.want {
			t.Errorf("%v.IsUnit() = %v, want %v", test.v, got, test.want)
		}
	}
}
func TestVectorDot(t *testing.T) {
	tests := []struct {
		v1, v2 Vector
		want   float64
	}{
		{Vector{1, 0, 0}, Vector{1, 0, 0}, 1},
		{Vector{1, 0, 0}, Vector{0, 1, 0}, 0},
		{Vector{1, 0, 0}, Vector{0, 1, 1}, 0},
		{Vector{1, 1, 1}, Vector{-1, -1, -1}, -3},
		{Vector{1, 2, 2}, Vector{-0.3, 0.4, -1.2}, -1.9},
	}
	for _, test := range tests {
		v1 := Vector{test.v1.X, test.v1.Y, test.v1.Z}
		v2 := Vector{test.v2.X, test.v2.Y, test.v2.Z}
		if !float64Eq(v1.Dot(v2), test.want) {
			t.Errorf("%v · %v = %v, want %v", v1, v2, v1.Dot(v2), test.want)
		}
		if !float64Eq(v2.Dot(v1), test.want) {
			t.Errorf("%v · %v = %v, want %v", v2, v1, v2.Dot(v1), test.want)
		}
	}
}

func TestVectorCross(t *testing.T) {
	tests := []struct {
		v1, v2, want Vector
	}{
		{Vector{1, 0, 0}, Vector{1, 0, 0}, Vector{0, 0, 0}},
		{Vector{1, 0, 0}, Vector{0, 1, 0}, Vector{0, 0, 1}},
		{Vector{0, 1, 0}, Vector{1, 0, 0}, Vector{0, 0, -1}},
		{Vector{1, 2, 3}, Vector{-4, 5, -6}, Vector{-27, -6, 13}},
	}
	for _, test := range tests {
		if got := test.v1.Cross(test.v2); !got.ApproxEqual(test.want) {
			t.Errorf("%v ⨯ %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestVectorAdd(t *testing.T) {
	tests := []struct {
		v1, v2, want Vector
	}{
		{Vector{0, 0, 0}, Vector{0, 0, 0}, Vector{0, 0, 0}},
		{Vector{1, 0, 0}, Vector{0, 0, 0}, Vector{1, 0, 0}},
		{Vector{1, 2, 3}, Vector{4, 5, 7}, Vector{5, 7, 10}},
		{Vector{1, -3, 5}, Vector{1, -6, -6}, Vector{2, -9, -1}},
	}
	for _, test := range tests {
		if got := test.v1.Add(test.v2); !got.ApproxEqual(test.want) {
			t.Errorf("%v + %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestVectorSub(t *testing.T) {
	tests := []struct {
		v1, v2, want Vector
	}{
		{Vector{0, 0, 0}, Vector{0, 0, 0}, Vector{0, 0, 0}},
		{Vector{1, 0, 0}, Vector{0, 0, 0}, Vector{1, 0, 0}},
		{Vector{1, 2, 3}, Vector{4, 5, 7}, Vector{-3, -3, -4}},
		{Vector{1, -3, 5}, Vector{1, -6, -6}, Vector{0, 3, 11}},
	}
	for _, test := range tests {
		if got := test.v1.Sub(test.v2); !got.ApproxEqual(test.want) {
			t.Errorf("%v - %v = %v, want %v", test.v1, test.v2, got, test.want)
		}
	}
}

func TestVectorDistance(t *testing.T) {
	tests := []struct {
		v1, v2 Vector
		want   float64
	}{
		{Vector{1, 0, 0}, Vector{1, 0, 0}, 0},
		{Vector{1, 0, 0}, Vector{0, 1, 0}, 1.41421356237310},
		{Vector{1, 0, 0}, Vector{0, 1, 1}, 1.73205080756888},
		{Vector{1, 1, 1}, Vector{-1, -1, -1}, 3.46410161513775},
		{Vector{1, 2, 2}, Vector{-0.3, 0.4, -1.2}, 3.80657326213486},
	}
	for _, test := range tests {
		v1 := Vector{test.v1.X, test.v1.Y, test.v1.Z}
		v2 := Vector{test.v2.X, test.v2.Y, test.v2.Z}
		if got, want := v1.Distance(v2), test.want; !float64Eq(got, want) {
			t.Errorf("%v.Distance(%v) = %v, want %v", v1, v2, got, want)
		}
		if got, want := v2.Distance(v1), test.want; !float64Eq(got, want) {
			t.Errorf("%v.Distance(%v) = %v, want %v", v2, v1, got, want)
		}
	}
}

func TestVectorMul(t *testing.T) {
	tests := []struct {
		v    Vector
		m    float64
		want Vector
	}{
		{Vector{0, 0, 0}, 3, Vector{0, 0, 0}},
		{Vector{1, 0, 0}, 1, Vector{1, 0, 0}},
		{Vector{1, 0, 0}, 0, Vector{0, 0, 0}},
		{Vector{1, 0, 0}, 3, Vector{3, 0, 0}},
		{Vector{1, -3, 5}, -1, Vector{-1, 3, -5}},
		{Vector{1, -3, 5}, 2, Vector{2, -6, 10}},
	}
	for _, test := range tests {
		if !test.v.Mul(test.m).ApproxEqual(test.want) {
			t.Errorf("%v%v = %v, want %v", test.m, test.v, test.v.Mul(test.m), test.want)
		}
	}
}

func TestVectorAngle(t *testing.T) {
	tests := []struct {
		v1, v2 Vector
		want   float64 // radians
	}{
		{Vector{1, 0, 0}, Vector{1, 0, 0}, 0},
		{Vector{1, 0, 0}, Vector{0, 1, 0}, math.Pi / 2},
		{Vector{1, 0, 0}, Vector{0, 1, 1}, math.Pi / 2},
		{Vector{1, 0, 0}, Vector{-1, 0, 0}, math.Pi},
		{Vector{1, 2, 3}, Vector{2, 3, -1}, 1.2055891055045298},
	}
	for _, test := range tests {
		if a := test.v1.Angle(test.v2).Radians(); !float64Eq(a, test.want) {
			t.Errorf("%v.Angle(%v) = %v, want %v", test.v1, test.v2, a, test.want)
		}
		if a := test.v2.Angle(test.v1).Radians(); !float64Eq(a, test.want) {
			t.Errorf("%v.Angle(%v) = %v, want %v", test.v2, test.v1, a, test.want)
		}
	}
}

func TestVectorOrtho(t *testing.T) {
	vectors := []Vector{
		{1, 0, 0},
		{1, 1, 0},
		{1, 2, 3},
		{1, -2, -5},
		{0.012, 0.0053, 0.00457},
		{-0.012, -1, -0.00457},
	}
	for _, v := range vectors {
		if !float64Eq(v.Dot(v.Ortho()), 0) {
			t.Errorf("%v = not orthogonal to %v.Ortho()", v, v)
		}
		if !float64Eq(v.Ortho().Norm(), 1) {
			t.Errorf("|%v.Ortho()| = %v, want 1", v, v.Ortho().Norm())
		}
	}
}

func TestVectorIdentities(t *testing.T) {
	tests := []struct {
		v1, v2 Vector
	}{
		{Vector{0, 0, 0}, Vector{0, 0, 0}},
		{Vector{0, 0, 0}, Vector{0, 1, 2}},
		{Vector{1, 0, 0}, Vector{0, 1, 0}},
		{Vector{1, 0, 0}, Vector{0, 1, 1}},
		{Vector{1, 1, 1}, Vector{-1, -1, -1}},
		{Vector{1, 2, 2}, Vector{-0.3, 0.4, -1.2}},
	}
	for _, test := range tests {
		a1 := test.v1.Angle(test.v2).Radians()
		a2 := test.v2.Angle(test.v1).Radians()
		c1 := test.v1.Cross(test.v2)
		c2 := test.v2.Cross(test.v1)
		d1 := test.v1.Dot(test.v2)
		d2 := test.v2.Dot(test.v1)
		// Angle commutes
		if !float64Eq(a1, a2) {
			t.Errorf("%v = %v.Angle(%v) != %v.Angle(%v) = %v", a1, test.v1, test.v2, test.v2, test.v1, a2)
		}
		// Dot commutes
		if !float64Eq(d1, d2) {
			t.Errorf("%v = %v · %v != %v · %v = %v", d1, test.v1, test.v2, test.v2, test.v1, d2)
		}
		// Cross anti-commutes
		if !c1.ApproxEqual(c2.Mul(-1.0)) {
			t.Errorf("%v = %v ⨯ %v != -(%v ⨯ %v) = -%v", c1, test.v1, test.v2, test.v2, test.v1, c2)
		}
		// Cross is orthogonal to original vectors
		if !float64Eq(test.v1.Dot(c1), 0.0) {
			t.Errorf("%v · (%v ⨯ %v) = %v != 0", test.v1, test.v1, test.v2, test.v1.Dot(c1))
		}
		if !float64Eq(test.v2.Dot(c1), 0.0) {
			t.Errorf("%v · (%v ⨯ %v) = %v != 0", test.v2, test.v1, test.v2, test.v2.Dot(c1))
		}
	}
}

func TestVectorLargestSmallestComponents(t *testing.T) {
	tests := []struct {
		v                 Vector
		largest, smallest Axis
	}{
		{Vector{0, 0, 0}, ZAxis, ZAxis},
		{Vector{1, 0, 0}, XAxis, ZAxis},
		{Vector{1, -1, 0}, YAxis, ZAxis},
		{Vector{-1, -1.1, -1.1}, ZAxis, XAxis},
		{Vector{0.5, -0.4, -0.5}, ZAxis, YAxis},
		{Vector{1e-15, 1e-14, 1e-13}, ZAxis, XAxis},
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

func TestVectorCmp(t *testing.T) {
	tests := []struct {
		a, b Vector
		want int
	}{
		{Vector{0, 0, 0}, Vector{0, 0, 0}, 0},
		{Vector{0, 0, 0}, Vector{1, 0, 0}, -1},
		{Vector{0, 1, 0}, Vector{0, 0, 0}, 1},
		{Vector{1, 2, 3}, Vector{3, 2, 1}, -1},
		{Vector{-1, 0, 0}, Vector{0, 0, -1}, -1},
		{Vector{8, 6, 4}, Vector{7, 5, 3}, 1},
		{Vector{-1, -0.5, 0}, Vector{0, 0, 0.1}, -1},
		{Vector{1, 2, 3}, Vector{2, 3, 4}, -1},
		{Vector{1.23, 4.56, 7.89}, Vector{1.23, 4.56, 7.89}, 0},
	}

	for _, test := range tests {
		if got := test.a.Cmp(test.b); got != test.want {
			t.Errorf("%v.Cmp(%v) = %d, want %d", test.a, test.b, got, test.want)
		}
	}
}

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

package s1

import (
	"math"
	"testing"
)

// float64Eq reports whether the two values are within the default epsilon.
func float64Eq(x, y float64) bool {
	return float64Near(x, y, epsilon)
}

// float64Near reports whether the two values are within the specified epsilon.
func float64Near(x, y, eps float64) bool {
	return math.Abs(x-y) <= eps
}

func TestEmptyValue(t *testing.T) {
	var a Angle
	if rad := a.Radians(); rad != 0 {
		t.Errorf("Empty value of Angle was %v, want 0", rad)
	}
}

func TestPiRadiansExactly180Degrees(t *testing.T) {
	if rad := (math.Pi * Radian).Radians(); rad != math.Pi {
		t.Errorf("(π * Radian).Radians() was %v, want π", rad)
	}
	if deg := (math.Pi * Radian).Degrees(); deg != 180 {
		t.Errorf("(π * Radian).Degrees() was %v, want 180", deg)
	}
	if rad := (180 * Degree).Radians(); rad != math.Pi {
		t.Errorf("(180 * Degree).Radians() was %v, want π", rad)
	}
	if deg := (180 * Degree).Degrees(); deg != 180 {
		t.Errorf("(180 * Degree).Degrees() was %v, want 180", deg)
	}

	if deg := (math.Pi / 2 * Radian).Degrees(); deg != 90 {
		t.Errorf("(π/2 * Radian).Degrees() was %v, want 90", deg)
	}

	// Check negative angles.
	if deg := (-math.Pi / 2 * Radian).Degrees(); deg != -90 {
		t.Errorf("(-π/2 * Radian).Degrees() was %v, want -90", deg)
	}
	if rad := (-45 * Degree).Radians(); rad != -math.Pi/4 {
		t.Errorf("(-45 * Degree).Radians() was %v, want -π/4", rad)
	}
}

func TestE5E6E7Representation(t *testing.T) {
	// NOTE(dsymonds): This first test gives a variance in the 16th decimal place. I should track that down.
	exp, act := (-45 * Degree).Radians(), (-4500000 * E5).Radians()
	if math.Abs(exp-act) > 1e-15 {
		t.Errorf("(-4500000 * E5).Radians() was %v, want %v", act, exp)
	}
	if exp, act := (-60 * Degree).Radians(), (-60000000 * E6).Radians(); exp != act {
		t.Errorf("(-60000000 * E6).Radians() was %v, want %v", act, exp)
	}
	if exp, act := (75 * Degree).Radians(), (750000000 * E7).Radians(); exp != act {
		t.Errorf("(-750000000 * E7).Radians() was %v, want %v", act, exp)
	}

	if exp, act := int32(-17256123), (-172.56123 * Degree).E5(); exp != act {
		t.Errorf("(-172.56123°).E5() was %v, want %v", act, exp)
	}
	if exp, act := int32(12345678), (12.345678 * Degree).E6(); exp != act {
		t.Errorf("(12.345678°).E6() was %v, want %v", act, exp)
	}
	if exp, act := int32(-123456789), (-12.3456789 * Degree).E7(); exp != act {
		t.Errorf("(-12.3456789°).E7() was %v, want %v", act, exp)
	}

	roundingTests := []struct {
		have Angle
		want int32
	}{
		{0.500000001, 1},
		{-0.500000001, -1},
		{0.499999999, 0},
		{-0.499999999, 0},
	}
	for _, test := range roundingTests {
		if act := (test.have * 1e-5 * Degree).E5(); test.want != act {
			t.Errorf("(%v°).E5() was %v, want %v", test.have, act, test.want)
		}
		if act := (test.have * 1e-6 * Degree).E6(); test.want != act {
			t.Errorf("(%v°).E6() was %v, want %v", test.have, act, test.want)
		}
		if act := (test.have * 1e-7 * Degree).E7(); test.want != act {
			t.Errorf("(%v°).E7() was %v, want %v", test.have, act, test.want)
		}
	}
}

func TestNormalizeCorrectlyCanonicalizesAngles(t *testing.T) {
	tests := []struct {
		in, want float64 // both in degrees
	}{
		{360, 0},
		{-180, 180},
		{180, 180},
		{540, 180},
		{-270, 90},
	}
	for _, test := range tests {
		deg := (Angle(test.in) * Degree).Normalized().Degrees()
		if deg != test.want {
			t.Errorf("Normalized %.0f° = %v, want %v", test.in, deg, test.want)
		}
	}
}

func TestAngleString(t *testing.T) {
	if s, exp := (180 * Degree).String(), "180.0000000"; s != exp {
		t.Errorf("(180°).String() = %q, want %q", s, exp)
	}
}

func TestDegreesVsRadians(t *testing.T) {
	// This test tests the exactness of specific values between degrees and radians.
	for k := -8; k <= 8; k++ {
		if got, want := Angle(45*k)*Degree, Angle((float64(k)*math.Pi)/4)*Radian; got != want {
			t.Errorf("45°*%d != (%d*π)/4 radians (%f vs %f)", k, k, got, want)
		}

		if got, want := (Angle(45*k) * Degree).Degrees(), float64(45*k); got != want {
			t.Errorf("Angle(45°*%d).Degrees() != 45*%d, (%f vs %f)", k, k, got, want)
		}
	}

	for k := uint64(0); k < 30; k++ {
		m := 1 << k
		n := float64(m)
		for _, test := range []struct{ deg, rad float64 }{
			{180, 1},
			{60, 3},
			{36, 5},
			{20, 9},
			{4, 45},
		} {
			if got, want := Angle(test.deg/n)*Degree, Angle(math.Pi/(test.rad*n))*Radian; got != want {
				t.Errorf("%v°/%d != π/%v*%d rad (%f vs %f)", test.deg, m, test.rad, m, got, want)
			}
		}
	}

	// We also spot check a non-identity.
	if got := (60 * Degree).Degrees(); float64Eq(got, 60) {
		t.Errorf("Angle(60).Degrees() == 60, but should not (%f vs %f)", got, 60.0)
	}
}

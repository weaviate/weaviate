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

package s1

import (
	"math"
	"testing"
)

func TestChordAngleBasics(t *testing.T) {
	var zeroChord ChordAngle
	tests := []struct {
		a, b     ChordAngle
		lessThan bool
		equal    bool
	}{
		{NegativeChordAngle, NegativeChordAngle, false, true},
		{NegativeChordAngle, zeroChord, true, false},
		{NegativeChordAngle, StraightChordAngle, true, false},
		{NegativeChordAngle, InfChordAngle(), true, false},

		{zeroChord, zeroChord, false, true},
		{zeroChord, StraightChordAngle, true, false},
		{zeroChord, InfChordAngle(), true, false},

		{StraightChordAngle, StraightChordAngle, false, true},
		{StraightChordAngle, InfChordAngle(), true, false},

		{InfChordAngle(), InfChordAngle(), false, true},
		{InfChordAngle(), StraightChordAngle, false, false},
	}

	for _, test := range tests {
		if got := test.a < test.b; got != test.lessThan {
			t.Errorf("%v should be less than %v", test.a, test.b)
		}
		if got := test.a == test.b; got != test.equal {
			t.Errorf("%v should be equal to %v", test.a, test.b)
		}
	}
}

func TestChordAngleAngleEquality(t *testing.T) {
	var zeroAngle Angle
	var zeroChord ChordAngle

	if InfAngle() != InfChordAngle().Angle() {
		t.Errorf("Infinite ChordAngle to Angle = %v, want %v", InfChordAngle().Angle(), InfAngle())
	}

	oneEighty := 180 * Degree
	if oneEighty != StraightChordAngle.Angle() {
		t.Errorf("Right ChordAngle to degrees = %v, want %v", StraightChordAngle.Angle(), oneEighty)
	}

	if zeroAngle != zeroChord.Angle() {
		t.Errorf("Zero ChordAngle to Angle = %v, want %v", zeroChord.Angle(), zeroAngle)
	}

	d := RightChordAngle.Angle().Degrees()
	if !float64Near(90, d, 1e-13) {
		t.Errorf("Right ChordAngle to degrees = %v, want %v", d, 90)
	}

}

func TestChordAngleIsFunctions(t *testing.T) {
	var zeroChord ChordAngle
	tests := []struct {
		have       ChordAngle
		isNegative bool
		isZero     bool
		isInf      bool
		isSpecial  bool
	}{
		{zeroChord, false, true, false, false},
		{NegativeChordAngle, true, false, false, true},
		{StraightChordAngle, false, false, false, false},
		{InfChordAngle(), false, false, true, true},
	}

	for _, test := range tests {
		if got := test.have < 0; got != test.isNegative {
			t.Errorf("%v.isNegative() = %t, want %t", test.have, got, test.isNegative)
		}
		if got := test.have == 0; got != test.isZero {
			t.Errorf("%v.isZero() = %t, want %t", test.have, got, test.isZero)
		}
		if got := test.have.isInf(); got != test.isInf {
			t.Errorf("%v.isInf() = %t, want %t", test.have, got, test.isInf)
		}
		if got := test.have.isSpecial(); got != test.isSpecial {
			t.Errorf("%v.isSpecial() = %t, want %t", test.have, got, test.isSpecial)
		}
	}
}

func TestChordAngleFromAngle(t *testing.T) {
	for _, angle := range []float64{0, 1, -1, math.Pi} {
		if got := ChordAngleFromAngle(Angle(angle)).Angle().Radians(); got != angle {
			t.Errorf("ChordAngleFromAngle(Angle(%v)) = %v, want %v", angle, got, angle)
		}
	}

	if got := ChordAngleFromAngle(Angle(math.Pi)); got != StraightChordAngle {
		t.Errorf("a ChordAngle from an Angle of π = %v, want %v", got, StraightChordAngle)
	}

	if InfAngle() != ChordAngleFromAngle(InfAngle()).Angle() {
		t.Errorf("converting infinite Angle to ChordAngle should yield infinite Angle")
	}
}

func TestChordAngleArithmetic(t *testing.T) {
	var (
		zero      ChordAngle
		degree30  = ChordAngleFromAngle(30 * Degree)
		degree60  = ChordAngleFromAngle(60 * Degree)
		degree90  = ChordAngleFromAngle(90 * Degree)
		degree120 = ChordAngleFromAngle(120 * Degree)
		degree180 = StraightChordAngle
	)

	addTests := []struct {
		a, b ChordAngle
		want ChordAngle
	}{
		{zero, zero, zero},
		{degree60, zero, degree60},
		{zero, degree60, degree60},
		{degree30, degree60, degree90},
		{degree60, degree30, degree90},
		{degree180, zero, degree180},
		{degree60, degree30, degree90},
		{degree90, degree90, degree180},
		{degree120, degree90, degree180},
		{degree120, degree120, degree180},
		{degree30, degree180, degree180},
		{degree180, degree180, degree180},
	}

	subTests := []struct {
		a, b ChordAngle
		want ChordAngle
	}{
		{zero, zero, zero},
		{degree60, degree60, zero},
		{degree180, degree180, zero},
		{zero, degree60, zero},
		{degree30, degree90, zero},
		{degree90, degree30, degree60},
		{degree90, degree60, degree30},
		{degree180, zero, degree180},
	}

	for _, test := range addTests {
		if got := float64(test.a.Add(test.b)); !float64Eq(got, float64(test.want)) {
			t.Errorf("%v.Add(%v) = %0.24f, want %0.24f", test.a.Angle().Degrees(), test.b.Angle().Degrees(), got, test.want)
		}
	}
	for _, test := range subTests {
		if got := float64(test.a.Sub(test.b)); !float64Eq(got, float64(test.want)) {
			t.Errorf("%v.Sub(%v) = %0.24f, want %0.24f", test.a.Angle().Degrees(), test.b.Angle().Degrees(), got, test.want)
		}
	}
}

func TestChordAngleTrigonometry(t *testing.T) {
	// Because of the way the math works out, the 9/10th's case has slightly more
	// difference than all the other computations, so this gets a more generous
	// epsilon to deal with that.
	const epsilon = 1e-14
	const iters = 40
	for iter := 0; iter <= iters; iter++ {
		radians := math.Pi * float64(iter) / float64(iters)
		angle := ChordAngleFromAngle(Angle(radians))
		if !float64Near(math.Sin(radians), angle.Sin(), epsilon) {
			t.Errorf("(%d/%d)*π. %v.Sin() = %v, want %v", iter, iters, angle, angle.Sin(), math.Sin(radians))
		}
		if !float64Near(math.Cos(radians), angle.Cos(), epsilon) {
			t.Errorf("(%d/%d)*π. %v.Cos() = %v, want %v", iter, iters, angle, angle.Cos(), math.Cos(radians))
		}
		// Since tan(x) is unbounded near pi/4, we map the result back to an
		// angle before comparing. The assertion is that the result is equal to
		// the tangent of a nearby angle.
		if !float64Near(math.Atan(math.Tan(radians)), math.Atan(angle.Tan()), epsilon) {
			t.Errorf("(%d/%d)*π. %v.Tan() = %v, want %v", iter, iters, angle, angle.Tan(), math.Tan(radians))
		}
	}

	// Unlike Angle, ChordAngle can represent 90 and 180 degrees exactly.
	angle90 := ChordAngleFromSquaredLength(2)
	angle180 := ChordAngleFromSquaredLength(4)
	if !float64Eq(1, angle90.Sin()) {
		t.Errorf("%v.Sin() = %v, want 1", angle90, angle90.Sin())
	}
	if !float64Eq(0, angle90.Cos()) {
		t.Errorf("%v.Cos() = %v, want 0", angle90, angle90.Cos())
	}
	if !math.IsInf(angle90.Tan(), 0) {
		t.Errorf("%v.Tan() should be infinite, but was not.", angle90)
	}
	if !float64Eq(0, angle180.Sin()) {
		t.Errorf("%v.Sin() = %v, want 0", angle180, angle180.Sin())
	}
	if !float64Eq(-1, angle180.Cos()) {
		t.Errorf("%v.Cos() = %v, want -1", angle180, angle180.Cos())
	}
	if !float64Eq(0, angle180.Tan()) {
		t.Errorf("%v.Tan() = %v, want 0", angle180, angle180.Tan())
	}
}

func TestChordAngleExpanded(t *testing.T) {
	var zero ChordAngle

	tests := []struct {
		have ChordAngle
		add  float64
		want ChordAngle
	}{
		{NegativeChordAngle, 5, NegativeChordAngle.Expanded(5)},
		{InfChordAngle(), -5, InfChordAngle()},
		{StraightChordAngle, 5, ChordAngleFromSquaredLength(5)},
		{zero, -5, zero},
		{ChordAngleFromSquaredLength(1.25), 0.25, ChordAngleFromSquaredLength(1.5)},
		{ChordAngleFromSquaredLength(0.75), 0.25, ChordAngleFromSquaredLength(1)},
	}

	for _, test := range tests {
		if got := test.have.Expanded(test.add); got != test.want {
			t.Errorf("%v.Expanded(%v) = %v, want %v", test.have, test.add, got, test.want)
		}
	}
}

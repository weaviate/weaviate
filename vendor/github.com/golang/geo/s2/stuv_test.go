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

package s2

import (
	"math"
	"testing"

	"github.com/golang/geo/r3"
)

func TestSTUV(t *testing.T) {
	if x := stToUV(uvToST(.125)); x != .125 {
		t.Error("stToUV(uvToST(.125) == ", x)
	}
	if x := uvToST(stToUV(.125)); x != .125 {
		t.Error("uvToST(stToUV(.125) == ", x)
	}
}

func TestUVNorms(t *testing.T) {
	step := 1 / 1024.0
	for face := 0; face < 6; face++ {
		for x := -1.0; x <= 1; x += step {
			if !float64Eq(float64(faceUVToXYZ(face, x, -1).Cross(faceUVToXYZ(face, x, 1)).Angle(uNorm(face, x))), 0.0) {
				t.Errorf("UNorm not orthogonal to the face(%d)", face)
			}
			if !float64Eq(float64(faceUVToXYZ(face, -1, x).Cross(faceUVToXYZ(face, 1, x)).Angle(vNorm(face, x))), 0.0) {
				t.Errorf("VNorm not orthogonal to the face(%d)", face)
			}
		}
	}
}

func TestFaceUVToXYZ(t *testing.T) {
	// Check that each face appears exactly once.
	var sum r3.Vector
	for face := 0; face < 6; face++ {
		center := faceUVToXYZ(face, 0, 0)
		if !center.ApproxEqual(unitNorm(face).Vector) {
			t.Errorf("faceUVToXYZ(%d, 0, 0) != unitNorm(%d), should be equal", face, face)
		}
		switch center.LargestComponent() {
		case r3.XAxis:
			if math.Abs(center.X) != 1 {
				t.Errorf("%v.X = %v, want %v", center, math.Abs(center.X), 1)
			}
		case r3.YAxis:
			if math.Abs(center.Y) != 1 {
				t.Errorf("%v.Y = %v, want %v", center, math.Abs(center.Y), 1)
			}
		default:
			if math.Abs(center.Z) != 1 {
				t.Errorf("%v.Z = %v, want %v", center, math.Abs(center.Z), 1)
			}
		}
		sum = sum.Add(center.Abs())

		// Check that each face has a right-handed coordinate system.
		if got := uAxis(face).Vector.Cross(vAxis(face).Vector).Dot(unitNorm(face).Vector); got != 1 {
			t.Errorf("right-handed check failed. uAxis(%d).Cross(vAxis(%d)).Dot(unitNorm%v) = %d, want 1", face, face, face, got)
		}

		// Check that the Hilbert curves on each face combine to form a
		// continuous curve over the entire cube.
		// The Hilbert curve on each face starts at (-1,-1) and terminates
		// at either (1,-1) (if axes not swapped) or (-1,1) (if swapped).
		var sign float64 = 1
		if face&swapMask == 1 {
			sign = -1
		}
		if faceUVToXYZ(face, sign, -sign) != faceUVToXYZ((face+1)%6, -1, -1) {
			t.Errorf("faceUVToXYZ(%v, %v, %v) != faceUVToXYZ(%v, -1, -1)", face, sign, -sign, (face+1)%6)
		}
	}

	// Adding up the absolute value all all the face normals should equal 2 on each axis.
	if !sum.ApproxEqual(r3.Vector{2, 2, 2}) {
		t.Errorf("sum of the abs of the 6 face norms should = %v, got %v", r3.Vector{2, 2, 2}, sum)
	}
}

func TestFaceXYZToUV(t *testing.T) {
	var (
		point    = Point{r3.Vector{1.1, 1.2, 1.3}}
		pointNeg = Point{r3.Vector{-1.1, -1.2, -1.3}}
	)

	tests := []struct {
		face  int
		point Point
		u     float64
		v     float64
		ok    bool
	}{
		{0, point, 1 + (1.0 / 11), 1 + (2.0 / 11), true},
		{0, pointNeg, 0, 0, false},
		{1, point, -11.0 / 12, 1 + (1.0 / 12), true},
		{1, pointNeg, 0, 0, false},
		{2, point, -11.0 / 13, -12.0 / 13, true},
		{2, pointNeg, 0, 0, false},
		{3, point, 0, 0, false},
		{3, pointNeg, 1 + (2.0 / 11), 1 + (1.0 / 11), true},
		{4, point, 0, 0, false},
		{4, pointNeg, 1 + (1.0 / 12), -(11.0 / 12), true},
		{5, point, 0, 0, false},
		{5, pointNeg, -12.0 / 13, -11.0 / 13, true},
	}

	for _, test := range tests {
		if u, v, ok := faceXYZToUV(test.face, test.point); !float64Eq(u, test.u) || !float64Eq(v, test.v) || ok != test.ok {
			t.Errorf("faceXYZToUV(%d, %v) = %f, %f, %t, want %f, %f, %t", test.face, test.point, u, v, ok, test.u, test.v, test.ok)
		}
	}
}

func TestFaceXYZtoUVW(t *testing.T) {
	var (
		origin = Point{r3.Vector{0, 0, 0}}
		posX   = Point{r3.Vector{1, 0, 0}}
		negX   = Point{r3.Vector{-1, 0, 0}}
		posY   = Point{r3.Vector{0, 1, 0}}
		negY   = Point{r3.Vector{0, -1, 0}}
		posZ   = Point{r3.Vector{0, 0, 1}}
		negZ   = Point{r3.Vector{0, 0, -1}}
	)

	for face := 0; face < 6; face++ {
		if got := faceXYZtoUVW(face, origin); got != origin {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, origin, got, origin)
		}

		if got := faceXYZtoUVW(face, uAxis(face)); got != posX {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, uAxis(face), got, posX)
		}

		if got := faceXYZtoUVW(face, Point{uAxis(face).Mul(-1)}); got != negX {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, uAxis(face).Mul(-1), got, negX)
		}

		if got := faceXYZtoUVW(face, vAxis(face)); got != posY {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, vAxis(face), got, posY)
		}

		if got := faceXYZtoUVW(face, Point{vAxis(face).Mul(-1)}); got != negY {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, vAxis(face).Mul(-1), got, negY)
		}

		if got := faceXYZtoUVW(face, unitNorm(face)); got != posZ {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, unitNorm(face), got, posZ)
		}

		if got := faceXYZtoUVW(face, Point{unitNorm(face).Mul(-1)}); got != negZ {
			t.Errorf("faceXYZtoUVW(%d, %v) = %v, want %v", face, unitNorm(face).Mul(-1), got, negZ)
		}
	}
}

func TestUVWAxis(t *testing.T) {
	for face := 0; face < 6; face++ {
		// Check that the axes are consistent with faceUVtoXYZ.
		if faceUVToXYZ(face, 1, 0).Sub(faceUVToXYZ(face, 0, 0)) != uAxis(face).Vector {
			t.Errorf("face 1,0 - face 0,0 should equal uAxis")
		}
		if faceUVToXYZ(face, 0, 1).Sub(faceUVToXYZ(face, 0, 0)) != vAxis(face).Vector {
			t.Errorf("faceUVToXYZ(%d, 0, 1).Sub(faceUVToXYZ(%d, 0, 0)) != vAxis(%d), should be equal.", face, face, face)
		}
		if faceUVToXYZ(face, 0, 0) != unitNorm(face).Vector {
			t.Errorf("faceUVToXYZ(%d, 0, 0) != unitNorm(%d), should be equal", face, face)
		}

		// Check that every face coordinate frame is right-handed.
		if got := uAxis(face).Vector.Cross(vAxis(face).Vector).Dot(unitNorm(face).Vector); got != 1 {
			t.Errorf("right-handed check failed. got %d, want 1", got)
		}

		// Check that GetUVWAxis is consistent with GetUAxis, GetVAxis, GetNorm.
		if uAxis(face) != uvwAxis(face, 0) {
			t.Errorf("uAxis(%d) != uvwAxis(%d, 0), should be equal", face, face)
		}
		if vAxis(face) != uvwAxis(face, 1) {
			t.Errorf("vAxis(%d) != uvwAxis(%d, 1), should be equal", face, face)
		}
		if unitNorm(face) != uvwAxis(face, 2) {
			t.Errorf("unitNorm(%d) != uvwAxis(%d, 2), should be equal", face, face)
		}
	}
}

func TestSiTiSTRoundtrip(t *testing.T) {
	// test int -> float -> int direction.
	for i := 0; i < 1000; i++ {
		si := uint32(randomUniformInt(maxSiTi))
		if got := stToSiTi(siTiToST(si)); got != si {
			t.Errorf("stToSiTi(siTiToST(%v)) = %v, want %v", si, got, si)
		}
	}
	// test float -> int -> float direction.
	for i := 0; i < 1000; i++ {
		st := randomUniformFloat64(0, 1.0)
		// this uses near not exact because there is some loss in precision
		// when scaling down to the nearest 1/maxLevel and back.
		if got := siTiToST(stToSiTi(st)); !float64Near(got, st, 1e-8) {
			t.Errorf("siTiToST(stToSiTi(%v)) = %v, want %v", st, got, st)
		}
	}
}

func TestUVWFace(t *testing.T) {
	// Check that uvwFace is consistent with uvwAxis.
	for f := 0; f < 6; f++ {
		for axis := 0; axis < 3; axis++ {
			if got, want := face(uvwAxis(f, axis).Mul(-1)), uvwFace(f, axis, 0); got != want {
				t.Errorf("face(%v) in positive direction = %v, want %v", uvwAxis(f, axis).Mul(-1), got, want)
			}
			if got, want := face(uvwAxis(f, axis).Vector), uvwFace(f, axis, 1); got != want {
				t.Errorf("face(%v) in negative direction = %v, want %v", uvwAxis(f, axis), got, want)
			}
		}
	}
}

func TestXYZToFaceSiTi(t *testing.T) {
	for level := 0; level < maxLevel; level++ {
		for i := 0; i < 1000; i++ {
			ci := randomCellIDForLevel(level)
			f, si, ti, gotLevel := xyzToFaceSiTi(ci.Point())
			if gotLevel != level {
				t.Errorf("level of CellID %v = %v, want %v", ci, gotLevel, level)
			}
			gotID := cellIDFromFaceIJ(f, int(si/2), int(ti/2)).Parent(level)
			if gotID != ci {
				t.Errorf("CellID = %b, want %b", gotID, ci)
			}

			// Test a point near the cell center but not equal to it.
			pMoved := ci.Point().Add(r3.Vector{1e-13, 1e-13, 1e-13})
			fMoved, siMoved, tiMoved, gotLevel := xyzToFaceSiTi(Point{pMoved})

			if gotLevel != -1 {
				t.Errorf("level of %v = %v, want %v", pMoved, gotLevel, -1)
			}

			if f != fMoved {
				t.Errorf("face of %v = %v, want %v", pMoved, fMoved, f)
			}

			if si != siMoved {
				t.Errorf("si of %v = %v, want %v", pMoved, siMoved, si)
			}

			if ti != tiMoved {
				t.Errorf("ti of %v = %v, want %v", pMoved, tiMoved, ti)
			}

			// Finally, test some random (si,ti) values that may be at different
			// levels, or not at a valid level at all (for example, si == 0).
			faceRandom := randomUniformInt(numFaces)
			mask := -1 << uint32(maxLevel-level)
			siRandom := uint32(randomUint32() & uint32(mask))
			tiRandom := uint32(randomUint32() & uint32(mask))
			for siRandom > maxSiTi || tiRandom > maxSiTi {
				siRandom = uint32(randomUint32() & uint32(mask))
				tiRandom = uint32(randomUint32() & uint32(mask))
			}

			pRandom := faceSiTiToXYZ(faceRandom, siRandom, tiRandom)
			f, si, ti, gotLevel = xyzToFaceSiTi(pRandom)

			// The chosen point is on the edge of a top-level face cell.
			if f != faceRandom {
				if gotLevel != -1 {
					t.Errorf("level of random CellID = %v, want %v", gotLevel, -1)
				}
				if !(si == 0 || si == maxSiTi || ti == 0 || ti == maxSiTi) {
					t.Errorf("face %d, si = %v, ti = %v, want 0 or %v for both", f, si, ti, maxSiTi)
				}
				continue
			}

			if siRandom != si {
				t.Errorf("xyzToFaceSiTi(%v).si = %v, want %v", pRandom, siRandom, si)
			}
			if tiRandom != ti {
				t.Errorf("xyzToFaceSiTi(%v).ti = %v, want %v", pRandom, tiRandom, ti)
			}
			if gotLevel >= 0 {
				if got := cellIDFromFaceIJ(f, int(si/2), int(ti/2)).Parent(gotLevel).Point(); !pRandom.ApproxEqual(got) {
					t.Errorf("cellIDFromFaceIJ(%d, %d, %d).Parent(%d) = %v, want %v", f, si/2, ti/2, gotLevel, got, pRandom)
				}
			}
		}
	}
}

func TestXYZFaceSiTiRoundtrip(t *testing.T) {
	for level := 0; level < maxLevel; level++ {
		for i := 0; i < 1000; i++ {
			ci := randomCellIDForLevel(level)
			f, si, ti, _ := xyzToFaceSiTi(ci.Point())
			op := faceSiTiToXYZ(f, si, ti)
			if !ci.Point().ApproxEqual(op) {
				t.Errorf("faceSiTiToXYZ(xyzToFaceSiTi(%v)) = %v, want %v", ci.Point(), op, ci.Point())
			}
		}
	}
}

func TestSTUVFace(t *testing.T) {
	tests := []struct {
		v    r3.Vector
		want int
	}{
		{r3.Vector{-1, -1, -1}, 5},
		{r3.Vector{-1, -1, 0}, 4},
		{r3.Vector{-1, -1, 1}, 2},
		{r3.Vector{-1, 0, -1}, 5},
		{r3.Vector{-1, 0, 0}, 3},
		{r3.Vector{-1, 0, 1}, 2},
		{r3.Vector{-1, 1, -1}, 5},
		{r3.Vector{-1, 1, 0}, 1},
		{r3.Vector{-1, 1, 1}, 2},
		{r3.Vector{0, -1, -1}, 5},
		{r3.Vector{0, -1, 0}, 4},
		{r3.Vector{0, -1, 1}, 2},
		{r3.Vector{0, 0, -1}, 5},
		{r3.Vector{0, 0, 0}, 2},
		{r3.Vector{0, 0, 1}, 2},
		{r3.Vector{0, 1, -1}, 5},
		{r3.Vector{0, 1, 0}, 1},
		{r3.Vector{0, 1, 1}, 2},
		{r3.Vector{1, -1, -1}, 5},
		{r3.Vector{1, -1, 0}, 4},
		{r3.Vector{1, -1, 1}, 2},
		{r3.Vector{1, 0, -1}, 5},
		{r3.Vector{1, 0, 0}, 0},
		{r3.Vector{1, 0, 1}, 2},
		{r3.Vector{1, 1, -1}, 5},
		{r3.Vector{1, 1, 0}, 1},
		{r3.Vector{1, 1, 1}, 2},
	}

	for _, test := range tests {
		if got := face(test.v); got != test.want {
			t.Errorf("face(%v) = %d, want %d", test.v, got, test.want)
		}
	}
}

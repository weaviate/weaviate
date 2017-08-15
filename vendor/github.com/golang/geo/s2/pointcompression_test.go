/*
Copyright 2017 Google Inc. All rights reserved.

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
	"bytes"
	"reflect"
	"testing"
)

func TestFacesIterator(t *testing.T) {
	iter := facesIterator{faces: []faceRun{{1, 1}, {2, 2}, {0, 1}}}
	var got []int
	for iter.next() {
		got = append(got, iter.curFace)
	}
	want := []int{1, 2, 2, 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("facesIterator produced sequence %v, want %v", got, want)
	}
}

func makeSnappedPoints(nvertices int, level int) []Point {
	const radiusKM = 0.1
	center := PointFromCoords(1, 1, 1)
	pts := regularPoints(center, kmToAngle(radiusKM), nvertices)
	for i, pt := range pts {
		id := CellFromPoint(pt).ID()
		if level < id.Level() {
			pts[i] = id.Parent(level).Point()
		}
	}
	return pts
}

func TestPointsCompression(t *testing.T) {
	loop100Mixed15 := makeSnappedPoints(100, maxLevel)
	for i := 0; i < 15; i++ {
		loop100Mixed15[3*i] = CellFromPoint(loop100Mixed15[3*i]).ID().Parent(4).Point()
	}

	tests := []struct {
		label string
		pts   []Point
		level int
	}{
		{"loop4", makeSnappedPoints(4, maxLevel), maxLevel},
		{"loop4unsnapped", makeSnappedPoints(4, 4), maxLevel},
		// Radius is 100m, so points are about 141 meters apart.
		// Snapping to level 14 will move them by < 47m.
		{"loop4Level14", makeSnappedPoints(4, 14), 14},
		{"loop100", makeSnappedPoints(100, maxLevel), maxLevel},
		{"loop100Unsnapped", makeSnappedPoints(100, 100), maxLevel},
		{"loop100Mixed15", loop100Mixed15, maxLevel},
	}

NextTest:
	for _, tt := range tests {
		sitis := (&Loop{vertices: tt.pts}).xyzFaceSiTiVertices()
		var buf bytes.Buffer
		e := &encoder{w: &buf}
		encodePointsCompressed(e, sitis, tt.level)
		if e.err != nil {
			t.Errorf("encodePointsCompressed (%s): %v", tt.label, e.err)
			continue
		}

		d := &decoder{r: &buf}
		got := make([]Point, len(tt.pts))
		decodePointsCompressed(d, tt.level, got)
		if d.err != nil {
			t.Errorf("decodePointsCompressed (%s): %v", tt.label, d.err)
		}

		for i, want := range tt.pts {
			if !got[i].ApproxEqual(want) {
				t.Errorf("decodePointsCompressed(encodePointsCompressed) for %s at %d: got %v want %v", tt.label, i, got[i], want)
				continue NextTest
			}
		}
	}
}

func TestZigzag(t *testing.T) {
	tab := []struct {
		signed   int32
		unsigned uint32
	}{
		{0, 0}, {-1, 1}, {1, 2}, {-2, 3}, {2147483647, 4294967294}, {-2147483648, 4294967295},
	}
	for _, tt := range tab {
		if s := zigzagDecode(tt.unsigned); s != tt.signed {
			t.Errorf("zigzagDecode(%d) = %d, want %d", tt.unsigned, s, tt.signed)
		}
		if u := zigzagEncode(tt.signed); u != tt.unsigned {
			t.Errorf("zigzagEncode(%d) = %d, want %d", tt.signed, u, tt.unsigned)
		}
	}
}

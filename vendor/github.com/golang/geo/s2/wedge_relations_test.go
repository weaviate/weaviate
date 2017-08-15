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
	"testing"

	"github.com/golang/geo/r3"
)

func TestWedgeRelations(t *testing.T) {
	// For simplicity, all of these tests use an origin of (0, 0, 1).
	// This shouldn't matter as long as the lower-level primitives are
	// implemented correctly.
	ab1 := Point{r3.Vector{0, 0, 1}.Normalize()}

	tests := []struct {
		desc           string
		a0, a1, b0, b1 Point
		contains       bool
		intersects     bool
		relation       WedgeRel
	}{
		{
			desc:       "Intersection in one wedge",
			a0:         Point{r3.Vector{-1, 0, 10}},
			a1:         Point{r3.Vector{1, 2, 10}},
			b0:         Point{r3.Vector{0, 1, 10}},
			b1:         Point{r3.Vector{1, -2, 10}},
			contains:   false,
			intersects: true,
			relation:   WedgeProperlyOverlaps,
		},
		{
			desc:       "Intersection in two wedges",
			a0:         Point{r3.Vector{-1, -1, 10}},
			a1:         Point{r3.Vector{1, -1, 10}},
			b0:         Point{r3.Vector{1, 0, 10}},
			b1:         Point{r3.Vector{-1, 1, 10}},
			contains:   false,
			intersects: true,
			relation:   WedgeProperlyOverlaps,
		},
		{
			desc:       "Normal containment",
			a0:         Point{r3.Vector{-1, -1, 10}},
			a1:         Point{r3.Vector{1, -1, 10}},
			b0:         Point{r3.Vector{-1, 0, 10}},
			b1:         Point{r3.Vector{1, 0, 10}},
			contains:   true,
			intersects: true,
			relation:   WedgeProperlyContains,
		},
		{
			desc:       "Containment with equality on one side",
			a0:         Point{r3.Vector{2, 1, 10}},
			a1:         Point{r3.Vector{-1, -1, 10}},
			b0:         Point{r3.Vector{2, 1, 10}},
			b1:         Point{r3.Vector{1, -5, 10}},
			contains:   true,
			intersects: true,
			relation:   WedgeProperlyContains,
		},
		{
			desc:       "Containment with equality on the other side",
			a0:         Point{r3.Vector{2, 1, 10}},
			a1:         Point{r3.Vector{-1, -1, 10}},
			b0:         Point{r3.Vector{1, -2, 10}},
			b1:         Point{r3.Vector{-1, -1, 10}},
			contains:   true,
			intersects: true,
			relation:   WedgeProperlyContains,
		},
		{
			desc:       "Containment with equality on both sides",
			a0:         Point{r3.Vector{-2, 3, 10}},
			a1:         Point{r3.Vector{4, -5, 10}},
			b0:         Point{r3.Vector{-2, 3, 10}},
			b1:         Point{r3.Vector{4, -5, 10}},
			contains:   true,
			intersects: true,
			relation:   WedgeEquals,
		},
		{
			desc:       "Disjoint with equality on one side",
			a0:         Point{r3.Vector{-2, 3, 10}},
			a1:         Point{r3.Vector{4, -5, 10}},
			b0:         Point{r3.Vector{4, -5, 10}},
			b1:         Point{r3.Vector{-2, -3, 10}},
			contains:   false,
			intersects: false,
			relation:   WedgeIsDisjoint,
		},
		{
			desc:       "Disjoint with equality on the other side",
			a0:         Point{r3.Vector{-2, 3, 10}},
			a1:         Point{r3.Vector{0, 5, 10}},
			b0:         Point{r3.Vector{4, -5, 10}},
			b1:         Point{r3.Vector{-2, 3, 10}},
			contains:   false,
			intersects: false,
			relation:   WedgeIsDisjoint,
		},
		{
			desc:       "Disjoint with equality on both sides",
			a0:         Point{r3.Vector{-2, 3, 10}},
			a1:         Point{r3.Vector{4, -5, 10}},
			b0:         Point{r3.Vector{4, -5, 10}},
			b1:         Point{r3.Vector{-2, 3, 10}},
			contains:   false,
			intersects: false,
			relation:   WedgeIsDisjoint,
		},
		{
			desc:       "B contains A with equality on one side",
			a0:         Point{r3.Vector{2, 1, 10}},
			a1:         Point{r3.Vector{1, -5, 10}},
			b0:         Point{r3.Vector{2, 1, 10}},
			b1:         Point{r3.Vector{-1, -1, 10}},
			contains:   false,
			intersects: true,
			relation:   WedgeIsProperlyContained,
		},

		{
			desc:       "B contains A with equality on the other side",
			a0:         Point{r3.Vector{2, 1, 10}},
			a1:         Point{r3.Vector{1, -5, 10}},
			b0:         Point{r3.Vector{-2, 1, 10}},
			b1:         Point{r3.Vector{1, -5, 10}},
			contains:   false,
			intersects: true,
			relation:   WedgeIsProperlyContained,
		},
	}

	for _, test := range tests {
		test.a0 = Point{test.a0.Normalize()}
		test.a1 = Point{test.a1.Normalize()}
		test.b0 = Point{test.b0.Normalize()}
		test.b1 = Point{test.b1.Normalize()}

		if got := WedgeContains(test.a0, ab1, test.a1, test.b0, test.b1); got != test.contains {
			t.Errorf("%s: WedgeContains(%v, %v, %v, %v, %v) = %t, want %t", test.desc, test.a0, ab1, test.a1, test.b0, test.b1, got, test.contains)
		}
		if got := WedgeIntersects(test.a0, ab1, test.a1, test.b0, test.b1); got != test.intersects {
			t.Errorf("%s: WedgeIntersects(%v, %v, %v, %v, %v) = %t, want %t", test.desc, test.a0, ab1, test.a1, test.b0, test.b1, got, test.intersects)
		}
		if got := WedgeRelation(test.a0, ab1, test.a1, test.b0, test.b1); got != test.relation {
			t.Errorf("%s: WedgeRelation(%v, %v, %v, %v, %v) = %v, want %v", test.desc, test.a0, ab1, test.a1, test.b0, test.b1, got, test.relation)
		}
	}
}

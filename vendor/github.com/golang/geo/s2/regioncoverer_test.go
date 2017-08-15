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
	"math/rand"
	"reflect"
	"testing"
)

func TestCovererRandomCells(t *testing.T) {
	rc := &RegionCoverer{MinLevel: 0, MaxLevel: 30, LevelMod: 1, MaxCells: 1}

	// Test random cell ids at all levels.
	for i := 0; i < 10000; i++ {
		id := CellID(randomUint64())
		for !id.IsValid() {
			id = CellID(randomUint64())
		}
		covering := rc.Covering(Region(CellFromCellID(id)))
		if len(covering) != 1 {
			t.Errorf("Iteration %d, cell ID token %s, got covering size = %d, want covering size = 1", i, id.ToToken(), len(covering))
		}
		if (covering)[0] != id {
			t.Errorf("Iteration %d, cell ID token %s, got covering = %v, want covering = %v", i, id.ToToken(), covering, id)
		}
	}
}

// checkCovering reports whether covering is a valid cover for the region.
func checkCovering(t *testing.T, rc *RegionCoverer, r Region, covering CellUnion, interior bool) {
	// Keep track of how many cells have the same rc.MinLevel ancestor.
	minLevelCells := map[CellID]int{}
	var tempCover CellUnion
	for _, ci := range covering {
		level := ci.Level()
		if level < rc.MinLevel {
			t.Errorf("CellID(%s).Level() = %d, want >= %d", ci.ToToken(), level, rc.MinLevel)
		}
		if level > rc.MaxLevel {
			t.Errorf("CellID(%s).Level() = %d, want <= %d", ci.ToToken(), level, rc.MaxLevel)
		}
		if rem := (level - rc.MinLevel) % rc.LevelMod; rem != 0 {
			t.Errorf("(CellID(%s).Level() - MinLevel) mod LevelMod = %d, want = %d", ci.ToToken(), rem, 0)
		}
		tempCover = append(tempCover, ci)
		minLevelCells[ci.Parent(rc.MinLevel)]++
	}
	if len(covering) > rc.MaxCells {
		// If the covering has more than the requested number of cells, then check
		// that the cell count cannot be reduced by using the parent of some cell.
		for ci, count := range minLevelCells {
			if count > 1 {
				t.Errorf("Min level CellID %s, count = %d, want = %d", ci.ToToken(), count, 1)
			}
		}
	}
	if interior {
		for _, ci := range covering {
			if !r.ContainsCell(CellFromCellID(ci)) {
				t.Errorf("Region(%v).ContainsCell(%v) = %t, want = %t", r, CellFromCellID(ci), false, true)
			}
		}
	} else {
		tempCover.Normalize()
		checkCoveringTight(t, r, tempCover, true, 0)
	}
}

// checkCoveringTight checks that "cover" completely covers the given region.
// If "checkTight" is true, also checks that it does not contain any cells that
// do not intersect the given region. ("id" is only used internally.)
func checkCoveringTight(t *testing.T, r Region, cover CellUnion, checkTight bool, id CellID) {
	if !id.IsValid() {
		for f := 0; f < 6; f++ {
			checkCoveringTight(t, r, cover, checkTight, CellIDFromFace(f))
		}
		return
	}

	if !r.IntersectsCell(CellFromCellID(id)) {
		// If region does not intersect id, then neither should the covering.
		if got := cover.IntersectsCellID(id); checkTight && got {
			t.Errorf("CellUnion(%v).IntersectsCellID(%s) = %t; want = %t", cover, id.ToToken(), got, false)
		}
	} else if !cover.ContainsCellID(id) {
		// The region may intersect id, but we can't assert that the covering
		// intersects id because we may discover that the region does not actually
		// intersect upon further subdivision.  (IntersectsCell is not exact.)
		if got := r.ContainsCell(CellFromCellID(id)); got {
			t.Errorf("Region(%v).ContainsCell(%v) = %t; want = %t", r, CellFromCellID(id), got, false)
		}
		if got := id.IsLeaf(); got {
			t.Errorf("CellID(%s).IsLeaf() = %t; want = %t", id.ToToken(), got, false)
		}

		for child := id.ChildBegin(); child != id.ChildEnd(); child = child.Next() {
			checkCoveringTight(t, r, cover, checkTight, child)
		}
	}
}

func TestCovererRandomCaps(t *testing.T) {
	rc := &RegionCoverer{}
	for i := 0; i < 1000; i++ {
		rc.MinLevel = int(rand.Int31n(maxLevel + 1))
		rc.MaxLevel = int(rand.Int31n(maxLevel + 1))
		for rc.MinLevel > rc.MaxLevel {
			rc.MinLevel = int(rand.Int31n(maxLevel + 1))
			rc.MaxLevel = int(rand.Int31n(maxLevel + 1))
		}
		rc.LevelMod = int(1 + rand.Int31n(3))
		rc.MaxCells = int(skewedInt(10))

		maxArea := math.Min(4*math.Pi, float64(3*rc.MaxCells+1)*AvgAreaMetric.Value(rc.MinLevel))
		r := Region(randomCap(0.1*AvgAreaMetric.Value(maxLevel), maxArea))

		covering := rc.Covering(r)
		checkCovering(t, rc, r, covering, false)
		interior := rc.InteriorCovering(r)
		checkCovering(t, rc, r, interior, true)

		// Check that Covering is deterministic.
		covering2 := rc.Covering(r)
		if !reflect.DeepEqual(covering, covering2) {
			t.Errorf("Iteration %d, got covering = %v, want covering = %v", i, covering2, covering)
		}

		// Also check Denormalize. The denormalized covering
		// may still be different and smaller than "covering" because
		// s2.RegionCoverer does not guarantee that it will not output all four
		// children of the same parent.
		covering.Denormalize(rc.MinLevel, rc.LevelMod)
		checkCovering(t, rc, r, covering, false)
	}
}

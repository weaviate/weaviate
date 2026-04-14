//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// correlationTestProps returns the shared test schema used across all plan
// tests (same structure as the design summary):
//
//	nestedObject: object {
//	  name:   text
//	  owner:  object  { firstname, lastname text; nicknames text[] }
//	  addresses: object[] { city, postcode text; numbers number[] }
//	  tags:   text[]
//	  cars:   object[] {
//	    make:        text
//	    tires:       object[] { width int; radiuses int[]; bolts object[]{size int}; caps object[]{color text} }
//	    accessories: object[] { type text }
//	    colors:      text[]
//	  }
//	}
func correlationTestProps() []*models.NestedProperty {
	vTrue := true
	return []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
			},
		},
		{
			Name:     "addresses",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString(), IndexFilterable: &vTrue},
			},
		},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{
					Name:     "tires",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
						{Name: "radiuses", DataType: schema.DataTypeIntArray.PropString(), IndexFilterable: &vTrue},
						// bolts and caps are sub-arrays of tires, enabling nested idxLoopAnd tests.
						{
							Name:     "bolts",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "size", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
							},
						},
						{
							Name:     "caps",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
							},
						},
					},
				},
				{
					Name:     "accessories",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
					},
				},
				{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
			},
		},
	}
}

func TestConditionCountsIsMasked(t *testing.T) {
	// isMasked returns true when: independents>1 OR (tokens>0 AND independents>0)
	for _, tt := range []struct {
		name         string
		tokens       int
		independents int
		want         bool
	}{
		// not masked
		{"zero counts", 0, 0, false},
		{"tokens only, no independent", 2, 0, false},
		{"single independent, no tokens", 0, 1, false},
		// masked: multiple independents
		{"two independents", 0, 2, true},
		{"three independents", 0, 3, true},
		// masked: tokens + any independent
		{"one token + one independent", 1, 1, true},
		{"two tokens + one independent", 2, 1, true},
		{"one token + two independents", 1, 2, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := conditionCounts{"p": {tt.tokens, tt.independents}}
			assert.Equal(t, tt.want, c.isMasked("p"))
		})
	}
}

func TestBuildExecutionPlanNoPaths(t *testing.T) {
	props := correlationTestProps()
	_, err := newExecutionPlanBuilder(props).build(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no paths")
}

func TestBuildExecutionPlan(t *testing.T) {
	props := correlationTestProps()

	// counts helper: all single conditions (no masked results)
	single := func(paths ...string) map[string][2]int {
		m := make(map[string][2]int, len(paths))
		for _, p := range paths {
			m[p] = [2]int{0, 1}
		}
		return m
	}
	// masked: tokens+1 independent
	tokenInd := func(paths ...string) map[string][2]int {
		m := make(map[string][2]int, len(paths))
		for _, p := range paths {
			m[p] = [2]int{1, 1}
		}
		return m
	}
	// masked: 2 independents
	multiInd := func(paths ...string) map[string][2]int {
		m := make(map[string][2]int, len(paths))
		for _, p := range paths {
			m[p] = [2]int{0, 2}
		}
		return m
	}

	for _, tt := range []struct {
		name   string
		paths  []string
		counts map[string][2]int
		// expected: one check per group in the plan
		wantGroups []conditionGroup
	}{
		// ---------------------------------------------------------------
		// Single path
		// ---------------------------------------------------------------
		{
			name:   "single scalar — groupAndAll, no meta needed",
			paths:  []string{"cars.make"},
			counts: single("cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make"}},
			},
		},
		{
			name:   "single multi-condition text[] through intermediate array — groupRunIdxLoop",
			paths:  []string{"cars.colors"},
			counts: multiInd("cars.colors"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.colors"}},
			},
		},
		{
			name:   "single multi-condition at root array (no intermediate) — groupAndAllMaskLeaf",
			paths:  []string{"tags"},
			counts: multiInd("tags"),
			wantGroups: []conditionGroup{
				{op: groupAndAllMaskLeaf, lcaPath: "", paths: []string{"tags"}},
			},
		},
		{
			name:   "single token+ind path through intermediate array — groupRunIdxLoop",
			paths:  []string{"cars.colors"},
			counts: tokenInd("cars.colors"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.colors"}},
			},
		},

		// ---------------------------------------------------------------
		// Regression: single-segment lcaPath must always use groupRunIdxLoop,
		// never groupAndAllMaskLeaf — root_idx encodes the ROOT element, not the
		// intermediate array element, so MaskLeaf cannot distinguish conditions
		// landing in different intermediate elements of the same root element.
		// ---------------------------------------------------------------
		{
			// cars (depth-1 intermediate object[]) — multi-condition on same path.
			// Even though lcaPath="cars" is a single segment, root_idx encodes the
			// parent (root) element, not the cars element. Two tags in different
			// cars of the same root element share root_idx → AndAllMaskLeaf would
			// produce a false positive. Only groupRunIdxLoop is correct.
			name:   "[regression] depth-1 intermediate, multi-condition same path — must be groupRunIdxLoop not groupAndAllMaskLeaf",
			paths:  []string{"cars.colors"},
			counts: multiInd("cars.colors"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.colors"}},
			},
		},
		{
			// addresses (depth-1 intermediate object[]) — multi-condition same path.
			name:   "[regression] depth-1 intermediate addresses, multi-condition — must be groupRunIdxLoop",
			paths:  []string{"addresses.numbers"},
			counts: multiInd("addresses.numbers"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "addresses", paths: []string{"addresses.numbers"}},
			},
		},
		{
			// Depth-2 intermediate (cars.tires) — multi-condition same path.
			// Same reasoning applies at any depth.
			name:   "[regression] depth-2 intermediate, multi-condition same path — must be groupRunIdxLoop",
			paths:  []string{"cars.tires.radiuses"},
			counts: multiInd("cars.tires.radiuses"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars.tires", paths: []string{"cars.tires.radiuses"}},
			},
		},
		{
			// Single path, single condition through intermediate array — still needs
			// groupAndAll because positions are naturally aligned (no masking needed).
			// Contrast: this is groupAndAll, not groupRunIdxLoop, because isMasked=false
			// and lastIntermediateObjectArray("cars.make")="cars"=lcaPath.
			// Kept here to anchor the boundary between the two ops.
			name:   "[regression] depth-1 intermediate, single condition — groupAndAll (not idxLoop, positions aligned)",
			paths:  []string{"cars.make"},
			counts: single("cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make"}},
			},
		},

		// ---------------------------------------------------------------
		// Two paths — same first segment (grouped together)
		// ---------------------------------------------------------------
		{
			name:   "scalar siblings in cars[] — groupAndAll (no further object[])",
			paths:  []string{"cars.make", "cars.colors"},
			counts: single("cars.make", "cars.colors"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make", "cars.colors"}},
			},
		},
		{
			name:   "tires + accessories (both through object[]) — groupRunIdxLoop(cars)",
			paths:  []string{"cars.tires.width", "cars.accessories.type"},
			counts: single("cars.tires.width", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.accessories.type"}},
			},
		},
		{
			name:   "make + tires.width (one through object[]) — groupRunIdxLoop(cars)",
			paths:  []string{"cars.make", "cars.tires.width"},
			counts: single("cars.make", "cars.tires.width"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.make", "cars.tires.width"}},
			},
		},
		{
			name:   "tires.width + tires.radiuses (scalar siblings in tires[]) — groupAndAll",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses"},
			counts: single("cars.tires.width", "cars.tires.radiuses"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars.tires", paths: []string{"cars.tires.width", "cars.tires.radiuses"}},
			},
		},
		{
			name:   "tires.bolts.size + tires.width (common LCA cars.tires) — groupRunIdxLoop(cars.tires)",
			paths:  []string{"cars.tires.bolts.size", "cars.tires.width"},
			counts: single("cars.tires.bolts.size", "cars.tires.width"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars.tires", paths: []string{"cars.tires.bolts.size", "cars.tires.width"}},
			},
		},
		{
			name:  "multi-condition colors + single make — groupRunIdxLoop(cars)",
			paths: []string{"cars.colors", "cars.make"},
			counts: func() map[string][2]int {
				m := single("cars.make")
				m["cars.colors"] = [2]int{0, 2}
				return m
			}(),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.colors", "cars.make"}},
			},
		},

		// ---------------------------------------------------------------
		// Two paths — different first segments (separate groups)
		// ---------------------------------------------------------------
		{
			name:   "addresses.city + cars.make — two groupAndAll groups",
			paths:  []string{"addresses.city", "cars.make"},
			counts: single("addresses.city", "cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city"}},
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make"}},
			},
		},
		{
			name:   "owner.firstname + addresses.city — two groupAndAll groups",
			paths:  []string{"owner.firstname", "addresses.city"},
			counts: single("owner.firstname", "addresses.city"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "", paths: []string{"owner.firstname"}},
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city"}},
			},
		},

		// ---------------------------------------------------------------
		// Defensive edge cases — CANNOT occur in real filters.
		//
		// (1) Identical paths: resolveNestedCorrelated deduplicates paths into
		//     positionsByPath, so pathOrder never contains a path twice. The
		//     second occurrence of the same condition becomes an extra bitmap in
		//     independent[], not a separate path entry.
		//
		// (2) Ancestor paths like "cars" alongside "cars.make": value filters
		//     target leaf scalar/array properties only — you cannot filter an
		//     object[] property by value. IsNull filters on intermediate arrays
		//     go through fetchNestedIsNull, not resolveNestedCorrelated.
		//
		// Kept for defensive robustness: if a bug ever produces unexpected paths
		// the plan degrades gracefully rather than panicking.
		// ---------------------------------------------------------------
		{
			name:   "[defensive] identical paths (addresses.city twice) — cannot happen in production",
			paths:  []string{"addresses.city", "addresses.city"},
			counts: single("addresses.city", "addresses.city"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.city"}},
			},
		},
		{
			name:   "[defensive] ancestor + descendant (cars + cars.make) — cannot happen in production",
			paths:  []string{"cars", "cars.make"},
			counts: single("cars", "cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars", "cars.make"}},
			},
		},
		{
			name:   "[defensive] ancestor among multiple (cars + cars.tires.width + cars.accessories.type) — cannot happen in production",
			paths:  []string{"cars", "cars.tires.width", "cars.accessories.type"},
			counts: single("cars", "cars.tires.width", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars", "cars.tires.width", "cars.accessories.type"}},
			},
		},

		// ---------------------------------------------------------------
		// Scalar siblings in various containers
		// ---------------------------------------------------------------
		{
			name:   "scalar siblings in addresses[] (city + postcode)",
			paths:  []string{"addresses.city", "addresses.postcode"},
			counts: single("addresses.city", "addresses.postcode"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.postcode"}},
			},
		},
		{
			name:   "scalar siblings in owner object (firstname + lastname)",
			paths:  []string{"owner.firstname", "owner.lastname"},
			counts: single("owner.firstname", "owner.lastname"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "", paths: []string{"owner.firstname", "owner.lastname"}},
			},
		},
		{
			name:   "scalar vs text[] in owner object (firstname + nicknames)",
			paths:  []string{"owner.firstname", "owner.nicknames"},
			counts: single("owner.firstname", "owner.nicknames"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "", paths: []string{"owner.firstname", "owner.nicknames"}},
			},
		},
		{
			name:   "three scalars in addresses[] (city + postcode + numbers)",
			paths:  []string{"addresses.city", "addresses.postcode", "addresses.numbers"},
			counts: single("addresses.city", "addresses.postcode", "addresses.numbers"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.postcode", "addresses.numbers"}},
			},
		},
		{
			name:   "scalar + scalar-array in addresses[] (postcode + numbers)",
			paths:  []string{"addresses.postcode", "addresses.numbers"},
			counts: single("addresses.postcode", "addresses.numbers"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.postcode", "addresses.numbers"}},
			},
		},
		{
			name:   "tires.width + tires.radiuses + tires.caps.color (tires LCA, bolts is further)",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses"},
			counts: single("cars.tires.width", "cars.tires.radiuses"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "cars.tires", paths: []string{"cars.tires.width", "cars.tires.radiuses"}},
			},
		},

		// ---------------------------------------------------------------
		// Paths through sub-arrays within same group
		// ---------------------------------------------------------------
		{
			name:   "colors + accessories.type (no scalar sibling) — groupRunIdxLoop(cars)",
			paths:  []string{"cars.colors", "cars.accessories.type"},
			counts: single("cars.colors", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.colors", "cars.accessories.type"}},
			},
		},
		{
			name:   "tires.width + colors + accessories.type — groupRunIdxLoop(cars)",
			paths:  []string{"cars.tires.width", "cars.colors", "cars.accessories.type"},
			counts: single("cars.tires.width", "cars.colors", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.colors", "cars.accessories.type"}},
			},
		},
		{
			name:   "tires.width + tires.radiuses + colors + accessories.type — groupRunIdxLoop(cars)",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses", "cars.colors", "cars.accessories.type"},
			counts: single("cars.tires.width", "cars.tires.radiuses", "cars.colors", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.tires.radiuses", "cars.colors", "cars.accessories.type"}},
			},
		},
		{
			name:   "tires.width + tires.radiuses + accessories.type (LCA still cars)",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses", "cars.accessories.type"},
			counts: single("cars.tires.width", "cars.tires.radiuses", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.tires.radiuses", "cars.accessories.type"}},
			},
		},
		{
			name:   "tires.width + accessories.type + accessories.color — groupRunIdxLoop(cars)",
			paths:  []string{"cars.tires.width", "cars.accessories.type", "cars.accessories.color"},
			counts: single("cars.tires.width", "cars.accessories.type", "cars.accessories.color"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.accessories.type", "cars.accessories.color"}},
			},
		},

		// ---------------------------------------------------------------
		// Deep nesting: LCA within tires
		// ---------------------------------------------------------------
		{
			name:   "tires.bolts.size + tires.caps.color — groupRunIdxLoop(cars.tires)",
			paths:  []string{"cars.tires.bolts.size", "cars.tires.caps.color"},
			counts: single("cars.tires.bolts.size", "cars.tires.caps.color"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars.tires", paths: []string{"cars.tires.bolts.size", "cars.tires.caps.color"}},
			},
		},
		{
			name:   "tires.bolts.size + tires.caps.color + accessories.type — LCA collapses to cars",
			paths:  []string{"cars.tires.bolts.size", "cars.tires.caps.color", "cars.accessories.type"},
			counts: single("cars.tires.bolts.size", "cars.tires.caps.color", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.bolts.size", "cars.tires.caps.color", "cars.accessories.type"}},
			},
		},

		// ---------------------------------------------------------------
		// Three paths
		// ---------------------------------------------------------------
		{
			name:   "three paths in cars — single group",
			paths:  []string{"cars.make", "cars.tires.width", "cars.accessories.type"},
			counts: single("cars.make", "cars.tires.width", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.make", "cars.tires.width", "cars.accessories.type"}},
			},
		},

		// ---------------------------------------------------------------
		// Cross-subtree paths (different first segments → separate groups)
		// ---------------------------------------------------------------
		{
			name:   "owner.firstname + addresses.city + cars.make (three subtrees)",
			paths:  []string{"owner.firstname", "addresses.city", "cars.make"},
			counts: single("owner.firstname", "addresses.city", "cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "", paths: []string{"owner.firstname"}},
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city"}},
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make"}},
			},
		},
		{
			name:   "name + owner.firstname + addresses.city + cars.make (four subtrees)",
			paths:  []string{"name", "owner.firstname", "addresses.city", "cars.make"},
			counts: single("name", "owner.firstname", "addresses.city", "cars.make"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "", paths: []string{"name"}},
				{op: groupAndAll, lcaPath: "", paths: []string{"owner.firstname"}},
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city"}},
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make"}},
			},
		},
		{
			name:   "addresses.city + addresses.postcode + cars.make + cars.colors (two subtrees, two groups)",
			paths:  []string{"addresses.city", "addresses.postcode", "cars.make", "cars.colors"},
			counts: single("addresses.city", "addresses.postcode", "cars.make", "cars.colors"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.postcode"}},
				{op: groupAndAll, lcaPath: "cars", paths: []string{"cars.make", "cars.colors"}},
			},
		},
		{
			name:   "addresses.city + addresses.postcode + cars.tires.width + cars.accessories.type",
			paths:  []string{"addresses.city", "addresses.postcode", "cars.tires.width", "cars.accessories.type"},
			counts: single("addresses.city", "addresses.postcode", "cars.tires.width", "cars.accessories.type"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.postcode"}},
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.accessories.type"}},
			},
		},
		{
			name:   "addresses + tires.bolts.size + tires.caps.color (deep tires LCA)",
			paths:  []string{"addresses.city", "cars.tires.bolts.size", "cars.tires.caps.color"},
			counts: single("addresses.city", "cars.tires.bolts.size", "cars.tires.caps.color"),
			wantGroups: []conditionGroup{
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city"}},
				{op: groupRunIdxLoop, lcaPath: "cars.tires", paths: []string{"cars.tires.bolts.size", "cars.tires.caps.color"}},
			},
		},

		// ---------------------------------------------------------------
		// Input order preserved within each group
		// ---------------------------------------------------------------
		{
			name:   "interleaved paths — groups preserve insertion order",
			paths:  []string{"cars.tires.width", "addresses.city", "cars.accessories.type", "addresses.postcode"},
			counts: single("cars.tires.width", "addresses.city", "cars.accessories.type", "addresses.postcode"),
			wantGroups: []conditionGroup{
				{op: groupRunIdxLoop, lcaPath: "cars", paths: []string{"cars.tires.width", "cars.accessories.type"}},
				{op: groupAndAll, lcaPath: "addresses", paths: []string{"addresses.city", "addresses.postcode"}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := newExecutionPlanBuilder(props).build(tt.paths, tt.counts)
			require.NoError(t, err)
			require.Len(t, plan, len(tt.wantGroups), "number of groups")
			for i, want := range tt.wantGroups {
				got := plan[i]
				assert.Equal(t, want.op, got.op, "group %d op", i)
				assert.Equal(t, want.lcaPath, got.lcaPath, "group %d lcaPath", i)
				assert.Equal(t, want.paths, got.paths, "group %d paths", i)
			}
		})
	}
}

func TestLastIntermediateObjectArray(t *testing.T) {
	props := correlationTestProps()

	// Schema (from correlationTestProps, rooted at nestedObject: object):
	//   name:      text
	//   owner:     object  { firstname text; nicknames text[] }
	//   addresses: object[] { city text; numbers number[] }
	//   tags:      text[]
	//   cars:      object[] {
	//     make:        text
	//     colors:      text[]
	//     tires:       object[] { width int; radiuses int[]; bolts object[]{size int} }
	//     accessories: object[] { type text }
	//   }

	for _, tt := range []struct {
		path string
		want string
	}{
		// No ObjectArray anywhere in the path
		{"name", ""},
		{"tags", ""},            // text[] at root — no intermediate object[]
		{"owner.firstname", ""}, // owner is DataTypeObject (not array)
		{"owner.nicknames", ""}, // object → text[] — still no object[]

		// Path IS an ObjectArray segment itself
		{"addresses", "addresses"},
		{"cars", "cars"},

		// Path through a single ObjectArray
		{"addresses.city", "addresses"},    // scalar leaf
		{"addresses.numbers", "addresses"}, // scalar-array leaf
		{"cars.make", "cars"},              // scalar leaf
		{"cars.colors", "cars"},            // scalar-array leaf

		// Deeper: two ObjectArray levels — returns the deepest one
		{"cars.tires", "cars.tires"},                  // tires is itself object[]
		{"cars.tires.width", "cars.tires"},            // int leaf
		{"cars.tires.radiuses", "cars.tires"},         // int[] leaf
		{"cars.accessories.type", "cars.accessories"}, // scalar leaf

		// Three ObjectArray levels
		{"cars.tires.bolts.size", "cars.tires.bolts"},

		// Unknown segment — returns last found ObjectArray before the miss
		{"cars.unknown", "cars"},
		{"nonexistent", ""},
	} {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, newExecutionPlanBuilder(props).lastIntermediateObjectArray(tt.path))
		})
	}
}

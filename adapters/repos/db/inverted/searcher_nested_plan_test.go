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
	"strings"
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

func TestBuildResolutionPlanLeafOps(t *testing.T) {
	props := correlationTestProps()
	objDT := schema.DataTypeObject
	arrDT := schema.DataTypeObjectArray

	// These cases all verify the top-level op (and lcaPath where relevant).
	// Cross-subtree pairs produce an interior maskLeafAnd; same-subtree pairs
	// produce a leaf or an idxLoopAnd.  The comments show the logical plan.
	tests := []struct {
		name    string
		pathA   string
		pathB   string
		rootDT  schema.DataType
		wantOp  nestedCorrelation
		wantLCA string
	}{
		// ancestor / descendant
		// → directAnd [addresses.city]  (identical paths: deduplicated to one)
		{
			name:  "identical paths",
			pathA: "addresses.city", pathB: "addresses.city", rootDT: objDT,
			wantOp: directAnd,
		},
		// → directAnd [cars.make]  ("cars" ancestor excluded as superset)
		{
			name:  "one is prefix of other",
			pathA: "cars", pathB: "cars.make", rootDT: objDT,
			wantOp: directAnd,
		},

		// scalar siblings (inherit all parent-element positions) → directAnd
		// → directAnd [addresses.city, addresses.postcode]
		{
			name:  "addresses.city and addresses.postcode (scalar siblings in object[])",
			pathA: "addresses.city", pathB: "addresses.postcode", rootDT: objDT,
			wantOp: directAnd,
		},
		// → directAnd [owner.firstname, owner.lastname]
		{
			name:  "owner.firstname and owner.lastname (scalar siblings in object)",
			pathA: "owner.firstname", pathB: "owner.lastname", rootDT: objDT,
			wantOp: directAnd,
		},
		// → directAnd [cars.make, cars.colors]
		{
			name:  "cars.make and cars.colors (scalar siblings in cars object[])",
			pathA: "cars.make", pathB: "cars.colors", rootDT: objDT,
			wantOp: directAnd,
		},
		// → directAnd [cars.tires.width, cars.tires.radiuses]
		{
			name:  "tires.width and tires.radiuses (scalar siblings in tires object[])",
			pathA: "cars.tires.width", pathB: "cars.tires.radiuses", rootDT: objDT,
			wantOp: directAnd,
		},

		// different sub-arrays under intermediate object[] → idxLoopAnd
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width]
		//       └── directAnd [cars.colors]
		{
			name:   "cars.tires.width and cars.colors (tires is sub-array of cars)",
			pathA:  "cars.tires.width", pathB: "cars.colors", rootDT: objDT,
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// make is scalar at cars level → superset of tires.width positions → directAnd
		// → directAnd [cars.tires.width, cars.make]
		{
			name:  "cars.tires.width and cars.make (tires sub-array, make scalar superset)",
			pathA: "cars.tires.width", pathB: "cars.make", rootDT: objDT,
			wantOp: directAnd,
		},
		// → directAnd [cars.tires.radiuses, cars.tires.width]
		{
			name:  "cars.tires.radiuses and cars.tires.width (both scalar at tires level)",
			pathA: "cars.tires.radiuses", pathB: "cars.tires.width", rootDT: objDT,
			wantOp: directAnd,
		},

		// diverge at root of object[] → maskLeafAnd (root_idx aligns elements)
		// → maskLeafAnd
		//       ├── directAnd [addresses.city]
		//       └── directAnd [cars.make]
		{
			name:   "diverge at root of object[] property",
			pathA:  "addresses.city", pathB: "cars.make", rootDT: arrDT,
			wantOp: maskLeafAnd,
		},

		// diverge at root of single object → maskLeafAnd
		// → maskLeafAnd
		//       ├── directAnd [addresses.city]
		//       └── directAnd [cars.make]
		{
			name:   "addresses.city and cars.make under single object",
			pathA:  "addresses.city", pathB: "cars.make", rootDT: objDT,
			wantOp: maskLeafAnd,
		},
		// → maskLeafAnd
		//       ├── directAnd [owner.firstname]
		//       └── directAnd [addresses.city]
		{
			name:   "owner.firstname and addresses.city under single object",
			pathA:  "owner.firstname", pathB: "addresses.city", rootDT: objDT,
			wantOp: maskLeafAnd,
		},

		// firstname is scalar at owner level → superset of nicknames (text[]) → directAnd
		// → directAnd [owner.firstname, owner.nicknames]
		{
			name:  "owner.firstname and owner.nicknames (scalar vs text[])",
			pathA: "owner.firstname", pathB: "owner.nicknames", rootDT: objDT,
			wantOp: directAnd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use buildResolutionPlan with the pair; cross-subtree pairs produce a
			// maskLeafAnd interior node — check only the top-level op.
			plan, err := newResolutionPlanBuilder(tt.rootDT, props).build([]string{tt.pathA, tt.pathB})
			require.NoError(t, err)
			// For cross-subtree pairs the plan is an interior maskLeafAnd with groups;
			// for same-subtree pairs it is a leaf. Either way the top-level op matches.
			assert.Equal(t, tt.wantOp, plan.op)
			if tt.wantOp == idxLoopAnd {
				assert.Equal(t, tt.wantLCA, plan.lcaPath)
			}
		})
	}
}

func TestBuildResolutionPlanNoPaths(t *testing.T) {
	props := correlationTestProps()
	_, err := newResolutionPlanBuilder(schema.DataTypeObject, props).build(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no paths")
}

func TestBuildResolutionPlanInvalidDT(t *testing.T) {
	props := correlationTestProps()
	// cars.make is DataTypeText (non-nested); paths that go deeper share the
	// same first-segment group, reaching the LCA check with a scalar LCA node.
	_, err := newResolutionPlanBuilder(schema.DataTypeObject, props).
		build([]string{"cars.make.foo", "cars.make.bar"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object or object[]")
}

func TestBuildResolutionPlanMultiPath(t *testing.T) {
	props := correlationTestProps()
	objDT := schema.DataTypeObject

	tests := []struct {
		name    string
		paths   []string
		wantOp  nestedCorrelation
		wantLCA string
	}{
		// empty paths is a programming error → expect error, not a plan
		// → directAnd [addresses.city]
		{name: "single path", paths: []string{"addresses.city"}, wantOp: directAnd},

		// N=3: scalar-superset rule (3b) only applies for N=2 → idxLoopAnd
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.make]
		//       ├── directAnd [cars.colors]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.make + cars.colors + cars.accessories.type (N=3, scalar rule N=2 only)",
			paths:  []string{"cars.make", "cars.colors", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.make]
		//       ├── directAnd [cars.tires.width]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.make + cars.tires.width + cars.accessories.type (N=3)",
			paths:  []string{"cars.make", "cars.tires.width", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.colors]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.colors + cars.accessories.type (no scalar, accessories is sub-array)",
			paths:  []string{"cars.colors", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.tires.width + cars.accessories.type (two sub-arrays under cars)",
			paths:  []string{"cars.tires.width", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width]
		//       ├── directAnd [cars.colors]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.tires.width + cars.colors + cars.accessories.type (no scalar)",
			paths:  []string{"cars.tires.width", "cars.colors", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// tires.width and tires.radiuses are scalar siblings → merged into one group
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width, cars.tires.radiuses]
		//       ├── directAnd [cars.colors]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.tires.width + cars.tires.radiuses + cars.colors + cars.accessories.type",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses", "cars.colors", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},

		// tires.width is scalar at tires level → superset of radiuses → directAnd
		// → directAnd [cars.tires.width, cars.tires.radiuses]
		{
			name:   "cars.tires.width + cars.tires.radiuses (scalar at tires level)",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses"},
			wantOp: directAnd,
		},
		// tires pair is scalar-rescued; accessories diverges under cars → idxLoopAnd
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width, cars.tires.radiuses]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.tires.width + cars.tires.radiuses + cars.accessories.type (LCA still cars)",
			paths:  []string{"cars.tires.width", "cars.tires.radiuses", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},

		// city is scalar at addresses level → superset of postcode and numbers → directAnd
		// → directAnd [addresses.city, addresses.postcode, addresses.numbers]
		{
			name:   "addresses.city + addresses.postcode + addresses.numbers (city is scalar)",
			paths:  []string{"addresses.city", "addresses.postcode", "addresses.numbers"},
			wantOp: directAnd,
		},
		// → directAnd [addresses.postcode, addresses.numbers]
		{
			name:   "addresses.postcode + addresses.numbers (postcode is scalar)",
			paths:  []string{"addresses.postcode", "addresses.numbers"},
			wantOp: directAnd,
		},

		// "cars" ancestor excluded as superset; descendants need idxLoopAnd
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars + cars.tires.width + cars.accessories.type — ancestor excluded",
			paths:  []string{"cars", "cars.tires.width", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},
		// "cars" ancestor excluded; only cars.make remains → directAnd
		// → directAnd [cars.make]
		{
			name:   "cars + cars.make (ancestor path only)",
			paths:  []string{"cars", "cars.make"},
			wantOp: directAnd,
		},

		// accessories.type and accessories.color must be in the same accessories element,
		// not just the same car → rem sub-grouping puts them in one directAnd sub-plan
		// → idxLoopAnd("cars")
		//       ├── directAnd [cars.tires.width]
		//       └── directAnd [cars.accessories.type, cars.accessories.color]
		{
			name:   "cars.tires.width + cars.accessories.type + cars.accessories.color",
			paths:  []string{"cars.tires.width", "cars.accessories.type", "cars.accessories.color"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},

		// bolts and caps both live under cars.tires → LCA is "cars.tires" directly,
		// so a single (non-nested) idxLoopAnd at that combined path is produced
		// → idxLoopAnd("cars.tires")
		//       ├── directAnd [cars.tires.bolts.size]
		//       └── directAnd [cars.tires.caps.color]
		{
			name:   "cars.tires.bolts.size + cars.tires.caps.color — idxLoopAnd at cars.tires",
			paths:  []string{"cars.tires.bolts.size", "cars.tires.caps.color"},
			wantOp: idxLoopAnd, wantLCA: "cars.tires",
		},
		// adding accessories.type forces divergence at the cars level: tires and
		// accessories are different subtrees → LCA = "cars". The tires sub-group
		// itself diverges under tires (bolts vs caps) → nested idxLoopAnd("tires")
		// → idxLoopAnd("cars")
		//       ├── idxLoopAnd("tires")
		//       │       ├── directAnd [cars.tires.bolts.size]
		//       │       └── directAnd [cars.tires.caps.color]
		//       └── directAnd [cars.accessories.type]
		{
			name:   "cars.tires.bolts.size + cars.tires.caps.color + cars.accessories.type — nested idxLoopAnd",
			paths:  []string{"cars.tires.bolts.size", "cars.tires.caps.color", "cars.accessories.type"},
			wantOp: idxLoopAnd, wantLCA: "cars",
		},

		// cross-subtree at root → maskLeafAnd
		// → maskLeafAnd
		//       ├── directAnd [owner.firstname]
		//       ├── directAnd [addresses.city]
		//       └── directAnd [cars.make]
		{
			name:   "owner.firstname + addresses.city + cars.make (diverge at root)",
			paths:  []string{"owner.firstname", "addresses.city", "cars.make"},
			wantOp: maskLeafAnd,
		},
		// → maskLeafAnd
		//       ├── directAnd [name]
		//       ├── directAnd [owner.firstname]
		//       ├── directAnd [addresses.city]
		//       └── directAnd [cars.make]
		{
			name:   "name + owner.firstname + addresses.city + cars.make (four subtrees)",
			paths:  []string{"name", "owner.firstname", "addresses.city", "cars.make"},
			wantOp: maskLeafAnd,
		},
		// → maskLeafAnd
		//       ├── directAnd [addresses.city, addresses.postcode]
		//       └── directAnd [cars.make, cars.colors]
		{
			name:   "addresses.city + addresses.postcode + cars.make + cars.colors (mixed subtrees)",
			paths:  []string{"addresses.city", "addresses.postcode", "cars.make", "cars.colors"},
			wantOp: maskLeafAnd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := newResolutionPlanBuilder(objDT, props).build(tt.paths)
			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, plan.op)
			if tt.wantOp == idxLoopAnd {
				assert.Equal(t, tt.wantLCA, plan.lcaPath)
			}
		})
	}
}

func TestBuildResolutionPlan(t *testing.T) {
	props := correlationTestProps()
	objDT := schema.DataTypeObject

	tests := []struct {
		name  string
		paths []string
		check func(t *testing.T, plan *resolutionPlan)
	}{
		{
			// → directAnd [addresses.city]
			name:  "single path",
			paths: []string{"addresses.city"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.Equal(t, []string{"addresses.city"}, p.paths)
				assert.Nil(t, p.groups)
			},
		},
		{
			// → directAnd [addresses.city, addresses.postcode]
			name:  "scalar siblings in addresses → directAnd leaf",
			paths: []string{"addresses.city", "addresses.postcode"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, p.paths)
				assert.Nil(t, p.groups)
			},
		},
		{
			// → idxLoopAnd("cars")
			//       ├── directAnd [cars.tires.width]
			//       └── directAnd [cars.accessories.type]
			name:  "cars.tires.width + cars.accessories.type → idxLoopAnd on cars",
			paths: []string{"cars.tires.width", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
			},
		},
		{
			// → maskLeafAnd
			//       ├── directAnd [addresses.city, addresses.postcode]
			//       └── directAnd [cars.make, cars.colors]
			name:  "addresses.city + addresses.postcode + cars.make + cars.colors → maskLeafAnd with two directAnd groups",
			paths: []string{"addresses.city", "addresses.postcode", "cars.make", "cars.colors"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				require.Len(t, p.groups, 2)

				// find each group by paths content
				var addrGroup, carsGroup *resolutionPlan
				for _, g := range p.groups {
					for _, path := range g.paths {
						if path != "" && path[:4] == "addr" {
							addrGroup = g
						} else if path != "" && path[:4] == "cars" {
							carsGroup = g
						}
					}
				}
				require.NotNil(t, addrGroup)
				require.NotNil(t, carsGroup)
				assert.Equal(t, directAnd, addrGroup.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, addrGroup.paths)
				assert.Equal(t, directAnd, carsGroup.op)
				assert.ElementsMatch(t, []string{"cars.make", "cars.colors"}, carsGroup.paths)
			},
		},
		{
			// → maskLeafAnd
			//       ├── directAnd           [addresses.city, addresses.postcode]
			//       └── idxLoopAnd("cars")
			//               ├── directAnd   [cars.tires.width]
			//               └── directAnd   [cars.accessories.type]
			name:  "addresses.city + addresses.postcode + cars.tires.width + cars.accessories.type → maskLeafAnd: directAnd + idxLoopAnd",
			paths: []string{"addresses.city", "addresses.postcode", "cars.tires.width", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				require.Len(t, p.groups, 2)

				var addrGroup, carsGroup *resolutionPlan
				for _, g := range p.groups {
					if g.op == idxLoopAnd && g.lcaPath == "cars" {
						carsGroup = g
					} else if g.op == directAnd {
						addrGroup = g
					}
				}
				require.NotNil(t, addrGroup, "addresses group not found")
				require.NotNil(t, carsGroup, "cars group not found")
				assert.Equal(t, directAnd, addrGroup.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, addrGroup.paths)
				assert.Equal(t, idxLoopAnd, carsGroup.op)
				assert.Equal(t, "cars", carsGroup.lcaPath)
			},
		},
		{
			// → idxLoopAnd("cars")
			//       ├── directAnd [cars.tires.width]
			//       ├── directAnd [cars.colors]
			//       └── directAnd [cars.accessories.type]
			name:  "cars.tires.width + cars.colors + cars.accessories.type → idxLoopAnd on cars with groups",
			paths: []string{"cars.tires.width", "cars.colors", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
				// Three distinct rem first-segments → three sub-plans.
				require.Len(t, p.groups, 3)
				assert.Nil(t, p.paths)
				for _, g := range p.groups {
					assert.Equal(t, directAnd, g.op)
					assert.Len(t, g.paths, 1)
				}
			},
		},
		{
			// accessories.type and accessories.color must be in the same accessories
			// element — rem sub-grouping puts them in one directAnd plan.
			// Sub-plans carry the original full paths.
			// → idxLoopAnd("cars")
			//       ├── directAnd [cars.tires.width]
			//       └── directAnd [cars.accessories.type, cars.accessories.color]
			name:  "cars.tires.width + cars.accessories.type + cars.accessories.color → idxLoopAnd with two groups",
			paths: []string{"cars.tires.width", "cars.accessories.type", "cars.accessories.color"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
				require.Len(t, p.groups, 2)
				assert.Nil(t, p.paths)

				// Identify groups by their path contents (not first-segment cut,
				// since full paths all start with "cars.").
				var tiresGroup, accGroup *resolutionPlan
				for _, g := range p.groups {
					if assert.Equal(t, directAnd, g.op) {
						if len(g.paths) == 1 {
							tiresGroup = g
						} else {
							accGroup = g
						}
					}
				}
				require.NotNil(t, tiresGroup, "tires group missing")
				require.NotNil(t, accGroup, "accessories group missing")
				assert.ElementsMatch(t, []string{"cars.tires.width"}, tiresGroup.paths)
				assert.ElementsMatch(t, []string{"cars.accessories.type", "cars.accessories.color"}, accGroup.paths)
			},
		},
		{
			// Both paths share LCA "cars.tires" → single idxLoopAnd at that path.
			// No nesting: the loop iterates _idx.cars.tires[N] entries directly.
			// → idxLoopAnd("cars.tires")
			//       ├── directAnd [cars.tires.bolts.size]
			//       └── directAnd [cars.tires.caps.color]
			name:  "cars.tires.bolts.size + cars.tires.caps.color → idxLoopAnd at cars.tires",
			paths: []string{"cars.tires.bolts.size", "cars.tires.caps.color"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars.tires", p.lcaPath)
				require.Len(t, p.groups, 2)
				for _, g := range p.groups {
					assert.Equal(t, directAnd, g.op)
					assert.Len(t, g.paths, 1)
				}
			},
		},
		{
			// accessories.type forces divergence at the cars level; the tires sub-group
			// diverges further under tires (bolts vs caps) → nested idxLoopAnd("tires")
			// inside idxLoopAnd("cars"). Sub-plans carry original full paths.
			// → idxLoopAnd("cars")
			//       ├── idxLoopAnd("tires")
			//       │       ├── directAnd [cars.tires.bolts.size]
			//       │       └── directAnd [cars.tires.caps.color]
			//       └── directAnd [cars.accessories.type]
			name:  "cars.tires.bolts.size + cars.tires.caps.color + cars.accessories.type → nested idxLoopAnd",
			paths: []string{"cars.tires.bolts.size", "cars.tires.caps.color", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
				require.Len(t, p.groups, 2)

				var tiresLoop, accGroup *resolutionPlan
				for _, g := range p.groups {
					if g.op == idxLoopAnd {
						tiresLoop = g
					} else {
						accGroup = g
					}
				}
				require.NotNil(t, tiresLoop, "tires idxLoopAnd missing")
				require.NotNil(t, accGroup, "accessories directAnd missing")

				assert.Equal(t, "tires", tiresLoop.lcaPath)
				require.Len(t, tiresLoop.groups, 2)
				for _, g := range tiresLoop.groups {
					assert.Equal(t, directAnd, g.op)
				}
				assert.Equal(t, directAnd, accGroup.op)
				assert.ElementsMatch(t, []string{"cars.accessories.type"}, accGroup.paths)
			},
		},
		{
			// → directAnd [owner.firstname, owner.lastname]
			name:  "owner.firstname + owner.lastname → directAnd (scalar siblings in object)",
			paths: []string{"owner.firstname", "owner.lastname"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.ElementsMatch(t, []string{"owner.firstname", "owner.lastname"}, p.paths)
			},
		},
		{
			// → maskLeafAnd
			//       ├── directAnd [owner.firstname]
			//       └── directAnd [addresses.city]
			name:  "owner.firstname + addresses.city → maskLeafAnd with two groups",
			paths: []string{"owner.firstname", "addresses.city"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				assert.Len(t, p.groups, 2)
			},
		},
		{
			// Paths are interleaved but grouping is order-independent.
			// → maskLeafAnd
			//       ├── directAnd [cars.make, cars.colors]
			//       └── directAnd [addresses.city, addresses.postcode]
			name:  "interleaved cars and addresses paths → same grouping as sorted order",
			paths: []string{"cars.make", "addresses.city", "cars.colors", "addresses.postcode"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				require.Len(t, p.groups, 2)

				byFirst := map[string]*resolutionPlan{}
				for _, g := range p.groups {
					for _, path := range g.paths {
						first, _, _ := strings.Cut(path, ".")
						byFirst[first] = g
						break
					}
				}

				carsGroup := byFirst["cars"]
				addrGroup := byFirst["addresses"]
				require.NotNil(t, carsGroup, "cars group missing")
				require.NotNil(t, addrGroup, "addresses group missing")

				assert.Equal(t, directAnd, carsGroup.op)
				assert.ElementsMatch(t, []string{"cars.make", "cars.colors"}, carsGroup.paths)
				assert.Equal(t, directAnd, addrGroup.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, addrGroup.paths)
			},
		},
		{
			// Paths are interleaved but grouping is order-independent.
			// → maskLeafAnd
			//       ├── directAnd           [addresses.city, addresses.postcode]
			//       └── idxLoopAnd("cars")
			//               ├── directAnd   [cars.tires.width]
			//               └── directAnd   [cars.accessories.type]
			name:  "interleaved deep cars and addresses paths",
			paths: []string{"cars.tires.width", "addresses.city", "cars.accessories.type", "addresses.postcode"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				require.Len(t, p.groups, 2)

				// Identify groups by op+lcaPath rather than path prefix, since
				// sub-plans now carry LCA-stripped paths (e.g. "tires.width").
				var carsGroup, addrGroup *resolutionPlan
				for _, g := range p.groups {
					if g.op == idxLoopAnd && g.lcaPath == "cars" {
						carsGroup = g
					} else if g.op == directAnd {
						addrGroup = g
					}
				}
				require.NotNil(t, carsGroup, "cars group missing")
				require.NotNil(t, addrGroup, "addresses group missing")

				assert.Equal(t, idxLoopAnd, carsGroup.op)
				assert.Equal(t, "cars", carsGroup.lcaPath)
				assert.Equal(t, directAnd, addrGroup.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, addrGroup.paths)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := newResolutionPlanBuilder(objDT, props).build(tt.paths)
			require.NoError(t, err)
			tt.check(t, plan)
		})
	}
}

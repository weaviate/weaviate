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
//	    tires:       object[] { width int; radiuses int[] }
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

func TestNestedPathRelationship(t *testing.T) {
	props := correlationTestProps()
	objDT := schema.DataTypeObject
	arrDT := schema.DataTypeObjectArray

	tests := []struct {
		name     string
		pathA    string
		pathB    string
		rootDT   schema.DataType
		wantKind nestedCorrelation
		wantLCA  string
	}{
		// --- ancestor / descendant → directAnd ---
		{
			name:  "identical paths",
			pathA: "addresses.city", pathB: "addresses.city", rootDT: objDT,
			wantKind: directAnd,
		},
		{
			name:  "one is prefix of other",
			pathA: "cars", pathB: "cars.make", rootDT: objDT,
			wantKind: directAnd,
		},

		// --- scalar siblings at same array element → directAnd ---
		{
			name:  "addresses.city and addresses.postcode (scalar siblings in object[])",
			pathA: "addresses.city", pathB: "addresses.postcode", rootDT: objDT,
			wantKind: directAnd,
		},
		{
			name:  "owner.firstname and owner.lastname (scalar siblings in object)",
			pathA: "owner.firstname", pathB: "owner.lastname", rootDT: objDT,
			wantKind: directAnd,
		},
		{
			name:  "cars.make and cars.colors (scalar siblings in cars object[])",
			pathA: "cars.make", pathB: "cars.colors", rootDT: objDT,
			wantKind: directAnd,
		},
		{
			name:  "tires.width and tires.radiuses (scalar siblings in tires object[])",
			pathA: "cars.tires.width", pathB: "cars.tires.radiuses", rootDT: objDT,
			wantKind: directAnd,
		},

		// --- different subtrees in object[] → idxLoopAnd ---
		{
			name:  "cars.tires.width and cars.colors (tires is sub-array of cars)",
			pathA: "cars.tires.width", pathB: "cars.colors", rootDT: objDT,
			wantKind: idxLoopAnd, wantLCA: "cars",
		},
		{
			name:  "cars.tires.width and cars.make (tires sub-array, make scalar)",
			pathA: "cars.tires.width", pathB: "cars.make", rootDT: objDT,
			wantKind: directAnd, // make is scalar → inherits all car positions (superset)
		},
		{
			name:  "cars.tires.radiuses and cars.tires.width diverge at tires level",
			pathA: "cars.tires.radiuses", pathB: "cars.tires.width", rootDT: objDT,
			wantKind: directAnd, // both scalar at tires element level
		},

		// --- diverge at root of object[] → maskLeafAnd ---
		// root_idx already distinguishes root elements so MaskLeafPositions AND
		// is sufficient; no _idx metadata is emitted at root level.
		{
			name:  "diverge at root of object[] property",
			pathA: "addresses.city", pathB: "cars.make", rootDT: arrDT,
			wantKind: maskLeafAnd,
		},

		// --- different root sub-properties of single object → maskLeafAnd ---
		{
			name:  "addresses.city and cars.make under single object",
			pathA: "addresses.city", pathB: "cars.make", rootDT: objDT,
			wantKind: maskLeafAnd,
		},
		{
			name:  "owner.firstname and addresses.city under single object",
			pathA: "owner.firstname", pathB: "addresses.city", rootDT: objDT,
			wantKind: maskLeafAnd,
		},

		// --- owner (object) with one path having sub-array → directAnd
		//     (scalar firstname inherits all owner positions, superset of nicknames) ---
		{
			name:  "owner.firstname and owner.nicknames (scalar vs text[])",
			pathA: "owner.firstname", pathB: "owner.nicknames", rootDT: objDT,
			wantKind: directAnd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel, err := nestedPathsRelationship([]string{tt.pathA, tt.pathB}, tt.rootDT, props)
			require.NoError(t, err)
			assert.Equal(t, tt.wantKind, rel.kind)
			if tt.wantKind == idxLoopAnd {
				assert.Equal(t, tt.wantLCA, rel.lcaPath)
			}
		})
	}
}

func TestNestedPathRelationshipInvalidDT(t *testing.T) {
	props := correlationTestProps()
	_, err := nestedPathsRelationship([]string{"addresses.city", "cars.make"}, schema.DataTypeText, props)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object or object[]")
}

func TestNestedPathsRelationshipMulti(t *testing.T) {
	props := correlationTestProps()
	objDT := schema.DataTypeObject

	tests := []struct {
		name     string
		paths    []string
		wantKind nestedCorrelation
		wantLCA  string
	}{
		// --- degenerate cases ---
		{name: "empty paths", paths: []string{}, wantKind: directAnd},
		{name: "single path", paths: []string{"addresses.city"}, wantKind: directAnd},

		// --- cars.make + cars.colors + cars.accessories.type ---
		// The scalar superset rule only holds for N=2. With N=3, make is a superset
		// of each sibling, but colors ∩ accessories.type may be empty even when a
		// document satisfies all three in the same car (e.g. Tesla: make={l4..l9},
		// colors={l9}, accessories.type={l7} → direct AND gives {} incorrectly).
		// idxLoopAnd on cars is required.
		{
			name:     "cars.make + cars.colors + cars.accessories.type (scalar rule N=2 only)",
			paths:    []string{"cars.make", "cars.colors", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},
		{
			name:     "cars.make + cars.tires.width + cars.accessories.type (scalar rule N=2 only)",
			paths:    []string{"cars.make", "cars.tires.width", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},

		// --- without a scalar at cars level → idxLoopAnd on cars ---
		// colors is text[] (not a further object[]), accessories is object[]
		// → at least one sub-array present without a scalar rescue
		{
			name:     "cars.colors + cars.accessories.type (no scalar, accessories is sub-array)",
			paths:    []string{"cars.colors", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},
		// tires and accessories are both sub-arrays under cars
		{
			name:     "cars.tires.width + cars.accessories.type (two sub-arrays under cars)",
			paths:    []string{"cars.tires.width", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},
		// all three without make: tires, colors, accessories
		{
			name:     "cars.tires.width + cars.colors + cars.accessories.type (no scalar)",
			paths:    []string{"cars.tires.width", "cars.colors", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},
		// four paths at cars level, none scalar
		{
			name:     "cars.tires.width + cars.tires.radiuses + cars.colors + cars.accessories.type",
			paths:    []string{"cars.tires.width", "cars.tires.radiuses", "cars.colors", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},

		// --- scalar rescues within tires sub-array ---
		// tires.width is scalar at tires level → superset of radiuses → directAnd
		{
			name:     "cars.tires.width + cars.tires.radiuses (width is scalar at tires level)",
			paths:    []string{"cars.tires.width", "cars.tires.radiuses"},
			wantKind: directAnd,
		},
		// adding tires.width to a cross-cars pair still requires idxLoopAnd on cars
		{
			name:     "cars.tires.width + cars.tires.radiuses + cars.accessories.type (LCA still cars)",
			paths:    []string{"cars.tires.width", "cars.tires.radiuses", "cars.accessories.type"},
			wantKind: idxLoopAnd, wantLCA: "cars",
		},

		// --- addresses level ---
		// city is scalar → superset of postcode and numbers → directAnd
		{
			name:     "addresses.city + addresses.postcode + addresses.numbers (city is scalar)",
			paths:    []string{"addresses.city", "addresses.postcode", "addresses.numbers"},
			wantKind: directAnd,
		},
		// postcode is also scalar → directAnd even without city
		{
			name:     "addresses.postcode + addresses.numbers (postcode is scalar)",
			paths:    []string{"addresses.postcode", "addresses.numbers"},
			wantKind: directAnd,
		},

		// --- cross-subtree at root (maskLeafAnd) ---
		{
			name:     "owner.firstname + addresses.city + cars.make (diverge at root)",
			paths:    []string{"owner.firstname", "addresses.city", "cars.make"},
			wantKind: maskLeafAnd,
		},
		// "name" is a scalar at root level but scalar rule only applies for N=2;
		// addresses goes through an object[], so maskLeafAnd for the root.
		{
			name:     "name + owner.firstname + addresses.city + cars.make (four subtrees at root)",
			paths:    []string{"name", "owner.firstname", "addresses.city", "cars.make"},
			wantKind: maskLeafAnd,
		},
		// Paths from two different sub-trees (addresses and cars) diverge at the
		// root of a single object. LCA = root (DataTypeObject) → maskLeafAnd.
		// MaskLeafPositions AND aligns on root=1 + docID giving correct doc-level
		// results even though addresses and cars have disjoint leaf positions.
		{
			name:     "addresses.city + addresses.postcode + cars.make + cars.colors (mixed subtrees at root)",
			paths:    []string{"addresses.city", "addresses.postcode", "cars.make", "cars.colors"},
			wantKind: maskLeafAnd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel, err := nestedPathsRelationship(tt.paths, objDT, props)
			require.NoError(t, err)
			assert.Equal(t, tt.wantKind, rel.kind)
			if tt.wantKind == idxLoopAnd {
				assert.Equal(t, tt.wantLCA, rel.lcaPath)
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
			// directAnd [addresses.city]
			name:  "single path",
			paths: []string{"addresses.city"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.Equal(t, []string{"addresses.city"}, p.paths)
				assert.Nil(t, p.groups)
			},
		},
		{
			// directAnd [addresses.city, addresses.postcode]
			name:  "scalar siblings in addresses → directAnd leaf",
			paths: []string{"addresses.city", "addresses.postcode"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, p.paths)
				assert.Nil(t, p.groups)
			},
		},
		{
			// idxLoopAnd("cars") [cars.tires.width, cars.accessories.type]
			name:  "cars.tires.width + cars.accessories.type → idxLoopAnd on cars",
			paths: []string{"cars.tires.width", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
			},
		},
		{
			// maskLeafAnd
			//   ├── directAnd [addresses.city, addresses.postcode]
			//   └── directAnd [cars.make, cars.colors]
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
			// maskLeafAnd
			//   ├── directAnd          [addresses.city, addresses.postcode]
			//   └── idxLoopAnd("cars") [cars.tires.width, cars.accessories.type]
			name:  "addresses.city + addresses.postcode + cars.tires.width + cars.accessories.type → maskLeafAnd: directAnd + idxLoopAnd",
			paths: []string{"addresses.city", "addresses.postcode", "cars.tires.width", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				require.Len(t, p.groups, 2)

				var addrGroup, carsGroup *resolutionPlan
				for _, g := range p.groups {
					for _, path := range g.paths {
						if len(path) > 4 && path[:4] == "addr" {
							addrGroup = g
						} else if len(path) > 4 && path[:4] == "cars" {
							carsGroup = g
						}
					}
				}
				require.NotNil(t, addrGroup, "addresses group not found")
				require.NotNil(t, carsGroup, "cars group not found")
				assert.Equal(t, directAnd, addrGroup.op)
				assert.Equal(t, idxLoopAnd, carsGroup.op)
				assert.Equal(t, "cars", carsGroup.lcaPath)
			},
		},
		{
			// idxLoopAnd("cars") [cars.tires.width, cars.colors, cars.accessories.type]
			name:  "cars.tires.width + cars.colors + cars.accessories.type → idxLoopAnd on cars",
			paths: []string{"cars.tires.width", "cars.colors", "cars.accessories.type"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, idxLoopAnd, p.op)
				assert.Equal(t, "cars", p.lcaPath)
			},
		},
		{
			// directAnd [owner.firstname, owner.lastname]
			name:  "owner.firstname + owner.lastname → directAnd (scalar siblings in object)",
			paths: []string{"owner.firstname", "owner.lastname"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, directAnd, p.op)
				assert.ElementsMatch(t, []string{"owner.firstname", "owner.lastname"}, p.paths)
			},
		},
		{
			// maskLeafAnd
			//   ├── directAnd [owner.firstname]
			//   └── directAnd [addresses.city]
			name:  "owner.firstname + addresses.city → maskLeafAnd with two groups",
			paths: []string{"owner.firstname", "addresses.city"},
			check: func(t *testing.T, p *resolutionPlan) {
				assert.Equal(t, maskLeafAnd, p.op)
				assert.Len(t, p.groups, 2)
			},
		},
		{
			// maskLeafAnd                          (same as sorted-order variant)
			//   ├── directAnd [cars.make, cars.colors]
			//   └── directAnd [addresses.city, addresses.postcode]
			// Paths are interleaved — cars.make, addresses.city, cars.colors, addresses.postcode.
			// The plan must still produce the same two groups regardless of input order.
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
			// maskLeafAnd
			//   ├── idxLoopAnd("cars") [cars.tires.width, cars.accessories.type]
			//   └── directAnd          [addresses.city, addresses.postcode]
			// Paths are interleaved: cars.tires.width, addresses.city, cars.accessories.type, addresses.postcode.
			name:  "interleaved deep cars and addresses paths",
			paths: []string{"cars.tires.width", "addresses.city", "cars.accessories.type", "addresses.postcode"},
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

				assert.Equal(t, idxLoopAnd, carsGroup.op)
				assert.Equal(t, "cars", carsGroup.lcaPath)
				assert.Equal(t, directAnd, addrGroup.op)
				assert.ElementsMatch(t, []string{"addresses.city", "addresses.postcode"}, addrGroup.paths)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := buildResolutionPlan(tt.paths, objDT, props)
			require.NoError(t, err)
			tt.check(t, plan)
		})
	}
}

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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// nestedPvp builds a minimal nested leaf propValuePair.
func nestedPvp(prop, relPath string) *propValuePair {
	return &propValuePair{nested: nestedInfo{isNested: true, relPath: relPath}, prop: prop}
}

// compoundAndPvp builds a compound AND propValuePair as produced by multi-token
// text tokenization (childrenFromTokenization=true, isWithinRootSubtree=true).
// prop is derived from the first child, mirroring buildNestedTextFilterPair.
func compoundAndPvp(children ...*propValuePair) *propValuePair {
	var prop string
	if len(children) > 0 {
		prop = children[0].prop
	}
	return &propValuePair{operator: filters.OperatorAnd, children: children, nested: nestedInfo{isWithinRootSubtree: true, childrenFromTokenization: true}, prop: prop}
}

// userNestedAndPvp builds a compound AND propValuePair as produced by user
// filter construction (childrenFromTokenization=false). Used to model cases
// like AND(addresses.city, cars.make) where the AND is not from tokenization.
func userNestedAndPvp(children ...*propValuePair) *propValuePair {
	return &propValuePair{operator: filters.OperatorAnd, children: children}
}

// wantChild describes one expected element in the groupNestedByProp output.
type wantChild struct {
	// correlatedProp is non-empty when the output child should be an
	// isWithinRootSubtree=true AND node wrapping conditions for that prop.
	correlatedProp string
	// groupSize is the number of children inside the correlated group.
	// Only inspected when correlatedProp is non-empty.
	groupSize int
	// isPlain is true when the output child should be a plain (non-correlated)
	// node, passed through unchanged from the input.
	isPlain bool
}

func TestGroupNestedByProp(t *testing.T) {
	flatPvp := func() *propValuePair {
		return &propValuePair{prop: "name", operator: filters.OperatorEqual}
	}
	class := &models.Class{Class: "TestClass"}

	tests := []struct {
		name     string
		children []*propValuePair
		want     []wantChild
	}{
		// output: (empty — passthrough)
		{
			name:     "empty children",
			children: nil,
			want:     nil,
		},

		// output:
		// └── addresses.city
		// Single-child group is kept as a plain child with no wrapper.
		{
			name:     "single nested child — no wrapper",
			children: []*propValuePair{nestedPvp("addresses", "city")},
			want:     []wantChild{{isPlain: true}},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── postcode
		{
			name: "two nested children same prop — wrapped",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				nestedPvp("addresses", "postcode"),
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// └── correlated(cars)
		//     ├── make
		//     ├── tires.width
		//     └── accessories.type
		{
			name: "three nested children same prop",
			children: []*propValuePair{
				nestedPvp("cars", "make"),
				nestedPvp("cars", "tires.width"),
				nestedPvp("cars", "accessories.type"),
			},
			want: []wantChild{{correlatedProp: "cars", groupSize: 3}},
		},

		// output:
		// └── correlated(addresses)
		//     ├── AND(city, city)  ← tokenization compound, treated as one unit
		//     └── postcode
		{
			name: "compound AND (multi-token text) same prop",
			children: []*propValuePair{
				compoundAndPvp(nestedPvp("addresses", "city"), nestedPvp("addresses", "city")),
				nestedPvp("addresses", "postcode"),
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// └── correlated(cars)
		//     ├── make
		//     └── AND(tires.width, tires.width)  ← tokenization compound
		{
			name: "direct and compound children same prop",
			children: []*propValuePair{
				nestedPvp("cars", "make"),
				compoundAndPvp(nestedPvp("cars", "tires.width"), nestedPvp("cars", "tires.width")),
			},
			want: []wantChild{{correlatedProp: "cars", groupSize: 2}},
		},

		// output:
		// ├── addresses.city
		// └── cars.make
		// Each prop has only one condition — no wrapper created for either.
		{
			name: "two props — single-child groups, no wrappers",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "make"),
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},

		// output:
		// ├── correlated(cars)       ← first-seen order preserved
		// │   ├── tires.width
		// │   └── accessories.type
		// └── correlated(addresses)
		//     ├── city
		//     └── postcode
		{
			name: "four conditions across two props interleaved",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "accessories.type"),
				nestedPvp("addresses", "postcode"),
			},
			want: []wantChild{
				{correlatedProp: "cars", groupSize: 2},
				{correlatedProp: "addresses", groupSize: 2},
			},
		},

		// output:
		// ├── correlated(cars)
		// │   ├── tires.width
		// │   └── accessories.type
		// └── name  ← flat, emitted at its original position
		{
			name: "nested and flat property mixed",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				flatPvp(),
				nestedPvp("cars", "accessories.type"),
			},
			want: []wantChild{
				{correlatedProp: "cars", groupSize: 2},
				{isPlain: true},
			},
		},

		// output:
		// ├── correlated(cars)
		// │   ├── tires.width
		// │   └── accessories.type
		// ├── correlated(addresses)
		// │   ├── city
		// │   └── postcode
		// └── name  ← flat
		{
			name: "cars + addresses + flat mixed",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "accessories.type"),
				nestedPvp("addresses", "postcode"),
				flatPvp(),
			},
			want: []wantChild{
				{correlatedProp: "cars", groupSize: 2},
				{correlatedProp: "addresses", groupSize: 2},
				{isPlain: true},
			},
		},

		// output:
		// ├── addresses.city               ← single-child group, no wrapper
		// └── AND(addresses.postcode, name) ← flat: childrenFromTokenization=false
		{
			name: "compound AND with non-nested grandchild goes to flat",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(nestedPvp("addresses", "postcode"), flatPvp()),
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── OR(postcode)  ← OR-of-same-root folds into the subtree
		// Recursive nestedRootProp recognises the OR as addresses-rooted.
		{
			name: "OR with same-root child folds into subtree",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{nestedPvp("addresses", "postcode")},
				},
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── OR(postcode, country)  ← all same-root, folds in
		{
			name: "OR with multiple same-root children folds into subtree",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{
						nestedPvp("addresses", "postcode"),
						nestedPvp("addresses", "country"),
					},
				},
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// ├── addresses.city                  ← single-child group, no wrapper
		// └── OR(addresses.postcode, cars.x)  ← mixed-root OR stays opaque
		{
			name: "OR with mixed-root children stays opaque",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{
						nestedPvp("addresses", "postcode"),
						nestedPvp("cars", "make"),
					},
				},
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},

		// output:
		// ├── addresses.city               ← single-child group, no wrapper
		// └── OR(addresses.postcode, flat) ← OR with flat stays opaque
		{
			name: "OR with flat child stays opaque",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{
						nestedPvp("addresses", "postcode"),
						flatPvp(),
					},
				},
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── NOT(postcode)  ← NOT-of-same-root folds in
		{
			name: "NOT with same-root child folds into subtree",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorNot,
					children: []*propValuePair{nestedPvp("addresses", "postcode")},
				},
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// ├── addresses.city                              ← leaf singleton, no wrapper
		// └── correlated(cars)[NOT(cars.make)]            ← singleton NOT wraps under
		//                                                   singleton-NOT/OR wrapping so the planner
		//                                                   evaluates per-element NOT
		//                                                   at cars LCA
		{
			name: "NOT singleton with cross-root sibling wraps under singleton-NOT/OR wrapping",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorNot,
					children: []*propValuePair{nestedPvp("cars", "make")},
				},
			},
			want: []wantChild{
				{isPlain: true},
				{correlatedProp: "cars", groupSize: 1},
			},
		},

		// output:
		// ├── addresses.city                              ← leaf singleton, no wrapper
		// └── correlated(cars)[OR(cars.make, cars.year)]  ← singleton OR wraps under
		//                                                   singleton-NOT/OR wrapping so the planner
		//                                                   evaluates per-element OR
		//                                                   at cars LCA
		{
			name: "OR singleton with cross-root sibling wraps under singleton-NOT/OR wrapping",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{
						nestedPvp("cars", "make"),
						nestedPvp("cars", "year"),
					},
				},
			},
			want: []wantChild{
				{isPlain: true},
				{correlatedProp: "cars", groupSize: 1},
			},
		},

		// output:
		// ├── addresses.city  ← leaf singleton, no wrapper (no benefit)
		// └── cars.make       ← leaf singleton, no wrapper (no benefit)
		// Leaves share docID-level semantics with their wrapped form so
		// singleton-NOT/OR wrapping deliberately leaves them as-is.
		{
			name: "direct nested leaf singletons stay opaque even with cross-root sibling",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "make"),
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── AND(postcode, country)  ← AND-of-same-root folds in
		// Recognises a user-written AND of same-root leaves as a same-root
		// subtree (in contrast to the existing test for AND with mixed roots
		// or AND with flat children which stay opaque).
		{
			name: "AND with multiple same-root nested leaves folds into subtree",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(
					nestedPvp("addresses", "postcode"),
					nestedPvp("addresses", "country"),
				),
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// └── correlated(addresses)
		//     ├── city
		//     └── AND(postcode, OR(country, region))  ← deep nesting all same-root
		// Recursive analysis crosses multiple levels of operator nesting.
		{
			name: "deeply nested same-root operators fold into subtree",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(
					nestedPvp("addresses", "postcode"),
					&propValuePair{
						operator: filters.OperatorOr,
						children: []*propValuePair{
							nestedPvp("addresses", "country"),
							nestedPvp("addresses", "region"),
						},
					},
				),
			},
			want: []wantChild{{correlatedProp: "addresses", groupSize: 2}},
		},

		// output:
		// └── AND(addresses.city, cars.make) ← flat: childrenFromTokenization=false
		{
			name: "compound AND(addresses.city, cars.make) — goes to flat",
			children: []*propValuePair{
				userNestedAndPvp(
					nestedPvp("addresses", "city"),
					nestedPvp("cars", "make"),
				),
			},
			want: []wantChild{{isPlain: true}},
		},

		// output:
		// ├── addresses.city                      ← single-child group, no wrapper
		// └── AND(addresses.postcode, cars.make)  ← flat: childrenFromTokenization=false
		{
			name: "nested + user AND(addresses, cars) — no wrapper, user AND in flat",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(
					nestedPvp("addresses", "postcode"),
					nestedPvp("cars", "make"),
				),
			},
			want: []wantChild{
				{isPlain: true},
				{isPlain: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupNestedByProp(tt.children, class, filters.OperatorAnd)
			if tt.want == nil {
				// nil or empty input → passthrough (may be nil or empty slice)
				assert.Empty(t, result)
				return
			}
			require.Len(t, result, len(tt.want), "output length")
			for i, wc := range tt.want {
				child := result[i]
				if wc.correlatedProp != "" {
					assert.True(t, child.nested.isWithinRootSubtree, "child[%d] should be correlated", i)
					assert.Equal(t, filters.OperatorAnd, child.operator, "child[%d] operator", i)
					assert.Equal(t, wc.correlatedProp, child.prop, "child[%d] prop", i)
					assert.Len(t, child.children, wc.groupSize, "child[%d] group size", i)
				} else {
					assert.False(t, child.nested.isWithinRootSubtree, "child[%d] should not be correlated", i)
				}
			}
		})
	}
}

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
)

// nestedPvp builds a minimal nested leaf propValuePair.
func nestedPvp(prop, relPath string) *propValuePair {
	return &propValuePair{isNested: true, prop: prop, nestedRelPath: relPath}
}

// compoundAndPvp builds a compound AND propValuePair as produced by multi-token
// text tokenization (childrenFromTokenization=true).
func compoundAndPvp(children ...*propValuePair) *propValuePair {
	return &propValuePair{operator: filters.OperatorAnd, children: children, childrenFromTokenization: true}
}

// userNestedAndPvp builds a compound AND propValuePair as produced by user
// filter construction (childrenFromTokenization=false). Used to model cases
// like AND(addresses.city, cars.make) where the AND is not from tokenization.
func userNestedAndPvp(children ...*propValuePair) *propValuePair {
	return &propValuePair{operator: filters.OperatorAnd, children: children}
}

func TestNestedCorrelatedGroup(t *testing.T) {
	flatPvp := func() *propValuePair {
		return &propValuePair{prop: "name", operator: filters.OperatorEqual}
	}

	tests := []struct {
		name          string
		children      []*propValuePair
		wantNilGroups bool           // groups == nil (unresolvable)
		wantGroups    map[string]int // expected group sizes by prop name
		wantOthers    int            // expected number of flat/other conditions
	}{
		// --- all nested, single prop group ---
		{
			name:       "single nested child",
			children:   []*propValuePair{nestedPvp("addresses", "city")},
			wantGroups: map[string]int{"addresses": 1},
		},
		{
			name: "two nested children same prop",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				nestedPvp("addresses", "postcode"),
			},
			wantGroups: map[string]int{"addresses": 2},
		},
		{
			name: "three nested children same prop",
			children: []*propValuePair{
				nestedPvp("cars", "make"),
				nestedPvp("cars", "tires.width"),
				nestedPvp("cars", "accessories.type"),
			},
			wantGroups: map[string]int{"cars": 3},
		},
		{
			// Compound AND from multi-token text is kept as a single unit.
			name: "compound AND (multi-token text) same prop",
			children: []*propValuePair{
				compoundAndPvp(nestedPvp("addresses", "city"), nestedPvp("addresses", "city")),
				nestedPvp("addresses", "postcode"),
			},
			wantGroups: map[string]int{"addresses": 2}, // 1 compound + 1 direct
		},
		{
			name: "direct and compound children same prop",
			children: []*propValuePair{
				nestedPvp("cars", "make"),
				compoundAndPvp(nestedPvp("cars", "tires.width"), nestedPvp("cars", "tires.width")),
			},
			wantGroups: map[string]int{"cars": 2},
		},

		// --- multiple prop groups, all nested ---
		{
			// Conditions spanning two props produce two separate groups, each
			// resolved with its own same-element semantics.
			name: "two props — two groups",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "make"),
			},
			wantGroups: map[string]int{"addresses": 1, "cars": 1},
		},
		{
			// Interleaved multi-prop conditions are grouped correctly.
			name: "four conditions across two props interleaved",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "accessories.type"),
				nestedPvp("addresses", "postcode"),
			},
			wantGroups: map[string]int{"cars": 2, "addresses": 2},
		},

		// --- mixed nested + flat: groups returned, flat conditions go into remaining ---
		{
			// Flat property alongside nested conditions: nested groups still get
			// same-element semantics; the flat condition goes into remaining.
			name: "nested and flat property mixed",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				flatPvp(),
				nestedPvp("cars", "accessories.type"),
			},
			wantGroups: map[string]int{"cars": 2},
			wantOthers: 1,
		},
		{
			// Full mixed: two nested groups + one flat property.
			// cars and addresses each get correlated resolution; name is resolved
			// normally and AND'd with the correlated results.
			name: "cars + addresses + flat mixed",
			children: []*propValuePair{
				nestedPvp("cars", "tires.width"),
				nestedPvp("addresses", "city"),
				nestedPvp("cars", "accessories.type"),
				nestedPvp("addresses", "postcode"),
				flatPvp(),
			},
			wantGroups: map[string]int{"cars": 2, "addresses": 2},
			wantOthers: 1,
		},
		{
			// Compound AND with a non-nested grandchild is treated as a flat
			// condition (goes to remaining, not into a nested group).
			name: "compound AND with non-nested grandchild goes to remaining",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(nestedPvp("addresses", "postcode"), flatPvp()),
			},
			wantGroups: map[string]int{"addresses": 1},
			wantOthers: 1,
		},
		{
			// OR compound child: not a valid correlated condition, goes to remaining.
			name: "non-AND compound child goes to remaining",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				{
					operator: filters.OperatorOr,
					children: []*propValuePair{nestedPvp("addresses", "postcode")},
				},
			},
			wantGroups: map[string]int{"addresses": 1},
			wantOthers: 1,
		},

		// --- nil groups (truly unresolvable) ---
		{
			name:          "empty children",
			wantNilGroups: true,
		},

		// --- compound AND with mixed props → goes to remaining, NOT unresolvable ---
		// AND(addresses.city, cars.make) is a perfectly valid user filter that
		// means "doc has a matching address city AND a matching car make".
		// When nested inside another AND as a compound AND child, it is treated
		// as an opaque condition in remaining: resolveDocIDsAndOr recurses into it
		// and applies correlated resolution on its children independently.
		{
			// A user-constructed AND(addresses.city, cars.make) nested as a
			// compound AND child: resolves correctly via the remaining path.
			name: "compound AND(addresses.city, cars.make) — goes to remaining",
			children: []*propValuePair{
				userNestedAndPvp(
					nestedPvp("addresses", "city"),
					nestedPvp("cars", "make"),
				),
			},
			// groups is nil because no direct nested children exist at this level,
			// but the user AND goes to remaining, not causing unresolvability.
			wantNilGroups: true,
		},
		{
			// Valid nested alongside user AND with mixed props: the user AND
			// goes to remaining, the direct nested child forms a group.
			name: "nested + user AND(addresses, cars) — nested groups, user AND in remaining",
			children: []*propValuePair{
				nestedPvp("addresses", "city"),
				userNestedAndPvp(
					nestedPvp("addresses", "postcode"),
					nestedPvp("cars", "make"),
				),
			},
			wantGroups: map[string]int{"addresses": 1},
			wantOthers: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pv := &propValuePair{operator: filters.OperatorAnd, children: tt.children}
			groups := pv.extractNestedCorrelatedGroups()
			if tt.wantNilGroups {
				assert.Nil(t, groups)
				return
			}
			require.NotNil(t, groups)
			assert.Equal(t, len(tt.wantGroups), len(groups), "number of groups")
			for prop, wantSize := range tt.wantGroups {
				assert.Len(t, groups[prop], wantSize, "group size for prop %q", prop)
			}
			// pv.children now holds only the flat conditions.
			assert.Len(t, pv.children, tt.wantOthers, "remaining (flat) children count")
		})
	}
}

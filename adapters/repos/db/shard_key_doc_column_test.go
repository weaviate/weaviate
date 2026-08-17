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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestKeyDocColumnConfigured covers the translation from what an operator wrote
// to whether a given property of a given shard gets a resident index.
//
// Every answer here costs memory for the life of the process, so the ones that
// matter are the negatives: a collection that was never named, a property that
// was not listed, and a collection named with no properties at all — which asks
// for none, matching the reindex variable that shares this configuration's
// parser. "*" is how every property is asked for.
func TestKeyDocColumnConfigured(t *testing.T) {
	tests := []struct {
		name       string
		configured map[string][]string
		class      string
		prop       string
		want       bool
	}{
		{
			name:  "nothing configured",
			class: "Foo", prop: "prop1",
		},
		{
			name:       "the property is listed",
			configured: map[string][]string{"Foo": {"prop1", "prop2"}},
			class:      "Foo", prop: "prop2", want: true,
		},
		{
			name:       "the property is not listed",
			configured: map[string][]string{"Foo": {"prop1"}},
			class:      "Foo", prop: "other",
		},
		{
			name:       "another collection is listed",
			configured: map[string][]string{"Bar": {"prop1"}},
			class:      "Foo", prop: "prop1",
		},
		{
			name:       "every property, by wildcard",
			configured: map[string][]string{"Foo": {"*"}},
			class:      "Foo", prop: "anything", want: true,
		},
		{
			// the environment refuses this shape outright, so the lookup should
			// never meet it — but it must not guess "all" if something else ever
			// builds the configuration
			name:       "a collection with no properties enables none",
			configured: map[string][]string{"Foo": nil},
			class:      "Foo", prop: "prop1",
		},
		{
			// only the first letter of a collection name is canonicalised, so a
			// mis-cased name parses and then matches nothing
			name:       "a mis-cased collection matches nothing",
			configured: map[string][]string{"FooBar": {"prop1"}},
			class:      "Foobar", prop: "prop1",
		},
		{
			// the schema holds "sKU"; configuring "SKU" has to reach it, since a
			// filter written the same way does
			name:       "a property whose first letter was capitalised",
			configured: map[string][]string{"Foo": {"SKU"}},
			class:      "Foo", prop: "sKU", want: true,
		},
		{
			name:       "and a later letter's case still has to match",
			configured: map[string][]string{"Foo": {"sku"}},
			class:      "Foo", prop: "sKU",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Shard{index: &Index{Config: IndexConfig{
				ClassName:                   schema.ClassName(tt.class),
				IndexFilterableKeyDocColumn: tt.configured,
			}}}
			assert.Equal(t, tt.want, s.keyDocColumnConfigured(tt.prop))
		})
	}
}

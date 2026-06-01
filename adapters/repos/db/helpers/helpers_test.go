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

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
)

func TestBucketNestedFromPropNameLSM(t *testing.T) {
	t.Run("basic property name", func(t *testing.T) {
		assert.Equal(t, "property.nested_foo", BucketNestedFromPropNameLSM("foo"))
	})

	t.Run("does not collide with regular property bucket", func(t *testing.T) {
		assert.NotEqual(t, BucketFromPropNameLSM("foo"), BucketNestedFromPropNameLSM("foo"))
	})
}

func TestBucketNestedMetaFromPropNameLSM(t *testing.T) {
	t.Run("basic property name", func(t *testing.T) {
		assert.Equal(t, "property.nestedmeta_foo", BucketNestedMetaFromPropNameLSM("foo"))
	})

	t.Run("does not collide with nested value bucket", func(t *testing.T) {
		assert.NotEqual(t, BucketNestedFromPropNameLSM("foo"), BucketNestedMetaFromPropNameLSM("foo"))
	})
}

func TestBucketGenerationAware(t *testing.T) {
	// Cover every helper variant against every meaningful generation value.
	// The contract: gen==0 returns the legacy unsuffixed name (so clusters
	// that never ran a semantic reindex find their existing buckets on
	// disk unchanged); gen>=1 appends a "__gen<N>" suffix so old and new
	// generations can coexist on disk during a migration.
	type variant struct {
		legacyName  func(string) string
		atGenName   func(string, int64) string
		propertyVar func(*models.Property) string
	}
	variants := map[string]variant{
		"filterable": {
			legacyName:  BucketFromPropNameLSM,
			atGenName:   BucketFromPropNameLSMAtGen,
			propertyVar: BucketFromPropertyLSM,
		},
		"searchable": {
			legacyName:  BucketSearchableFromPropNameLSM,
			atGenName:   BucketSearchableFromPropNameLSMAtGen,
			propertyVar: BucketSearchableFromPropertyLSM,
		},
		"rangeable": {
			legacyName:  BucketRangeableFromPropNameLSM,
			atGenName:   BucketRangeableFromPropNameLSMAtGen,
			propertyVar: BucketRangeableFromPropertyLSM,
		},
	}

	for label, v := range variants {
		t.Run(label+"/gen=0 returns legacy unsuffixed name", func(t *testing.T) {
			legacy := v.legacyName("foo")
			assert.Equal(t, legacy, v.atGenName("foo", 0),
				"gen=0 must equal legacy name for back-compat with existing on-disk buckets")
			assert.Equal(t, legacy, v.propertyVar(&models.Property{Name: "foo", BucketGeneration: 0}),
				"property-form with gen=0 must equal legacy name")
		})

		t.Run(label+"/gen=0 also covers negative values defensively", func(t *testing.T) {
			// BucketGeneration is int64; a corrupted or unexpected negative
			// value must not produce a "__gen-1" path on disk.
			legacy := v.legacyName("foo")
			assert.Equal(t, legacy, v.atGenName("foo", -1))
		})

		t.Run(label+"/gen>=1 appends __gen<N>", func(t *testing.T) {
			legacy := v.legacyName("foo")
			for _, gen := range []int64{1, 2, 7, 42} {
				expected := legacy + "__gen" + itoa(gen)
				assert.Equal(t, expected, v.atGenName("foo", gen))
				assert.Equal(t, expected, v.propertyVar(&models.Property{Name: "foo", BucketGeneration: gen}))
			}
		})

		t.Run(label+"/different generations do not collide", func(t *testing.T) {
			seen := map[string]int64{}
			for _, gen := range []int64{0, 1, 2, 3, 10} {
				name := v.atGenName("foo", gen)
				if prev, dup := seen[name]; dup {
					t.Fatalf("gen=%d produced same bucket name %q as gen=%d", gen, name, prev)
				}
				seen[name] = gen
			}
		})

		t.Run(label+"/across variants, same gen+propname do not collide", func(t *testing.T) {
			// Sanity check: filterable / searchable / rangeable buckets stay
			// distinct at every generation (their distinctness at gen=0 is
			// covered by the legacy tests above).
			assert.NotEqual(t,
				BucketFromPropNameLSMAtGen("foo", 3),
				BucketSearchableFromPropNameLSMAtGen("foo", 3))
			assert.NotEqual(t,
				BucketSearchableFromPropNameLSMAtGen("foo", 3),
				BucketRangeableFromPropNameLSMAtGen("foo", 3))
			assert.NotEqual(t,
				BucketFromPropNameLSMAtGen("foo", 3),
				BucketRangeableFromPropNameLSMAtGen("foo", 3))
		})
	}
}

// itoa avoids pulling strconv into the test file just to format an int64
// for assertion strings.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var s string
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

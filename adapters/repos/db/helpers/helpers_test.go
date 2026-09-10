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

// The ForID functions are the single source of physical names, keyed on the
// physical index ID. These tables ARE the on-disk format: every expected
// string below matches what shipped versions already wrote to disk, so a
// failing case here means data loss on upgrade, not a naming taste issue.
func TestPhysicalNamesForID(t *testing.T) {
	tests := []struct {
		id         string
		suffix     string
		raw        string
		compressed string
		flatMeta   string
	}{
		// legacy unnamed vector: ID "main", but raw bucket "vectors" —
		// the historical asymmetry that must never change
		{"main", "", "vectors", "vectors_compressed", "meta.db"},
		{"vectors_title", "title", "vectors_title", "vectors_compressed_title", "meta_title.db"},
		{"vectors_de_DE", "de_DE", "vectors_de_DE", "vectors_compressed_de_DE", "meta_de_DE.db"},
		// hfresh centroid hnsw for a named vector: suffix keeps the
		// "_centroids" tail, exactly what the old strip-based derivation gave
		{"vectors_title_centroids", "title_centroids", "vectors_title_centroids", "vectors_compressed_title_centroids", "meta_title_centroids.db"},
		// IDs outside the vectors_/main scheme (geo."prop",
		// "main_centroids"): suffix "" — mirrors the old CutPrefix fallback
		{"geo.location", "", "vectors", "vectors_compressed", "meta.db"},
		{"main_centroids", "", "vectors", "vectors_compressed", "meta.db"},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			assert.Equal(t, tc.suffix, PhysicalIDSuffix(tc.id))
			assert.Equal(t, tc.raw, VectorsBucketNameForID(tc.id))
			assert.Equal(t, tc.compressed, CompressedBucketNameForID(tc.id))
			assert.Equal(t, tc.flatMeta, FlatMetadataFileNameForID(tc.id))
		})
	}
}

// TestCentroidsID pins the hfresh centroid graph's ID for both shipped
// parent shapes; #12923's layout pin checks the files it names on disk.
func TestCentroidsID(t *testing.T) {
	tests := []struct {
		id   string
		want string
	}{
		{"main", "main_centroids"},
		{"vectors_title", "vectors_title_centroids"},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			assert.Equal(t, tc.want, CentroidsID(tc.id))
		})
	}
}

// The ForID functions and the legacy targetVector-based helpers must agree
// for every canonical pairing — the drop path still derives from schema
// names while the live index derives from its ID.
func TestPhysicalNamesForIDMatchTargetVectorHelpers(t *testing.T) {
	for _, tv := range []string{"", "title", "de_DE"} {
		id := VectorIndexIDForTarget(tv)
		assert.Equal(t, GetVectorsBucketName(tv), VectorsBucketNameForID(id), "raw bucket, tv=%q", tv)
		assert.Equal(t, GetCompressedBucketName(tv), CompressedBucketNameForID(id), "compressed bucket, tv=%q", tv)
		assert.Equal(t, FlatMetadataFileName(tv), FlatMetadataFileNameForID(id), "flat metadata, tv=%q", tv)
	}
	assert.Equal(t, "main", VectorIndexIDForTarget(""))
	assert.Equal(t, "vectors_title", VectorIndexIDForTarget("title"))
}

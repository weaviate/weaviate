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
)

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

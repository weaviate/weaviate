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

package flat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// FlatMetadataFileNameForID is keyed on the physical index ID, same as the
// other ForID helpers in package helpers (see
// helpers.TestPhysicalNamesForID). These strings ARE the on-disk format:
// every expected string below matches what shipped versions already wrote
// to disk, so a failing case here means data loss on upgrade, not a naming
// taste issue.
func TestFlatMetadataFileNameForID(t *testing.T) {
	tests := []struct {
		id       string
		flatMeta string
	}{
		// legacy unnamed vector: ID "main", but metadata file "meta.db" —
		// the historical asymmetry that must never change
		{"main", "meta.db"},
		{"vectors_title", "meta_title.db"},
		{"vectors_de_DE", "meta_de_DE.db"},
		// hfresh centroid hnsw for a named vector: suffix keeps the
		// "_centroids" tail, exactly what the old strip-based derivation gave
		{"vectors_title_centroids", "meta_title_centroids.db"},
		// IDs outside the vectors_/main scheme (geo."prop",
		// "main_centroids"): suffix "" — mirrors the old CutPrefix fallback
		{"geo.location", "meta.db"},
		{"main_centroids", "meta.db"},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			assert.Equal(t, tc.flatMeta, FlatMetadataFileNameForID(tc.id))
		})
	}
}

// FlatMetadataFileName and FlatMetadataFileNameForID must agree for every
// canonical pairing — the drop path still derives from schema names while
// the live index derives from its ID.
func TestFlatMetadataFileNameAgreesWithForID(t *testing.T) {
	for _, tv := range []string{"", "title", "de_DE"} {
		id := helpers.VectorIndexIDForTarget(tv)
		assert.Equal(t, FlatMetadataFileName(tv), FlatMetadataFileNameForID(id), "flat metadata, tv=%q", tv)
	}
}

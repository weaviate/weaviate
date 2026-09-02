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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// TestCompressedBucketFromPhysicalID pins that the compressed bucket derives
// from the index's physical ID, not from the logical target vector: an index
// whose ID and TargetVector disagree must place compressed vectors according
// to the ID. Before this change the name was reverse-engineered by stripping
// "vectors_" off the ID, which happened to agree; this pins the direction.
//
// "vectors_tv_centroids" is the shape of an hfresh centroid graph's physical
// ID (TargetVector "tv"'s ID with a "_centroids" suffix appended) — it still
// carries the "vectors_" prefix, so it derives a suffixed bucket name despite
// the centroid graph's own TargetVector being "". "geo.prop" is a geo index's
// physical ID (dotted property name, no "vectors_" prefix): PhysicalIDSuffix's
// fallback maps it to "", the bare legacy bucket name, matching the geo
// index's own empty TargetVector.
func TestCompressedBucketFromPhysicalID(t *testing.T) {
	tests := []struct {
		name           string
		physicalID     string
		wantCompressed string
	}{
		{
			name:           "named vector",
			physicalID:     "vectors_title",
			wantCompressed: "vectors_compressed_title",
		},
		{
			name:           "legacy vector",
			physicalID:     "main",
			wantCompressed: "vectors_compressed",
		},
		{
			name:           "hfresh centroid graph",
			physicalID:     "vectors_tv_centroids",
			wantCompressed: "vectors_compressed_tv_centroids",
		},
		{
			name:           "geo index",
			physicalID:     "geo.prop",
			wantCompressed: "vectors_compressed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantCompressed, helpers.CompressedBucketNameForID(tt.physicalID))
		})
	}
}

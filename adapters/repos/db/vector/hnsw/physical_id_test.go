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
func TestCompressedBucketFromPhysicalID(t *testing.T) {
	assert.Equal(t, "vectors_compressed_title", helpers.CompressedBucketNameForID("vectors_title"))
	assert.Equal(t, "vectors_compressed", helpers.CompressedBucketNameForID("main"))
}

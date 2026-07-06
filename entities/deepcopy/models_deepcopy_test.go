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

package deepcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

// BucketGeneration is the RAFT-replicated reindex-generation counter
// (weaviate/weaviate#11689); a deep copy must preserve it or migrations
// relying on the copy resolve the wrong LSM bucket.
func TestProp_PreservesBucketGeneration(t *testing.T) {
	original := &models.Property{
		Name:             "prop",
		BucketGeneration: 7,
	}

	cp := Prop(original)

	assert.Equal(t, int64(7), cp.BucketGeneration)

	// mutating the original must not affect the copy
	original.BucketGeneration = 99
	assert.Equal(t, int64(7), cp.BucketGeneration)
}

func TestClass_PreservesPropertyBucketGeneration(t *testing.T) {
	original := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{Name: "prop", BucketGeneration: 3},
		},
	}

	cp := Class(original)

	assert.Equal(t, int64(3), cp.Properties[0].BucketGeneration)
}

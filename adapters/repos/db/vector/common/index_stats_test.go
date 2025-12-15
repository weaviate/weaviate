//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDynamicIndexDetection(t *testing.T) {
	// Test the core logic for detecting dynamic indexes
	t.Run("test dynamic index type detection", func(t *testing.T) {
		// Test that dynamic indexes are correctly identified
		indexType := IndexTypeDynamic
		isDynamic := IsDynamic(IndexType(indexType))

		assert.True(t, isDynamic, "Dynamic index should be detected as dynamic")
	})

	t.Run("test non-dynamic index type detection", func(t *testing.T) {
		// Test that regular indexes are not detected as dynamic
		indexType := IndexTypeHNSW
		isDynamic := IsDynamic(IndexType(indexType))

		assert.False(t, isDynamic, "HNSW index should not be detected as dynamic")
	})

	t.Run("test flat index type detection", func(t *testing.T) {
		// Test that flat indexes are not detected as dynamic
		indexType := IndexTypeFlat
		isDynamic := IsDynamic(IndexType(indexType))

		assert.False(t, isDynamic, "Flat index should not be detected as dynamic")
	})

	t.Run("test empty index type", func(t *testing.T) {
		// Test that empty index type is not detected as dynamic
		indexType := IndexTypeNoop
		isDynamic := IsDynamic(IndexType(indexType))

		assert.False(t, isDynamic, "Empty index type should not be detected as dynamic")
	})
}

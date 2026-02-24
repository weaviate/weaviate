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

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func TestSegmentExtraInfo(t *testing.T) {
	strategy := segmentindex.StrategyReplace

	t.Run("no secondary indices, no tombstones", func(t *testing.T) {
		result := segmentExtraInfo(0, strategy, 0, false)
		assert.Equal(t, ".l0.s0", result)
		assert.NotContains(t, result, ".d1")
	})

	t.Run("no secondary indices, has tombstones", func(t *testing.T) {
		// Even with hasSecondaryTombstones=true, the marker is not written
		// when the bucket has no secondary indices.
		result := segmentExtraInfo(0, strategy, 0, true)
		assert.Equal(t, ".l0.s0", result)
		assert.NotContains(t, result, ".d1")
	})

	t.Run("has secondary indices, no tombstones", func(t *testing.T) {
		result := segmentExtraInfo(0, strategy, 1, false)
		assert.Equal(t, ".l0.s0", result)
		assert.NotContains(t, result, ".d1")
	})

	t.Run("has secondary indices, has tombstones", func(t *testing.T) {
		result := segmentExtraInfo(0, strategy, 1, true)
		assert.Equal(t, ".l0.s0.d1", result)
		assert.Contains(t, result, ".d1")
	})

	t.Run("level and strategy are encoded", func(t *testing.T) {
		result := segmentExtraInfo(3, segmentindex.StrategyReplace, 2, true)
		assert.Equal(t, ".l3.s0.d1", result)
	})
}

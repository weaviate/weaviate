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
)

func TestHaltedShardsForTransfer(t *testing.T) {
	t.Run("returns true when shard is in halted map", func(t *testing.T) {
		idx := &Index{}
		idx.haltedShardsForTransfer.Store("shard1", struct{}{})

		_, ok := idx.haltedShardsForTransfer.Load("shard1")
		assert.True(t, ok)
	})

	t.Run("returns false when shard is not in halted map", func(t *testing.T) {
		idx := &Index{}
		idx.haltedShardsForTransfer.Store("shard1", struct{}{})

		_, ok := idx.haltedShardsForTransfer.Load("shard2")
		assert.False(t, ok)
	})

	t.Run("returns false when halted map is empty", func(t *testing.T) {
		idx := &Index{}

		_, ok := idx.haltedShardsForTransfer.Load("shard1")
		assert.False(t, ok)
	})

	t.Run("clear removes all entries", func(t *testing.T) {
		idx := &Index{}
		idx.haltedShardsForTransfer.Store("shard1", struct{}{})
		idx.haltedShardsForTransfer.Store("shard2", struct{}{})

		idx.haltedShardsForTransfer.Clear()

		_, ok1 := idx.haltedShardsForTransfer.Load("shard1")
		_, ok2 := idx.haltedShardsForTransfer.Load("shard2")
		assert.False(t, ok1)
		assert.False(t, ok2)
	})

	t.Run("delete removes single entry", func(t *testing.T) {
		idx := &Index{}
		idx.haltedShardsForTransfer.Store("shard1", struct{}{})
		idx.haltedShardsForTransfer.Store("shard2", struct{}{})

		idx.haltedShardsForTransfer.Delete("shard1")

		_, ok1 := idx.haltedShardsForTransfer.Load("shard1")
		_, ok2 := idx.haltedShardsForTransfer.Load("shard2")
		assert.False(t, ok1)
		assert.True(t, ok2)
	})
}

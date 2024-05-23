//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable(t *testing.T) {
	t.Run("empty returns no nodes", func(t *testing.T) {
		m := NewMemtable()
		nodes := m.Nodes()

		assert.Empty(t, nodes)
	})

	t.Run("returns only nodes for set bits - unique inserts", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{113, 213}) // ...1101
		m.Insert(5, []uint64{15, 25})    // ...0101
		m.Insert(0, []uint64{10, 20})    // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{113, 213}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("returns only nodes for set bits - overwriting inserts", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{11, 22, 33}) // ...1101
		m.Insert(5, []uint64{11, 22})      // ...0101
		m.Insert(0, []uint64{11})          // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{33}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("returns only nodes for set bits - overwriting inserts with deletes", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{11, 22, 33}) // ...1101
		m.Delete(5, []uint64{11, 22})      // ...0101
		m.Insert(5, []uint64{11, 22})      // ...0101
		m.Delete(0, []uint64{11})          // ...0000
		m.Insert(0, []uint64{11})          // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{33}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("delete does not mind key value", func(t *testing.T) {
		m := NewMemtable()
		m.Delete(13, []uint64{33}) // ...1101
		m.Delete(5, []uint64{22})  // ...0101
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())
	})

	t.Run("deletes all", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{33}) // ...1101
		m.Delete(13, []uint64{33}) // ...1101
		m.Insert(5, []uint64{22})  // ...0101
		m.Insert(0, []uint64{11})  // ...0000
		m.Delete(5, []uint64{22})  // ...0101
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())
	})

	t.Run("deletes all but one", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{33}) // ...1101
		m.Delete(13, []uint64{33}) // ...1101
		m.Insert(5, []uint64{22})  // ...0101
		m.Insert(0, []uint64{11})  // ...0000
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 2+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{22}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())
	})

	t.Run("delete removes values regardless of key being deleted", func(t *testing.T) {
		m := NewMemtable()
		m.Insert(13, []uint64{33})              // ...00001101
		m.Insert(5, []uint64{22})               // ...00000101
		m.Insert(0, []uint64{11})               // ...00000000
		m.Delete(123, []uint64{11, 22, 33, 44}) // ...01111011

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33, 44}, nodeNN.Deletions.ToArray())
	})
}

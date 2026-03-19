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

package replica

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// TestExpandDiscriminantToLeaves verifies that non-leaf bits are correctly
// expanded to leaf-level descendants, matching the result of a full DiffUsing
// traversal.
func TestExpandDiscriminantToLeaves(t *testing.T) {
	t.Run("NoDiff", func(t *testing.T) {
		// All-zero discriminant: no bits anywhere → expand is a no-op
		height := 4
		diff := hashtree.NewBitset(hashtree.NodesCount(height))
		expandDiscriminantToLeaves(diff, height)
		require.Zero(t, diff.SetCount())
	})

	t.Run("AllLeavesViaSingleRoot", func(t *testing.T) {
		// Set only the root bit; expanding must produce all leaf bits.
		height := 3
		diff := hashtree.NewBitset(hashtree.NodesCount(height))
		diff.Set(0)

		expandDiscriminantToLeaves(diff, height)

		// Only leaf bits should remain.
		firstLeaf := hashtree.NodesCount(height - 1)
		leafCount := hashtree.LeavesCount(height)
		require.Equal(t, leafCount, diff.SetCount())
		for i := 0; i < leafCount; i++ {
			require.True(t, diff.IsSet(firstLeaf+i), "leaf %d should be set", i)
		}
		// Internal bits should be cleared.
		for i := 0; i < firstLeaf; i++ {
			require.False(t, diff.IsSet(i), "internal node %d should not be set", i)
		}
	})

	t.Run("PartialDiff", func(t *testing.T) {
		// Simulate stopping at height-1: set the left child of the root only.
		// After expansion the leaves under that child should be set.
		height := 3
		diff := hashtree.NewBitset(hashtree.NodesCount(height))
		diff.Set(1) // left child of root (level-1 node 0)

		expandDiscriminantToLeaves(diff, height)

		// Left subtree leaves: nodes 7,8,9,10 (left half of level 3)
		require.Equal(t, 4, diff.SetCount())
		firstLeaf := hashtree.NodesCount(height - 1)
		for i := 0; i < 4; i++ {
			require.True(t, diff.IsSet(firstLeaf+i))
		}
		for i := 4; i < 8; i++ {
			require.False(t, diff.IsSet(firstLeaf+i))
		}
	})

	t.Run("LeafBitsAlreadySet", func(t *testing.T) {
		// Bits already at the leaf level must not be touched by expansion.
		height := 2
		diff := hashtree.NewBitset(hashtree.NodesCount(height))
		firstLeaf := hashtree.NodesCount(height - 1)
		diff.Set(firstLeaf + 1) // leaf 1

		expandDiscriminantToLeaves(diff, height)

		require.Equal(t, 1, diff.SetCount())
		require.True(t, diff.IsSet(firstLeaf+1))
	})

	t.Run("EquivalenceWithDiffUsing", func(t *testing.T) {
		// Build two trees with known differences. Use DiffUsing (full traversal)
		// to get the true leaf discriminant. Then simulate the skip-leaf-level
		// approach: run the comparison loop up to height-1 and call
		// expandDiscriminantToLeaves. The resulting leaf bits must be a superset
		// of the DiffUsing result (false positives are allowed; false negatives
		// are not).
		height := 4
		ht1, err := hashtree.NewHashTree(height)
		require.NoError(t, err)
		ht2, err := hashtree.NewHashTree(height)
		require.NoError(t, err)

		// Populate: leaf 5 differs, leaf 11 differs.
		require.NoError(t, ht1.AggregateLeafWith(5, []byte("a")))
		require.NoError(t, ht2.AggregateLeafWith(5, []byte("b")))
		require.NoError(t, ht1.AggregateLeafWith(11, []byte("c")))
		require.NoError(t, ht2.AggregateLeafWith(11, []byte("d")))

		// Full-traversal ground truth.
		trueDiff, err := ht1.Diff(ht2)
		require.NoError(t, err)

		// Simulate stopped-at-H-1 discriminant: manually replicate what
		// LevelDiff produces up through level height-1.
		leavesCount := hashtree.LeavesCount(height)
		digests1 := make([]hashtree.Digest, leavesCount)
		digests2 := make([]hashtree.Digest, leavesCount)

		diff := hashtree.NewBitset(hashtree.NodesCount(height))
		diff.Set(0)

		for l := 0; l < height; l++ {
			_, err := ht1.Level(l, diff, digests1)
			require.NoError(t, err)
			_, err = ht2.Level(l, diff, digests2)
			require.NoError(t, err)
			cnt := hashtree.LevelDiff(l, diff, digests1, digests2)
			if cnt == 0 {
				break
			}
		}

		expandDiscriminantToLeaves(diff, height)

		// Every leaf that is set in trueDiff must also be set in our result.
		firstLeaf := hashtree.NodesCount(height - 1)
		for i := 0; i < leavesCount; i++ {
			node := firstLeaf + i
			if trueDiff.IsSet(node) {
				require.True(t, diff.IsSet(node),
					"leaf %d: trueDiff has it set but expanded diff does not", i)
			}
		}
	})

	t.Run("Height0", func(t *testing.T) {
		// height=0: the single node is both root and leaf; expandDiscriminantToLeaves
		// must be a no-op (loop runs 0 times).
		diff := hashtree.NewBitset(hashtree.NodesCount(0))
		diff.Set(0)
		expandDiscriminantToLeaves(diff, 0)
		require.True(t, diff.IsSet(0))
		require.Equal(t, 1, diff.SetCount())
	})
}

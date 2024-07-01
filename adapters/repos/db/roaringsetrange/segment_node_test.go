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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestSegmentNode_WithDeletions(t *testing.T) {
	key := uint8(0)
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7}

	sn, err := NewSegmentNode(key, roaringset.NewBitmap(additions...), roaringset.NewBitmap(deletions...))
	require.Nil(t, err)
	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))
	assert.Equal(t, key, sn.Key())
	assert.ElementsMatch(t, additions, sn.Additions().ToArray())
	assert.ElementsMatch(t, deletions, sn.Deletions().ToArray())

	snBuf := NewSegmentNodeFromBuffer(buf)
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))
	assert.Equal(t, key, snBuf.Key())
	assert.ElementsMatch(t, additions, snBuf.Additions().ToArray())
	assert.ElementsMatch(t, deletions, snBuf.Deletions().ToArray())
}

func TestSegmentNode_WithoutDeletions(t *testing.T) {
	key := uint8(63)
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7} // ignored

	sn, err := NewSegmentNode(key, roaringset.NewBitmap(additions...), roaringset.NewBitmap(deletions...))
	require.Nil(t, err)
	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))
	assert.Equal(t, key, sn.Key())
	assert.ElementsMatch(t, additions, sn.Additions().ToArray())
	assert.True(t, sn.Deletions().IsEmpty())

	snBuf := NewSegmentNodeFromBuffer(buf)
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))
	assert.Equal(t, key, snBuf.Key())
	assert.ElementsMatch(t, additions, snBuf.Additions().ToArray())
	assert.True(t, snBuf.Deletions().IsEmpty())
}

func TestSegmentNode_WithDeletions_InitializingFromBufferTooLarge(t *testing.T) {
	key := uint8(0)
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7}

	sn, err := NewSegmentNode(key, roaringset.NewBitmap(additions...), roaringset.NewBitmap(deletions...))
	require.Nil(t, err)
	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	bufTooLarge := make([]byte, 3*len(buf))
	copy(bufTooLarge, buf)

	snBuf := NewSegmentNodeFromBuffer(bufTooLarge)
	// assert that the buffer self reports the useful length, not the length of
	// the initialization buffer
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))
	// assert that ToBuffer() returns a buffer that is no longer than the useful
	// length
	assert.Equal(t, len(buf), len(snBuf.ToBuffer()))

	assert.Equal(t, key, snBuf.Key())
	assert.ElementsMatch(t, additions, snBuf.Additions().ToArray())
	assert.ElementsMatch(t, deletions, snBuf.Deletions().ToArray())
}

func TestSegmentNode_WithoutDeletions_InitializingFromBufferTooLarge(t *testing.T) {
	key := uint8(63)
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7} // ignored

	sn, err := NewSegmentNode(key, roaringset.NewBitmap(additions...), roaringset.NewBitmap(deletions...))
	require.Nil(t, err)
	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	bufTooLarge := make([]byte, 3*len(buf))
	copy(bufTooLarge, buf)

	snBuf := NewSegmentNodeFromBuffer(bufTooLarge)
	// assert that the buffer self reports the useful length, not the length of
	// the initialization buffer
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))
	// assert that ToBuffer() returns a buffer that is no longer than the useful
	// length
	assert.Equal(t, len(buf), len(snBuf.ToBuffer()))

	assert.Equal(t, key, snBuf.Key())
	assert.ElementsMatch(t, additions, snBuf.Additions().ToArray())
	assert.True(t, snBuf.Deletions().IsEmpty())
}

func TestSegmentNode_DeletionsNotStoredForNon0Key(t *testing.T) {
	key1 := uint8(0)
	key2 := uint8(15)
	key3 := uint8(63)
	additions := roaringset.NewBitmap(1, 2, 3, 4, 6)
	deletions := roaringset.NewBitmap(5, 7)

	sn1, err := NewSegmentNode(key1, additions, deletions)
	require.Nil(t, err)
	sn2, err := NewSegmentNode(key2, additions, deletions)
	require.Nil(t, err)
	sn3, err := NewSegmentNode(key3, additions, deletions)
	require.Nil(t, err)

	assert.Greater(t, sn1.Len(), sn2.Len())
	assert.Equal(t, sn2.Len(), sn3.Len())
	assert.False(t, sn1.Deletions().IsEmpty())
	assert.True(t, sn2.Deletions().IsEmpty())
	assert.True(t, sn3.Deletions().IsEmpty())
}

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

package roaringset

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerialization_HappyPath(t *testing.T) {
	additions := NewBitmap(1, 2, 3, 4, 6)
	deletions := NewBitmap(5, 7)
	key := []byte("my-key")

	sn, err := NewSegmentNode(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	newSN := NewSegmentNodeFromBuffer(buf)
	assert.Equal(t, newSN.Len(), uint64(len(buf)))

	// without copying
	newAdditions := newSN.Additions()
	assert.True(t, newAdditions.Contains(4))
	assert.False(t, newAdditions.Contains(5))
	newDeletions := newSN.Deletions()
	assert.False(t, newDeletions.Contains(4))
	assert.True(t, newDeletions.Contains(5))
	assert.Equal(t, []byte("my-key"), newSN.PrimaryKey())

	// with copying
	newAdditions = newSN.AdditionsWithCopy()
	assert.True(t, newAdditions.Contains(4))
	assert.False(t, newAdditions.Contains(5))
	newDeletions = newSN.DeletionsWithCopy()
	assert.False(t, newDeletions.Contains(4))
	assert.True(t, newDeletions.Contains(5))
}

func TestSerialization_EmptyBitmapsReturnNil(t *testing.T) {
	// A node holding no additions/deletions writes a length indicator of 0 for
	// the empty region(s). Additions()/Deletions() return nil in that case
	// rather than allocating an empty bitmap.
	key := []byte("my-key")

	t.Run("both empty", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(), NewBitmap())
		require.Nil(t, err)

		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())
		assert.Nil(t, newSN.Additions())
		assert.Nil(t, newSN.Deletions())
		assert.Equal(t, key, newSN.PrimaryKey())
	})

	t.Run("additions present, deletions empty", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(1, 2, 3), NewBitmap())
		require.Nil(t, err)

		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())
		require.NotNil(t, newSN.Additions())
		assert.True(t, newSN.Additions().Contains(2))
		assert.Nil(t, newSN.Deletions())
	})

	t.Run("additions empty, deletions present", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(), NewBitmap(5, 7))
		require.Nil(t, err)

		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())
		assert.Nil(t, newSN.Additions())
		require.NotNil(t, newSN.Deletions())
		assert.True(t, newSN.Deletions().Contains(5))
	})
}

func TestSerialization_InitializingFromBufferTooLarge(t *testing.T) {
	additions := NewBitmap(1, 2, 3, 4, 6)
	deletions := NewBitmap(5, 7)
	key := []byte("my-key")

	sn, err := NewSegmentNode(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	bufTooLarge := make([]byte, 3*len(buf))
	copy(bufTooLarge, buf)

	newSN := NewSegmentNodeFromBuffer(bufTooLarge)
	// assert that the buffer self reports the useful length, not the length of
	// the initialization buffer
	assert.Equal(t, newSN.Len(), uint64(len(buf)))
	// assert that ToBuffer() returns a buffer that is no longer than the useful
	// length
	assert.Equal(t, len(buf), len(newSN.ToBuffer()))
}

func TestSerialization_UnhappyPath(t *testing.T) {
	t.Run("with primary key that's too long", func(t *testing.T) {
		key := make([]byte, math.MaxUint32+3)
		_, err := NewSegmentNode(key, nil, nil)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "key too long")
	})
}

func TestSerialization_KeyIndexAndWriteTo(t *testing.T) {
	buf := &bytes.Buffer{}
	offset := 7
	// write some dummy data, so we have an offset
	buf.Write(make([]byte, offset))

	additions := NewBitmap(1, 2, 3, 4, 6)
	deletions := NewBitmap(5, 7)
	key := []byte("my-key")

	sn, err := NewSegmentNode(key, additions, deletions)
	require.Nil(t, err)

	keyIndex, err := sn.KeyIndexAndWriteTo(buf, offset)
	require.Nil(t, err)

	res := buf.Bytes()
	assert.Equal(t, keyIndex.ValueEnd, len(res))

	newSN := NewSegmentNodeFromBuffer(res[keyIndex.ValueStart:keyIndex.ValueEnd])
	newAdditions := newSN.Additions()
	assert.True(t, newAdditions.Contains(4))
	assert.False(t, newAdditions.Contains(5))
	newDeletions := newSN.Deletions()
	assert.False(t, newDeletions.Contains(4))
	assert.True(t, newDeletions.Contains(5))
	assert.Equal(t, []byte("my-key"), newSN.PrimaryKey())
}

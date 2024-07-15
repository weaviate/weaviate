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

package roaringset

import (
	"bytes"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializationList_HappyPath(t *testing.T) {
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7}
	key := []byte("my-key")

	sn, err := NewSegmentNodeList(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	newSN := NewSegmentNodeListFromBuffer(buf)
	assert.Equal(t, newSN.Len(), uint64(len(buf)))

	// without copying
	newAdditions := newSN.Additions()

	assert.True(t, slices.Index(newAdditions, 4) != -1)
	assert.False(t, slices.Index(newAdditions, 5) != -1)
	newDeletions := newSN.Deletions()
	assert.False(t, slices.Index(newDeletions, 4) != -1)
	assert.True(t, slices.Index(newDeletions, 5) != -1)
	assert.True(t, slices.Index(newDeletions, 7) != -1)
	assert.Equal(t, []byte("my-key"), newSN.PrimaryKey())

	// with copying
	newAdditions = newSN.AdditionsWithCopy()
	assert.True(t, slices.Index(newAdditions, 4) != -1)
	assert.False(t, slices.Index(newAdditions, 5) != -1)
	newDeletions = newSN.DeletionsWithCopy()
	assert.False(t, slices.Index(newDeletions, 4) != -1)
	assert.True(t, slices.Index(newDeletions, 5) != -1)
	assert.True(t, slices.Index(newDeletions, 7) != -1)
}

func TestSerializationList_InitializingFromBufferTooLarge(t *testing.T) {
	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7}
	key := []byte("my-key")

	sn, err := NewSegmentNodeList(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	bufTooLarge := make([]byte, 3*len(buf))
	copy(bufTooLarge, buf)

	newSN := NewSegmentNodeListFromBuffer(bufTooLarge)
	// assert that the buffer self reports the useful length, not the length of
	// the initialization buffer
	assert.Equal(t, newSN.Len(), uint64(len(buf)))
	// assert that ToBuffer() returns a buffer that is no longer than the useful
	// length
	assert.Equal(t, len(buf), len(newSN.ToBuffer()))
}

func TestSerializationList_UnhappyPath(t *testing.T) {
	t.Run("with primary key that's too long", func(t *testing.T) {
		key := make([]byte, math.MaxUint32+3)
		_, err := NewSegmentNodeList(key, nil, nil)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "key too long")
	})
}

func TestSerializationList_KeyIndexAndWriteTo(t *testing.T) {
	buf := &bytes.Buffer{}
	offset := 7
	// write some dummy data, so we have an offset
	buf.Write(make([]byte, offset))

	additions := []uint64{1, 2, 3, 4, 6}
	deletions := []uint64{5, 7}
	key := []byte("my-key")

	sn, err := NewSegmentNodeList(key, additions, deletions)
	require.Nil(t, err)

	keyIndex, err := sn.KeyIndexAndWriteTo(buf, offset)
	require.Nil(t, err)

	res := buf.Bytes()
	assert.Equal(t, keyIndex.ValueEnd, len(res))

	newSN := NewSegmentNodeListFromBuffer(res[keyIndex.ValueStart:keyIndex.ValueEnd])
	newAdditions := newSN.Additions()
	assert.True(t, slices.Index(newAdditions, 4) != -1)
	assert.False(t, slices.Index(newAdditions, 5) != -1)
	newDeletions := newSN.Deletions()
	assert.False(t, slices.Index(newDeletions, 4) != -1)
	assert.True(t, slices.Index(newDeletions, 5) != -1)
	assert.Equal(t, []byte("my-key"), newSN.PrimaryKey())
}

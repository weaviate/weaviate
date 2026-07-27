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
	"encoding/binary"
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

func TestSerialization_CloneToBuf(t *testing.T) {
	key := []byte("my-key")

	t.Run("clones match the plain accessors and are independent of the node", func(t *testing.T) {
		additions := NewBitmap(1, 2, 3, 4, 6)
		deletions := NewBitmap(5, 7)
		sn, err := NewSegmentNode(key, additions, deletions)
		require.Nil(t, err)
		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())

		pool := NewBitmapBufPoolTrackingForTests()
		addClone, addRelease := newSN.AdditionsCloneToBuf(pool)
		delClone, delRelease := newSN.DeletionsCloneToBuf(pool)
		require.NotNil(t, addClone)
		require.NotNil(t, addRelease)
		require.NotNil(t, delClone)
		require.NotNil(t, delRelease)

		assert.Equal(t, additions.ToArray(), addClone.ToArray())
		assert.Equal(t, deletions.ToArray(), delClone.ToArray())

		// mutating the clones must not touch the node's memory
		addClone.Set(100)
		delClone.Set(200)
		assert.Equal(t, additions.ToArray(), newSN.Additions().ToArray())
		assert.Equal(t, deletions.ToArray(), newSN.Deletions().ToArray())

		addRelease()
		delRelease()
		assert.Equal(t, int64(0), pool.Outstanding())
	})

	t.Run("clone can grow in place within the pooled buffer's capacity", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(1), NewBitmap())
		require.Nil(t, err)
		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())

		// factor wrapper hands out a buffer with headroom, mirroring the first
		// disk layer's pool in SegmentGroup.roaringSetGet
		pool := NewBitmapBufPoolFactorWrapper(NewBitmapBufPoolTrackingForTests(), 2)
		addClone, addRelease := newSN.AdditionsCloneToBuf(pool)
		require.NotNil(t, addClone)
		defer addRelease()

		for i := uint64(2); i < 100; i++ {
			addClone.Set(i)
		}
		assert.Equal(t, 99, addClone.GetCardinality())
	})

	t.Run("empty regions return nil bitmap and nil release", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(), NewBitmap())
		require.Nil(t, err)
		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())

		pool := NewBitmapBufPoolTrackingForTests()
		addClone, addRelease := newSN.AdditionsCloneToBuf(pool)
		delClone, delRelease := newSN.DeletionsCloneToBuf(pool)
		assert.Nil(t, addClone)
		assert.Nil(t, addRelease)
		assert.Nil(t, delClone)
		assert.Nil(t, delRelease)
		assert.Equal(t, int64(0), pool.Outstanding())
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

// TestDeletionsCloneToBuf_CorruptHeaderStopsAtRegion pins the bounded decode
// window of the deletions clone: a region whose length indicator is shrunk
// below what the serialization's own header describes must fail loudly at
// the region boundary, not silently decode recycled pooled-buffer bytes.
// Cloning and releasing the intact deletions first primes the recycled
// buffer's tail with exactly the "missing" bytes, so an unbounded decode
// would silently reconstruct them.
func TestDeletionsCloneToBuf_CorruptHeaderStopsAtRegion(t *testing.T) {
	values := make([]uint64, 64)
	for i := range values {
		values[i] = uint64(i * 7)
	}
	additions := NewBitmap(1)
	deletions := NewBitmap(values...)

	sn, err := NewSegmentNode([]byte("key"), additions, deletions)
	require.NoError(t, err)

	pool := NewBitmapBufPoolRanged(nil, 1<<20, nil, 512, 1024, 4096)

	// positive control + adversarial priming (see godoc)
	intact, release := sn.DeletionsCloneToBuf(pool)
	require.NotNil(t, intact)
	require.Equal(t, deletions.ToArray(), intact.ToArray())
	release()

	// shrink the deletions region's length indicator (it sits right after
	// the additions region; see the SegmentNode layout godoc)
	addLen := binary.LittleEndian.Uint64(sn.data[8:16])
	delLenOff := 16 + addLen
	delLen := binary.LittleEndian.Uint64(sn.data[delLenOff : delLenOff+8])
	require.Greater(t, delLen, uint64(24), "fixture deletions region too small to truncate")
	binary.LittleEndian.PutUint64(sn.data[delLenOff:delLenOff+8], delLen-16)

	require.Panics(t, func() {
		bm, release := sn.DeletionsCloneToBuf(pool)
		defer release()
		bm.ToArray()
	}, "a corrupt deletions header must fail at the region boundary, not silently decode recycled buffer bytes")
}

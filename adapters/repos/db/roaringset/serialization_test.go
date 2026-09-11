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
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
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

	// The flush hands NewSegmentNode Compacted() bitmaps. Compacted keeps key 0's
	// container, so the copy still occupies bytes while ToBuffer reports it empty,
	// and that empty buffer is the zero length indicator Additions returns nil for.
	t.Run("a compacted empty bitmap still writes a zero length indicator", func(t *testing.T) {
		grownThenEmptied := NewBitmap(slice(0, 1000)...)
		for _, value := range slice(0, 1000) {
			grownThenEmptied.Remove(value)
		}

		empties := []struct {
			name string
			bm   *sroar.Bitmap
		}{
			{name: "never written to", bm: NewBitmap()},
			{name: "grown then emptied", bm: grownThenEmptied},
		}

		for _, tt := range empties {
			t.Run(tt.name, func(t *testing.T) {
				compacted := tt.bm.Compacted()
				require.Greater(t, compacted.LenInBytes(), 0,
					"Compacted keeps key 0's container, so the copy is not zero bytes")
				require.Empty(t, compacted.ToBuffer(),
					"a non-empty buffer here would serialize as a non-zero length indicator")

				sn, err := NewSegmentNode(key, compacted, compacted)
				require.NoError(t, err)

				newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())
				assert.Nil(t, newSN.Additions())
				assert.Nil(t, newSN.Deletions())
				assert.Equal(t, key, newSN.PrimaryKey())
			})
		}
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
		require.NotNil(t, addClone)
		require.NotNil(t, addRelease)

		assert.Equal(t, additions.ToArray(), addClone.ToArray())

		// mutating the clone must not touch the node's memory
		addClone.Set(100)
		assert.Equal(t, additions.ToArray(), newSN.Additions().ToArray())

		addRelease()
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

	t.Run("empty region returns nil bitmap and nil release", func(t *testing.T) {
		sn, err := NewSegmentNode(key, NewBitmap(), NewBitmap())
		require.Nil(t, err)
		newSN := NewSegmentNodeFromBuffer(sn.ToBuffer())

		pool := NewBitmapBufPoolTrackingForTests()
		addClone, addRelease := newSN.AdditionsCloneToBuf(pool)
		assert.Nil(t, addClone)
		assert.Nil(t, addRelease)
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

// TestSerialization_WrittenNodeParsesBackAtItsOffset writes a node past a
// prefix, the way both writers place one after the header and the nodes before
// it, and reads it back from where it landed.
func TestSerialization_WrittenNodeParsesBackAtItsOffset(t *testing.T) {
	buf := &bytes.Buffer{}
	start := 7
	// dummy prefix, so the node does not begin at zero
	buf.Write(make([]byte, start))

	additions := NewBitmap(1, 2, 3, 4, 6)
	deletions := NewBitmap(5, 7)
	key := []byte("my-key")

	sn, err := NewSegmentNode(key, additions, deletions)
	require.NoError(t, err)

	n, err := buf.Write(sn.ToBuffer())
	require.NoError(t, err)

	res := buf.Bytes()
	newSN := NewSegmentNodeFromBuffer(res[start : start+n])
	newAdditions := newSN.Additions()
	assert.True(t, newAdditions.Contains(4))
	assert.False(t, newAdditions.Contains(5))
	newDeletions := newSN.Deletions()
	assert.False(t, newDeletions.Contains(4))
	assert.True(t, newDeletions.Contains(5))
	assert.Equal(t, []byte("my-key"), newSN.PrimaryKey())
}

// TestNewSegmentNodeCompactedMatchesNewSegmentNode is the only thing comparing
// the two encoders, so without it they drift apart silently. Both are fed the
// same compacted bitmaps: the claim is about how the bytes are built, never
// which bytes.
func TestNewSegmentNodeCompactedMatchesNewSegmentNode(t *testing.T) {
	tests := []struct {
		name      string
		key       []byte
		additions []uint64
		deletions []uint64
	}{
		{name: "both empty", key: []byte("k")},
		{name: "additions only", key: []byte("k"), additions: slice(0, 100)},
		{name: "deletions only", key: []byte("k"), deletions: slice(0, 100)},
		{
			name:      "both present",
			key:       []byte("k"),
			additions: slice(0, 100),
			deletions: slice(200, 300),
		},
		{
			// Above the array container threshold on one side only, so the two
			// container types meet in one node.
			name:      "a bitmap container beside an array one",
			key:       []byte("k"),
			additions: slice(0, 5000),
			deletions: slice(70000, 70010),
		},
		{name: "zero-length key", key: []byte{}, additions: slice(0, 10)},
		{name: "long key", key: bytes.Repeat([]byte("k"), 1024), additions: slice(0, 10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			additions := NewBitmap(tt.additions...).Compacted()
			deletions := NewBitmap(tt.deletions...).Compacted()

			want, err := NewSegmentNode(tt.key, additions, deletions)
			require.NoError(t, err)

			got, _, err := NewSegmentNodeCompacted(tt.key, additions, deletions, nil)
			require.NoError(t, err)

			require.Equal(t, want.ToBuffer(), got.ToBuffer())
			require.Equal(t, want.Len(), got.Len())

			// Read back through the accessors, since equal bytes that no reader
			// can parse would still pass the comparison above.
			readBack := NewSegmentNodeFromBuffer(got.ToBuffer())
			require.Equal(t, tt.key, readBack.PrimaryKey())
			if len(tt.additions) == 0 {
				require.Nil(t, readBack.Additions())
			} else {
				require.Equal(t, tt.additions, readBack.Additions().ToArray())
			}
			if len(tt.deletions) == 0 {
				require.Nil(t, readBack.Deletions())
			} else {
				require.Equal(t, tt.deletions, readBack.Deletions().ToArray())
			}
		})
	}

	t.Run("a key over uint32 is refused before any buffer is sized", func(t *testing.T) {
		// The guard has to run ahead of the callbacks, or one of them sizes a node
		// around a key length the node cannot record.
		_, _, err := NewSegmentNodeCompacted(make([]byte, math.MaxUint32+1),
			NewBitmap(1), NewBitmap(), nil)
		require.ErrorContains(t, err, "key too long")
	})
}

// TestNewSegmentNodeCompactedReusesTheBuffer pins what the encoder is for. Byte
// equality alone would pass for an encoder that built each bitmap beside the
// node and copied it in.
func TestNewSegmentNodeCompactedReusesTheBuffer(t *testing.T) {
	t.Run("the node aliases the buffer it was given", func(t *testing.T) {
		buf := make([]byte, 4096)
		node, out, err := NewSegmentNodeCompacted([]byte("k"),
			NewBitmap(slice(0, 100)...).Compacted(), NewBitmap(), buf)
		require.NoError(t, err)

		require.Same(t, unsafe.SliceData(buf), unsafe.SliceData(out))
		require.Same(t, unsafe.SliceData(buf), unsafe.SliceData(node.ToBuffer()))
	})

	t.Run("encoding into the returned buffer costs less than one node's payload", func(t *testing.T) {
		additions := NewBitmap(slice(0, 1000)...).Compacted()

		var buf []byte
		res := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				node, out, err := NewSegmentNodeCompacted([]byte("k"), additions, NewBitmap(), buf)
				if err != nil || node == nil {
					b.Fatal(err)
				}
				buf = out
			}
		})

		// Bytes, not a count: an encoder building each bitmap beside the node and
		// copying it in also allocates fewer times with the buffer carried forward
		// than without, so only the size separates the two.
		require.Less(t, res.AllocedBytesPerOp(), int64(len(additions.ToBuffer())),
			"one node's payload must not be allocated per node")
	})

	t.Run("a smaller node after a larger one leaves none of it behind", func(t *testing.T) {
		// The shrink direction is where a leak lands: growing allocates fresh
		// zeroed memory and hides it. The 0xFF fill is what a dirty buffer looks
		// like to the second encode.
		buf := bytes.Repeat([]byte{0xFF}, 8192)

		_, buf, err := NewSegmentNodeCompacted([]byte("large"),
			NewBitmap(slice(0, 5000)...).Compacted(), NewBitmap(slice(0, 5000)...).Compacted(), buf)
		require.NoError(t, err)

		small := NewBitmap(slice(0, 3)...).Compacted()
		got, _, err := NewSegmentNodeCompacted([]byte("k"), small, NewBitmap(), buf)
		require.NoError(t, err)

		want, err := NewSegmentNode([]byte("k"), small, NewBitmap())
		require.NoError(t, err)
		require.Equal(t, want.ToBuffer(), got.ToBuffer())
	})
}

// TestCompactedToBufCallsGetOnceWithTheExactSize pins sroar's contract where it
// lives. NewSegmentNodeCompacted builds its callbacks internally, so no caller
// of it can observe them, and a sroar bump that changed the arity would surface
// inside the encoder rather than here.
func TestCompactedToBufCallsGetOnceWithTheExactSize(t *testing.T) {
	bm := NewBitmap(slice(0, 5000)...)

	calls := 0
	var handed int
	bm.CompactedToBuf(func(sizeBytes int) []byte {
		calls++
		handed = sizeBytes
		return make([]byte, sizeBytes)
	})

	require.Equal(t, 1, calls)
	require.Equal(t, len(bm.Compacted().ToBuffer()), handed)
}

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

package roaringsetrange

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
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

// TestNewSegmentNodeCompactedMatchesNewSegmentNode pins that the two encoders emit
// the same bytes across the input shapes cleanupLayer produces. NewSegmentNode is
// fed the same bitmaps already compacted, so only the byte building differs.
func TestNewSegmentNodeCompactedMatchesNewSegmentNode(t *testing.T) {
	tests := []struct {
		name      string
		key       uint8
		additions []uint64
		deletions []uint64
		// An empty slice still yields a non-nil bitmap, so nil is asked for
		// rather than inferred. cleanupLayer leaves Deletions nil for every
		// non-zero key, and for key 0 whenever the node records no deletions.
		nilDeletions bool
		// nilAdditions covers the other half of NewSegmentNodeCompacted's nil
		// contract. No production path produces it: a compaction cursor's additions
		// come from SegmentNode.Additions, and a merge clones.
		nilAdditions bool
	}{
		{
			name:      "both sides populated",
			additions: valuesFrom(0, 100),
			deletions: valuesFrom(200, 300),
		},
		{name: "additions empty", deletions: valuesFrom(200, 300)},
		{name: "deletions empty", additions: valuesFrom(0, 100)},
		{name: "both empty"},
		{name: "nil deletions on key 0", additions: valuesFrom(0, 100), nilDeletions: true},
		{name: "nil additions on key 0", deletions: valuesFrom(200, 300), nilAdditions: true},
		{
			name:      "deletions dropped for a non-zero key",
			key:       42,
			additions: valuesFrom(0, 100),
			deletions: valuesFrom(200, 300),
		},
		{
			name:      "a non-zero key whose additions are empty",
			key:       42,
			additions: nil,
		},
		{
			// Each side holds more values than fit an array container. Both
			// payloads are then bitmap containers, written through the unsafe
			// views sroar lays over the buffer at the odd offset 17.
			name:      "bitmap containers on both sides",
			additions: valuesFrom(0, 5000),
			deletions: valuesFrom(70000, 75000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var additions, wantAdditions *sroar.Bitmap
			if !tt.nilAdditions {
				additions = roaringset.NewBitmap(tt.additions...)
				wantAdditions = additions.Compacted()
			}
			var deletions, wantDeletions *sroar.Bitmap
			if !tt.nilDeletions {
				deletions = roaringset.NewBitmap(tt.deletions...)
				wantDeletions = deletions.Compacted()
			}

			want, err := NewSegmentNode(tt.key, wantAdditions, wantDeletions)
			require.NoError(t, err)

			got, _ := NewSegmentNodeCompacted(tt.key, additions, deletions, nil)

			require.Equal(t, want.ToBuffer(), got.ToBuffer())
			require.Equal(t, uint64(len(got.data)), got.Len(),
				"the node must declare the whole buffer it occupies")

			// The accessors read it back, since equal bytes that no reader can
			// parse would still pass the comparison above.
			readBack := NewSegmentNodeFromBuffer(got.ToBuffer())
			require.Equal(t, tt.key, readBack.Key())
			require.ElementsMatch(t, tt.additions, readBack.Additions().ToArray())
			if tt.key != 0 {
				require.Nil(t, readBack.Deletions())
			} else {
				require.ElementsMatch(t, tt.deletions, readBack.Deletions().ToArray())
			}
		})
	}
}

// TestNewSegmentNodeCompactedReusesTheScratch pins that the second return value is
// the scratch for the next call rather than the node's own bytes. Every row builds
// through the scratch the row before it handed back, so a node that shrinks is
// where the previous node's tail would leak in.
func TestNewSegmentNodeCompactedReusesTheScratch(t *testing.T) {
	big := valuesFrom(0, 5000)

	tests := []struct {
		name      string
		key       uint8
		additions []uint64
		deletions []uint64
	}{
		{name: "large, grows the scratch", additions: big, deletions: big},
		{name: "tiny after large", additions: []uint64{1}, deletions: []uint64{2}},
		{name: "a non-zero key after large", key: 7, additions: []uint64{1}},
		{name: "empty after large"},
		{name: "large again", additions: big, deletions: big},
	}

	var (
		scratch   []byte
		highWater int
		shrank    bool
	)
	for _, tt := range tests {
		additions := roaringset.NewBitmap(tt.additions...)
		deletions := roaringset.NewBitmap(tt.deletions...)

		want, err := NewSegmentNode(tt.key, additions.Compacted(), deletions.Compacted())
		require.NoError(t, err, "row %q", tt.name)

		handedIn := len(scratch)
		got, out := NewSegmentNodeCompacted(tt.key, additions, deletions, scratch)
		scratch = out

		require.Equal(t, want.ToBuffer(), got.ToBuffer(),
			"row %q: no byte of the previous node may survive inside this one", tt.name)

		if int(got.Len()) < handedIn {
			shrank = true
		}
		if int(got.Len()) > highWater {
			highWater = int(got.Len())
		}
		require.GreaterOrEqual(t, len(scratch), highWater,
			"row %q: the scratch must still hold the largest node built so far", tt.name)
	}

	require.True(t, shrank,
		"no row built a node smaller than the scratch it was handed, so the fixture cannot tell a carried-forward scratch from a fresh one")
}

func valuesFrom(first, end uint64) []uint64 {
	values := make([]uint64, 0, end-first)
	for v := first; v < end; v++ {
		values = append(values, v)
	}
	return values
}

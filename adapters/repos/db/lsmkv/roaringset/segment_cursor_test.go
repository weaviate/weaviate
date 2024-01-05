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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func TestSegmentCursor(t *testing.T) {
	seg, offsets := createDummySegment(t, 5)

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewSegmentCursor(seg, nil)
		key, layer, err := c.First()
		require.Nil(t, err)
		assert.Equal(t, []byte("00000"), key)
		assert.True(t, layer.Additions.Contains(0))
		assert.True(t, layer.Additions.Contains(1))
		assert.True(t, layer.Deletions.Contains(2))
		assert.True(t, layer.Deletions.Contains(3))
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewSegmentCursor(seg, nil)
		it := uint64(0)
		for key, layer, err := c.First(); key != nil; key, layer, err = c.Next() {
			require.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("%05d", it)), key)
			assert.True(t, layer.Additions.Contains(it*4))
			assert.True(t, layer.Additions.Contains(it*4+1))
			assert.True(t, layer.Deletions.Contains(it*4+2))
			assert.True(t, layer.Deletions.Contains(it*4+3))
			it++
		}

		assert.Equal(t, uint64(5), it)
	})

	t.Run("seek and iterate from there", func(t *testing.T) {
		seeker := createDummySeeker(t, offsets, 3)
		c := NewSegmentCursor(seg, seeker)

		// start on it 3 as this is where the seeker points us
		it := uint64(3)
		for key, layer, err := c.Seek([]byte("dummyseeker")); key != nil; key, layer, err = c.Next() {
			require.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("%05d", it)), key)
			assert.True(t, layer.Additions.Contains(it*4))
			assert.True(t, layer.Additions.Contains(it*4+1))
			assert.True(t, layer.Deletions.Contains(it*4+2))
			assert.True(t, layer.Deletions.Contains(it*4+3))
			it++
		}

		assert.Equal(t, uint64(5), it)
	})

	t.Run("seeker returns error", func(t *testing.T) {
		seeker := createDummySeeker(t, offsets, 3)
		seeker.err = fmt.Errorf("seek and fail")
		c := NewSegmentCursor(seg, seeker)

		_, _, err := c.Seek([]byte("dummyseeker"))
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "seek and fail")
	})
}

func createDummySegment(t *testing.T, count uint64) ([]byte, []uint64) {
	out := []byte{}
	offsets := []uint64{}

	for i := uint64(0); i < count; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		add := NewBitmap(i*4, i*4+1)
		del := NewBitmap(i*4+2, i*4+3)
		sn, err := NewSegmentNode(key, add, del)
		require.Nil(t, err)
		offsets = append(offsets, uint64(len(out)))
		out = append(out, sn.ToBuffer()...)
	}

	return out, offsets
}

func createDummySeeker(t *testing.T, offsets []uint64, pos int) *dummySeeker {
	return &dummySeeker{offsets, pos, nil}
}

type dummySeeker struct {
	offsets []uint64
	pos     int
	err     error
}

// Seek returns the hard-coded pos that was set on init time, it ignores the
// key
func (s dummySeeker) Seek(key []byte) (segmentindex.Node, error) {
	return segmentindex.Node{
		Start: s.offsets[s.pos],
		End:   s.offsets[s.pos+1],
	}, s.err
}

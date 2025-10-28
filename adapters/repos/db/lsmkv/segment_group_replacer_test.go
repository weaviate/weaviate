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

package lsmkv

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentReplacer_OnDisk(t *testing.T) {
	logger, _ := test.NewNullLogger()

	createDiskOf4 := func() (*SegmentGroup, *fakeSegment, *fakeSegment, *fakeSegment, *fakeSegment) {
		segA := newFakeReplaceSegment(map[string][]byte{
			"keyA": []byte("valueA"),
		})
		segA.setPath("directory/segment-0001.db")
		segB := newFakeReplaceSegment(map[string][]byte{
			"keyB": []byte("valueB"),
		})
		segB.setPath("directory/segment-0002.db")
		segC := newFakeReplaceSegment(map[string][]byte{
			"keyC": []byte("valueC"),
		})
		segC.setPath("directory/segment-0003.db")
		segD := newFakeReplaceSegment(map[string][]byte{
			"keyD": []byte("valueD"),
		})
		segD.setPath("directory/segment-0004.db")

		diskSegments := &SegmentGroup{
			logger:           logger,
			segments:         []Segment{segA, segB, segC, segD},
			segmentsWithRefs: map[string]Segment{},
		}

		return diskSegments, segA, segB, segC, segD
	}

	t.Run("switches on disk 2 segments", func(t *testing.T) {
		diskSegments, segA, segB, segC, segD := createDiskOf4()
		segBC := newFakeReplaceSegment(map[string][]byte{
			"keyB": []byte("valueB"),
			"keyC": []byte("valueC"),
		})

		replacer := newSegmentReplacer(diskSegments, 1, 2, segBC)
		oldLeft, oldRight, err := replacer.switchOnDisk()
		require.NoError(t, err)

		// replaced segments are returned
		assert.Equal(t, segB, oldLeft)
		assert.Equal(t, segC, oldRight)

		// replaced segments are marked for deletion
		assert.False(t, segA.isMarkedForDeletion)
		assert.True(t, segB.isMarkedForDeletion)
		assert.True(t, segC.isMarkedForDeletion)
		assert.False(t, segD.isMarkedForDeletion)
		assert.False(t, segBC.isMarkedForDeletion)

		// new segment has stripped extensions
		assert.False(t, segA.isStrippedExtensions)
		assert.False(t, segB.isStrippedExtensions)
		assert.False(t, segC.isStrippedExtensions)
		assert.False(t, segD.isStrippedExtensions)
		assert.True(t, segBC.isStrippedExtensions)
		assert.Equal(t, "0002", segBC.strippedLeftSegID)
		assert.Equal(t, "0003", segBC.strippedRightSegID)
	})

	t.Run("switches on disk 1 segment", func(t *testing.T) {
		diskSegments, segA, segB, segC, segD := createDiskOf4()
		segBB := newFakeReplaceSegment(map[string][]byte{
			"keyBB": []byte("valueBB"),
		})

		replacer := newSegmentReplacer(diskSegments, 1, 1, segBB)
		oldLeft, oldRight, err := replacer.switchOnDisk()
		require.NoError(t, err)

		// replaced segments are returned
		assert.Nil(t, oldLeft)
		assert.Equal(t, segB, oldRight)

		// replaced segments are marked for deletion
		assert.False(t, segA.isMarkedForDeletion)
		assert.True(t, segB.isMarkedForDeletion)
		assert.False(t, segC.isMarkedForDeletion)
		assert.False(t, segD.isMarkedForDeletion)
		assert.False(t, segBB.isMarkedForDeletion)

		// new segment has stripped extensions
		assert.False(t, segA.isStrippedExtensions)
		assert.False(t, segB.isStrippedExtensions)
		assert.False(t, segC.isStrippedExtensions)
		assert.False(t, segD.isStrippedExtensions)
		assert.True(t, segBB.isStrippedExtensions)
		assert.Equal(t, "", segBB.strippedLeftSegID)
		assert.Equal(t, "0002", segBB.strippedRightSegID)
	})
}

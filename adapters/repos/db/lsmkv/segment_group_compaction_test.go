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

	"github.com/stretchr/testify/assert"
)

func TestSegmentGroup_BestCompactionPair(t *testing.T) {
	var maxSegmentSize int64 = 10000

	tests := []struct {
		name         string
		segments     []*segment
		expectedPair []string
	}{
		{
			name: "single segment",
			segments: []*segment{
				{size: 1000, path: "segment0", level: 0},
			},
			expectedPair: nil,
		},
		{
			name: "two segments, same level",
			segments: []*segment{
				{size: 1000, path: "segment0", level: 0},
				{size: 1000, path: "segment1", level: 0},
			},
			expectedPair: []string{"segment0", "segment1"},
		},
		{
			name: "multiple segments, multiple levels, lowest level is picked",
			segments: []*segment{
				{size: 4000, path: "segment0", level: 2},
				{size: 4000, path: "segment1", level: 2},
				{size: 2000, path: "segment2", level: 1},
				{size: 2000, path: "segment3", level: 1},
				{size: 1000, path: "segment4", level: 0},
				{size: 1000, path: "segment5", level: 0},
			},
			expectedPair: []string{"segment4", "segment5"},
		},
		{
			name: "two segments that don't fit the max size, but eliglbe segments of a lower level are present",
			segments: []*segment{
				{size: 8000, path: "segment0", level: 3},
				{size: 8000, path: "segment1", level: 3},
				{size: 4000, path: "segment2", level: 2},
				{size: 4000, path: "segment3", level: 2},
			},
			expectedPair: []string{"segment2", "segment3"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sg := &SegmentGroup{
				segments:       test.segments,
				maxSegmentSize: maxSegmentSize,
			}
			pair := sg.bestCompactionCandidatePair()
			if test.expectedPair == nil {
				assert.Nil(t, pair)
			} else {
				leftPath := test.segments[pair[0]].path
				rightPath := test.segments[pair[1]].path
				assert.Equal(t, test.expectedPair, []string{leftPath, rightPath})
			}
		})
	}
}

func TestSegmenGroup_CompactionLargerThanMaxSize(t *testing.T) {
	maxSegmentSize := int64(10000)
	// this test only tests the unhappy path which has an early exist condition,
	// meaning we don't need real segments, it is only metadata that is evaluated
	// here.
	sg := &SegmentGroup{
		segments: []*segment{
			{size: 8000, path: "segment0", level: 3},
			{size: 8000, path: "segment1", level: 3},
		},
		maxSegmentSize: maxSegmentSize,
	}

	ok, err := sg.compactOnce()
	assert.False(t, ok, "segments are too large to run")
	assert.Nil(t, err)
}

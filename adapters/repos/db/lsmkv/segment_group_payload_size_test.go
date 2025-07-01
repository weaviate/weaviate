//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentGroup_Size(t *testing.T) {
	tests := []struct {
		name         string
		segments     []Segment
		expectedSize int64
		description  string
	}{
		{
			name:         "empty segment group",
			segments:     []Segment{},
			expectedSize: 0,
			description:  "should return 0 when no segments are present",
		},
		{
			name: "single segment",
			segments: []Segment{
				&segment{size: 1024},
			},
			expectedSize: 1024,
			description:  "should return the size of a single segment",
		},
		{
			name: "multiple segments with different sizes",
			segments: []Segment{
				&segment{size: 512},
				&segment{size: 1024},
				&segment{size: 2048},
			},
			expectedSize: 3584, // 512 + 1024 + 2048
			description:  "should return the sum of all segment sizes",
		},
		{
			name: "segments with zero size",
			segments: []Segment{
				&segment{size: 0},
				&segment{size: 1024},
				&segment{size: 0},
			},
			expectedSize: 1024,
			description:  "should handle segments with zero size correctly",
		},
		{
			name: "large sizes",
			segments: []Segment{
				&segment{size: 1024 * 1024}, // 1MB
				&segment{size: 2048 * 1024}, // 2MB
				&segment{size: 4096 * 1024}, // 4MB
			},
			expectedSize: 7168 * 1024, // 7MB
			description:  "should handle large sizes correctly",
		},
		{
			name: "many small segments",
			segments: []Segment{
				&segment{size: 1},
				&segment{size: 2},
				&segment{size: 3},
				&segment{size: 4},
				&segment{size: 5},
				&segment{size: 6},
				&segment{size: 7},
				&segment{size: 8},
				&segment{size: 9},
				&segment{size: 10},
			},
			expectedSize: 55, // sum of 1 to 10
			description:  "should handle many small segments correctly",
		},
		{
			name: "mixed sizes including edge cases",
			segments: []Segment{
				&segment{size: 0},
				&segment{size: 1},
				&segment{size: 1000000},
				&segment{size: 0},
				&segment{size: 999999},
			},
			expectedSize: 2000000, // 0 + 1 + 1000000 + 0 + 999999
			description:  "should handle mixed sizes including zeros and large values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg := &SegmentGroup{
				segments: tt.segments,
			}

			result := sg.Size()

			assert.Equal(t, tt.expectedSize, result, tt.description)
		})
	}
}

func TestSegmentGroup_Size_WithEnqueuedSegments(t *testing.T) {
	sg := &SegmentGroup{
		segments: []Segment{
			&segment{size: 100},
			&segment{size: 200},
		},
		enqueuedSegments: []Segment{
			&segment{size: 300},
			&segment{size: 400},
		},
	}

	expectedSize := int64(1000) // 100 + 200 + 300 + 400

	result := sg.Size()
	assert.Equal(t, expectedSize, result, "should include both regular and enqueued segments")
}

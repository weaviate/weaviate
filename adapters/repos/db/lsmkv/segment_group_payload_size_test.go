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

func TestSegmentGroup_PayloadSize(t *testing.T) {
	tests := []struct {
		name         string
		segments     []*segment
		expectedSize int64
		description  string
	}{
		{
			name:         "empty segment group",
			segments:     []*segment{},
			expectedSize: 0,
			description:  "should return 0 when no segments are present",
		},
		{
			name: "single segment with payload",
			segments: []*segment{
				{dataEndPos: 1024},
			},
			expectedSize: 1024,
			description:  "should return the payload size of a single segment",
		},
		{
			name: "multiple segments with different payload sizes",
			segments: []*segment{
				{dataEndPos: 512},
				{dataEndPos: 1024},
				{dataEndPos: 2048},
			},
			expectedSize: 3584, // 512 + 1024 + 2048
			description:  "should return the sum of all segment payload sizes",
		},
		{
			name: "segments with zero payload size",
			segments: []*segment{
				{dataEndPos: 0},
				{dataEndPos: 1024},
				{dataEndPos: 0},
			},
			expectedSize: 1024,
			description:  "should handle segments with zero payload size correctly",
		},
		{
			name: "large payload sizes",
			segments: []*segment{
				{dataEndPos: 1024 * 1024}, // 1MB
				{dataEndPos: 2048 * 1024}, // 2MB
				{dataEndPos: 4096 * 1024}, // 4MB
			},
			expectedSize: 7168 * 1024, // 7MB
			description:  "should handle large payload sizes correctly",
		},
		{
			name: "many small segments",
			segments: []*segment{
				{dataEndPos: 1},
				{dataEndPos: 2},
				{dataEndPos: 3},
				{dataEndPos: 4},
				{dataEndPos: 5},
				{dataEndPos: 6},
				{dataEndPos: 7},
				{dataEndPos: 8},
				{dataEndPos: 9},
				{dataEndPos: 10},
			},
			expectedSize: 55, // sum of 1 to 10
			description:  "should handle many small segments correctly",
		},
		{
			name: "mixed payload sizes including edge cases",
			segments: []*segment{
				{dataEndPos: 0},
				{dataEndPos: 1},
				{dataEndPos: 1000000},
				{dataEndPos: 0},
				{dataEndPos: 999999},
			},
			expectedSize: 2000000, // 0 + 1 + 1000000 + 0 + 999999
			description:  "should handle mixed payload sizes including zeros and large values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg := &SegmentGroup{
				segments: tt.segments,
			}

			result := sg.payloadSize()

			assert.Equal(t, tt.expectedSize, result, tt.description)
		})
	}
}

func TestSegmentGroup_PayloadSize_WithEnqueuedSegments(t *testing.T) {
	sg := &SegmentGroup{
		segments: []*segment{
			{dataEndPos: 100},
			{dataEndPos: 200},
		},
		enqueuedSegments: []*segment{
			{dataEndPos: 300},
			{dataEndPos: 400},
		},
	}

	expectedSize := int64(1000) // 100 + 200 + 300 + 400

	result := sg.payloadSize()
	assert.Equal(t, expectedSize, result, "should include both regular and enqueued segments")
}

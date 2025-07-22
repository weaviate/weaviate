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

	"github.com/bits-and-blooms/bloom/v3"
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

func TestBloomFilterSize(t *testing.T) {
	// Test to determine the actual size of our test bloom filter
	bf := createTestBloomFilter()
	bs := bf.BitSet()
	bsSize := bs.BinaryStorageSize()
	totalSize := bsSize + 2*8 // 2 uint64s

	t.Logf("Bloom filter size: %d bytes (bitset: %d + 2*uint64: %d)", totalSize, bsSize, 2*8)

	// Test what getBloomFilterSize actually returns
	actualSize := getBloomFilterSize(bf)
	t.Logf("getBloomFilterSize returns: %d bytes", actualSize)

	// Test what happens when we create a segment with this bloom filter
	seg := &segment{
		bloomFilter: bf,
	}
	sg := &SegmentGroup{
		segments: []Segment{seg},
	}
	metadataSize := sg.MetadataSize()
	t.Logf("SegmentGroup.MetadataSize() returns: %d bytes", metadataSize)
}

func TestSegmentGroup_MetadataSize(t *testing.T) {
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
			name: "single segment with bloom filter only",
			segments: []Segment{
				&segment{
					bloomFilter: createTestBloomFilter(),
				},
			},
			expectedSize: 60, // actual bloom filter size with 0.001 false positive rate
			description:  "should handle segment with bloom filter",
		},
		{
			name: "single segment with .cna file only",
			segments: []Segment{
				&segment{
					calcCountNetAdditions: true,
					path:                  "/tmp/test.dat",
				},
			},
			expectedSize: 12, // .cna files are always 12 bytes
			description:  "should return 12 bytes for .cna file",
		},
		{
			name: "multiple segments with .cna files",
			segments: []Segment{
				&segment{calcCountNetAdditions: true, path: "/tmp/test1.dat"},
				&segment{calcCountNetAdditions: true, path: "/tmp/test2.dat"},
				&segment{calcCountNetAdditions: true, path: "/tmp/test3.dat"},
			},
			expectedSize: 36, // 3 * 12 bytes
			description:  "should return sum of all .cna file sizes",
		},
		{
			name: "segment with secondary bloom filters",
			segments: []Segment{
				&segment{
					secondaryIndexCount:   2,
					secondaryBloomFilters: []*bloom.BloomFilter{createTestBloomFilter(), createTestBloomFilter()},
				},
			},
			expectedSize: 108, // 2 * 54 bytes
			description:  "should handle secondary bloom filters",
		},
		{
			name: "mixed segments with various metadata",
			segments: []Segment{
				&segment{calcCountNetAdditions: true, path: "/tmp/test1.dat"},                                          // 12 bytes
				&segment{calcCountNetAdditions: true, path: "/tmp/test2.dat"},                                          // 12 bytes
				&segment{secondaryIndexCount: 1, secondaryBloomFilters: []*bloom.BloomFilter{createTestBloomFilter()}}, // 60 bytes
			},
			expectedSize: 84, // 12 + 12 + 60 bytes
			description:  "should handle mixed metadata types correctly",
		},
		{
			name: "segment with nil bloom filter",
			segments: []Segment{
				&segment{
					bloomFilter:           nil,
					calcCountNetAdditions: true, // .cna file present
					path:                  "/tmp/test.dat",
				},
			},
			expectedSize: 12,
			description:  "should handle nil bloom filter gracefully",
		},
		{
			name: "segment with nil secondary bloom filters",
			segments: []Segment{
				&segment{
					secondaryIndexCount:   2,
					secondaryBloomFilters: nil,
					calcCountNetAdditions: true, // .cna file present
					path:                  "/tmp/test.dat",
				},
			},
			expectedSize: 12,
			description:  "should handle nil secondary bloom filters gracefully",
		},
		{
			name: "segment with mixed nil and non-nil secondary bloom filters",
			segments: []Segment{
				&segment{
					secondaryIndexCount:   3,
					secondaryBloomFilters: []*bloom.BloomFilter{nil, createTestBloomFilter(), nil},
					calcCountNetAdditions: true, // .cna file present
					path:                  "/tmp/test.dat",
				},
			},
			expectedSize: 60, // 12 + 48
			description:  "should handle mixed nil and non-nil secondary bloom filters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg := &SegmentGroup{
				segments: tt.segments,
			}

			result := sg.MetadataSize()

			assert.Equal(t, tt.expectedSize, result, tt.description)
		})
	}
}

func TestSegmentGroup_MetadataSize_WithEnqueuedSegments(t *testing.T) {
	sg := &SegmentGroup{
		segments: []Segment{
			&segment{calcCountNetAdditions: true, path: "/tmp/test1.dat"}, // 12 bytes
			&segment{calcCountNetAdditions: true, path: "/tmp/test2.dat"}, // 12 bytes
		},
		enqueuedSegments: []Segment{
			&segment{calcCountNetAdditions: true, path: "/tmp/test3.dat"}, // 12 bytes
			&segment{calcCountNetAdditions: true, path: "/tmp/test4.dat"}, // 12 bytes
		},
	}

	expectedSize := int64(48) // 4 * 12 bytes

	result := sg.MetadataSize()
	assert.Equal(t, expectedSize, result, "should include metadata from both regular and enqueued segments")
}

// Test helper function to create a mock segment with specific metadata
func createMockSegmentWithMetadata(hasCNA bool, bloomFilterSize int, secondaryBloomFilterSizes []int) *segment {
	seg := &segment{
		calcCountNetAdditions: hasCNA,
	}

	if hasCNA {
		seg.path = "/tmp/test.dat"
	}

	if bloomFilterSize > 0 {
		// Create a bloom filter with some data
		seg.bloomFilter = createTestBloomFilter()
	}

	if len(secondaryBloomFilterSizes) > 0 {
		seg.secondaryIndexCount = uint16(len(secondaryBloomFilterSizes))
		seg.secondaryBloomFilters = make([]*bloom.BloomFilter, len(secondaryBloomFilterSizes))
		for i, size := range secondaryBloomFilterSizes {
			if size > 0 {
				seg.secondaryBloomFilters[i] = createTestBloomFilter()
			}
		}
	}

	return seg
}

func TestSegmentGroup_MetadataSize_ComplexScenarios(t *testing.T) {
	tests := []struct {
		name         string
		segments     []*segment
		expectedSize int64
		description  string
	}{
		{
			name: "complex scenario with all metadata types",
			segments: []*segment{
				createMockSegmentWithMetadata(true, 100, []int{50, 75}),  // 12 + 60 + 48 + 48 = 168
				createMockSegmentWithMetadata(false, 200, []int{}),       // 60
				createMockSegmentWithMetadata(true, 0, []int{25, 0, 30}), // 12 + 48 + 0 + 48 = 108
			},
			expectedSize: 324, // 168 + 60 + 96
			description:  "should handle complex scenarios with mixed metadata types",
		},
		{
			name: "segments with only .cna files",
			segments: []*segment{
				createMockSegmentWithMetadata(true, 0, []int{}),
				createMockSegmentWithMetadata(true, 0, []int{}),
				createMockSegmentWithMetadata(true, 0, []int{}),
				createMockSegmentWithMetadata(true, 0, []int{}),
				createMockSegmentWithMetadata(true, 0, []int{}),
			},
			expectedSize: 60, // 5 * 12 bytes
			description:  "should handle segments with only .cna files",
		},
		{
			name: "segments with only bloom filters",
			segments: []*segment{
				createMockSegmentWithMetadata(false, 150, []int{}),
				createMockSegmentWithMetadata(false, 250, []int{}),
				createMockSegmentWithMetadata(false, 350, []int{}),
			},
			expectedSize: 180, // 3 * 60 bytes
			description:  "should handle segments with only bloom filters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert []*segment to []Segment for the test
			segments := make([]Segment, len(tt.segments))
			for i, seg := range tt.segments {
				segments[i] = seg
			}

			sg := &SegmentGroup{
				segments: segments,
			}

			result := sg.MetadataSize()

			assert.Equal(t, tt.expectedSize, result, tt.description)
		})
	}
}

// createTestBloomFilter creates a bloom filter with some data for testing
func createTestBloomFilter() *bloom.BloomFilter {
	// Create a bloom filter with the same parameters as the actual implementation
	// The actual implementation uses: bloom.NewWithEstimates(uint(len(keys)), 0.001)
	bf := bloom.NewWithEstimates(10, 0.001)
	bf.Add([]byte("test"))
	return bf
}

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
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/usecases/byteops"
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

// createTestBloomFilter creates a bloom filter with some data for testing
func createTestBloomFilter() *bloom.BloomFilter {
	// Create a bloom filter with the same parameters as the actual implementation
	// The actual implementation uses: bloom.NewWithEstimates(uint(len(keys)), 0.001)
	bf := bloom.NewWithEstimates(10, 0.001)
	bf.Add([]byte("test"))
	return bf
}

// createMockSegmentWithMetadataFile creates a segment with an actual metadata file for testing writeMetadata scenarios
func createMockSegmentWithMetadataFile(t *testing.T, hasCNA bool, bloomFilterSize int, secondaryBloomFilterSizes []int) *segment {
	seg := &segment{
		calcCountNetAdditions: hasCNA,
		useBloomFilter:        true,
		strategy:              segmentindex.StrategyReplace,
	}

	// Create a temporary directory for the segment
	tempDir := t.TempDir()
	seg.path = filepath.Join(tempDir, "test.dat")

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

	// Use the actual implementation to write the metadata file
	metadataPath := seg.metadataPath()
	if metadataPath != "" {
		// Create primary bloom filter data directly
		var primaryBloom []byte
		if seg.bloomFilter != nil {
			bfSize := getBloomFilterSize(seg.bloomFilter)
			rw := byteops.NewReadWriter(make([]byte, bfSize))
			if _, err := seg.bloomFilter.WriteTo(&rw); err != nil {
				t.Fatalf("failed to write primary bloom filter: %v", err)
			}
			primaryBloom = rw.Buffer
		}

		// Create secondary bloom filters data directly
		var secondaryBloom [][]byte
		if seg.secondaryIndexCount > 0 {
			secondaryBloom = make([][]byte, seg.secondaryIndexCount)
			for i, bf := range seg.secondaryBloomFilters {
				if bf != nil {
					bfSize := getBloomFilterSize(bf)
					rw := byteops.NewReadWriter(make([]byte, bfSize))
					if _, err := bf.WriteTo(&rw); err != nil {
						t.Fatalf("failed to write secondary bloom filter %d: %v", i, err)
					}
					secondaryBloom[i] = rw.Buffer
				}
			}
		}

		// Create CNA data directly
		var netAdditions []byte
		if hasCNA {
			// Create a simple CNA with a test count
			cnaData := make([]byte, 8)
			binary.LittleEndian.PutUint64(cnaData, 42) // Some test count
			netAdditions = cnaData
		}

		// Use the actual implementation to write the metadata file
		err := seg.writeMetadataToDisk(metadataPath, primaryBloom, secondaryBloom, netAdditions)
		if err != nil {
			t.Fatalf("failed to write metadata file: %v", err)
		}
	}

	return seg
}

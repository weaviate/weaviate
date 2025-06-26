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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	KiB = int64(1024)
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

func TestSegmentGroup_BestCompactionPair(t *testing.T) {
	var maxSegmentSize int64 = 10000

	tests := []struct {
		name         string
		segments     []Segment
		expectedPair []string
	}{
		{
			name: "single segment",
			segments: []Segment{
				&segment{size: 1000, path: "segment0", level: 0},
			},
			expectedPair: nil,
		},
		{
			name: "two segments, same level",
			segments: []Segment{
				&segment{size: 1000, path: "segment0", level: 0},
				&segment{size: 1000, path: "segment1", level: 0},
			},
			expectedPair: []string{"segment0", "segment1"},
		},
		{
			name: "multiple segments, multiple levels, lowest level is picked",
			segments: []Segment{
				&segment{size: 4000, path: "segment0", level: 2},
				&segment{size: 4000, path: "segment1", level: 2},
				&segment{size: 2000, path: "segment2", level: 1},
				&segment{size: 2000, path: "segment3", level: 1},
				&segment{size: 1000, path: "segment4", level: 0},
				&segment{size: 1000, path: "segment5", level: 0},
			},
			expectedPair: []string{"segment4", "segment5"},
		},
		{
			name: "two segments that don't fit the max size, but eliglbe segments of a lower level are present",
			segments: []Segment{
				&segment{size: 8000, path: "segment0", level: 3},
				&segment{size: 8000, path: "segment1", level: 3},
				&segment{size: 4000, path: "segment2", level: 2},
				&segment{size: 4000, path: "segment3", level: 2},
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
			pair, level := sg.findCompactionCandidates()
			if test.expectedPair == nil {
				assert.Nil(t, pair)
				assert.Equal(t, uint16(0), level)
			} else {
				leftPath := test.segments[pair[0]].getPath()
				rightPath := test.segments[pair[1]].getPath()
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
		segments: []Segment{
			&segment{size: 8000, path: "segment0", level: 3},
			&segment{size: 8000, path: "segment1", level: 3},
		},
		maxSegmentSize: maxSegmentSize,
	}

	ok, err := sg.compactOnce()
	assert.False(t, ok, "segments are too large to run")
	assert.Nil(t, err)
}

func TestSegmentGroup_CompactionCandidates(t *testing.T) {
	compactionResizeFactor := float32(1)
	sg := &SegmentGroup{
		segments: createSegments(),
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 14,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 14,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 14,
				controlPath:   "seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 14,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 14,
				controlPath:   "seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 14,
				controlPath:   "seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 14,
				controlPath:   "seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 14,
				controlPath:   "seg_36+seg_37+seg_38+seg_39",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 15,
				controlPath:   "seg_01+seg_02+seg_03",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 15,
				controlPath:   "seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 15,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 15,
				controlPath:   "seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 15,
				controlPath:   "seg_28+seg_29+seg_30+seg_31+seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 16,
				controlPath:   "seg_01+seg_02+seg_03+seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 16,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 17,
				controlPath:   "seg_01+seg_02+seg_03+seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11+seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 1,
				controlPath:   "seg_49+seg_50",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 2,
				controlPath:   "seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 3,
				controlPath:   "seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 4,
				controlPath:   "seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 5,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize300_Resize08(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:       createSegments(),
		maxSegmentSize: 300 * GiB,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 12,
				controlPath:   "seg_05+seg_06",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 12,
				controlPath:   "seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11+seg_12",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15+seg_16",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 12,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 12,
				controlPath:   "seg_18+seg_19+seg_20",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 12,
				controlPath:   "seg_21+seg_22",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 12,
				controlPath:   "seg_23+seg_24",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 12,
				controlPath:   "seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 12,
				controlPath:   "seg_31+seg_32",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 12,
				controlPath:   "seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 12,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 12,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 12,
				controlPath:   "seg_36+seg_37+seg_38",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 12,
				controlPath:   "seg_39+seg_40",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 12,
				controlPath:   "seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{23, 24},
				expectedLevel: 1,
				controlPath:   "seg_49+seg_50",
			},
			{
				expectedPair:  []int{22, 23},
				expectedLevel: 2,
				controlPath:   "seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 3,
				controlPath:   "seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 4,
				controlPath:   "seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 5,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize400_Resize09(t *testing.T) {
	compactionResizeFactor := float32(.9)
	sg := &SegmentGroup{
		segments:       createSegments(),
		maxSegmentSize: 400 * GiB,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19+seg_20+seg_21",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 1,
				controlPath:   "seg_49+seg_50",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 2,
				controlPath:   "seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 3,
				controlPath:   "seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 4,
				controlPath:   "seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 5,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize500_Resize08(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:       createSegments(),
		maxSegmentSize: 500 * GiB,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 13,
				controlPath:   "seg_03+seg_04+seg_05",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 13,
				controlPath:   "seg_03+seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21+seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 1,
				controlPath:   "seg_49+seg_50",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 2,
				controlPath:   "seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 3,
				controlPath:   "seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 4,
				controlPath:   "seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 5,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize600_Resize09(t *testing.T) {
	compactionResizeFactor := float32(.9)
	sg := &SegmentGroup{
		segments:       createSegments(),
		maxSegmentSize: 600 * GiB,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 14,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 14,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 14,
				controlPath:   "seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 14,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 14,
				controlPath:   "seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 14,
				controlPath:   "seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 14,
				controlPath:   "seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 14,
				controlPath:   "seg_36+seg_37+seg_38+seg_39",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 1,
				controlPath:   "seg_49+seg_50",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 2,
				controlPath:   "seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 3,
				controlPath:   "seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 4,
				controlPath:   "seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 5,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49+seg_50",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize300To600_Resize08(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:       createSegments(),
		maxSegmentSize: 300 * GiB,
	}

	t.Run("compact with 300 GiB limit", func(t *testing.T) {
		for pair, level := sg.findCompactionCandidates(); pair != nil; pair, level = sg.findCompactionCandidates() {
			compactSegments(sg, pair, level, compactionResizeFactor)
		}
	})

	t.Run("compact again with 600 GiB limit", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_21+seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37+seg_38",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_39+seg_40+seg_41+seg_42",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.maxSegmentSize = 600 * GiB
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_Leftover(t *testing.T) {
	compactionResizeFactor := float32(1)
	sg := &SegmentGroup{
		segments:                createSegments(),
		compactLeftOverSegments: true,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 14,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 14,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 14,
				controlPath:   "seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 14,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 14,
				controlPath:   "seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 14,
				controlPath:   "seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 14,
				controlPath:   "seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 14,
				controlPath:   "seg_36+seg_37+seg_38+seg_39",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 15,
				controlPath:   "seg_01+seg_02+seg_03",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 15,
				controlPath:   "seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 15,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 15,
				controlPath:   "seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 15,
				controlPath:   "seg_28+seg_29+seg_30+seg_31+seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 16,
				controlPath:   "seg_01+seg_02+seg_03+seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 16,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{0, 1},
				expectedLevel: 17,
				controlPath:   "seg_01+seg_02+seg_03+seg_04+seg_05+seg_06+seg_07+seg_08+seg_09+seg_10+seg_11+seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20+seg_21+seg_22+seg_23+seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 1,
				controlPath:   "seg_48+seg_49",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 2,
				controlPath:   "seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 3,
				controlPath:   "seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 4,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 8,
				controlPath:   "seg_43+seg_44",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 14,
				controlPath:   "seg_36+seg_37+seg_38+seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 15,
				controlPath:   "seg_28+seg_29+seg_30+seg_31+seg_32+seg_33+seg_34+seg_35+seg_36+seg_37+seg_38+seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize300_Resize08_Leftover(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:                createSegments(),
		maxSegmentSize:          300 * GiB,
		compactLeftOverSegments: true,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 12,
				controlPath:   "seg_05+seg_06",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 12,
				controlPath:   "seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11+seg_12",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 12,
				controlPath:   "seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15+seg_16",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 12,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 12,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 12,
				controlPath:   "seg_18+seg_19+seg_20",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 12,
				controlPath:   "seg_21+seg_22",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 12,
				controlPath:   "seg_23+seg_24",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 12,
				controlPath:   "seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 12,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 12,
				controlPath:   "seg_31+seg_32",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 12,
				controlPath:   "seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 12,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 12,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 12,
				controlPath:   "seg_36+seg_37+seg_38",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 12,
				controlPath:   "seg_39+seg_40",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 12,
				controlPath:   "seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  []int{22, 23},
				expectedLevel: 1,
				controlPath:   "seg_48+seg_49",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 2,
				controlPath:   "seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 3,
				controlPath:   "seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 4,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 8,
				controlPath:   "seg_43+seg_44",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize400_Resize09_Leftover(t *testing.T) {
	compactionResizeFactor := float32(.9)
	sg := &SegmentGroup{
		segments:                createSegments(),
		maxSegmentSize:          400 * GiB,
		compactLeftOverSegments: true,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19+seg_20+seg_21",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 1,
				controlPath:   "seg_48+seg_49",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 2,
				controlPath:   "seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 3,
				controlPath:   "seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 4,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 8,
				controlPath:   "seg_43+seg_44",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41+seg_42",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize500_Resize08_Leftover(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:                createSegments(),
		maxSegmentSize:          500 * GiB,
		compactLeftOverSegments: true,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 13,
				controlPath:   "seg_03+seg_04+seg_05",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 13,
				controlPath:   "seg_03+seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21+seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39+seg_40+seg_41",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 1,
				controlPath:   "seg_48+seg_49",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 2,
				controlPath:   "seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 3,
				controlPath:   "seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 4,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 8,
				controlPath:   "seg_43+seg_44",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize600_Resize09_Leftover(t *testing.T) {
	compactionResizeFactor := float32(.9)
	sg := &SegmentGroup{
		segments:                createSegments(),
		maxSegmentSize:          600 * GiB,
		compactLeftOverSegments: true,
	}

	t.Run("existing segments", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_06+seg_07",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_10+seg_11",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_12+seg_13",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_16+seg_17",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_18+seg_19",
			},
			{
				expectedPair:  []int{11, 12},
				expectedLevel: 13,
				controlPath:   "seg_20+seg_21",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 13,
				controlPath:   "seg_22+seg_23",
			},
			{
				expectedPair:  []int{13, 14},
				expectedLevel: 13,
				controlPath:   "seg_24+seg_25",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 13,
				controlPath:   "seg_28+seg_29",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 13,
				controlPath:   "seg_30+seg_31",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 13,
				controlPath:   "seg_32+seg_33",
			},
			{
				expectedPair:  []int{18, 19},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35",
			},
			{
				expectedPair:  []int{19, 20},
				expectedLevel: 13,
				controlPath:   "seg_36+seg_37",
			},
			{
				expectedPair:  []int{20, 21},
				expectedLevel: 13,
				controlPath:   "seg_38+seg_39",
			},
			{
				expectedPair:  []int{21, 22},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  []int{2, 3},
				expectedLevel: 14,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 14,
				controlPath:   "seg_08+seg_09+seg_10+seg_11",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 14,
				controlPath:   "seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 14,
				controlPath:   "seg_20+seg_21+seg_22+seg_23",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 14,
				controlPath:   "seg_24+seg_25+seg_26+seg_27",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 14,
				controlPath:   "seg_28+seg_29+seg_30+seg_31",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 14,
				controlPath:   "seg_32+seg_33+seg_34+seg_35",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 14,
				controlPath:   "seg_36+seg_37+seg_38+seg_39",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 14,
				controlPath:   "seg_12+seg_13+seg_14+seg_15+seg_16+seg_17+seg_18+seg_19",
			},
			{
				expectedPair:  []int{17, 18},
				expectedLevel: 1,
				controlPath:   "seg_48+seg_49",
			},
			{
				expectedPair:  []int{16, 17},
				expectedLevel: 2,
				controlPath:   "seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{15, 16},
				expectedLevel: 3,
				controlPath:   "seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{14, 15},
				expectedLevel: 4,
				controlPath:   "seg_45+seg_46+seg_47+seg_48+seg_49",
			},
			{
				expectedPair:  []int{12, 13},
				expectedLevel: 8,
				controlPath:   "seg_43+seg_44",
			},
			{
				expectedPair:  []int{10, 11},
				expectedLevel: 13,
				controlPath:   "seg_40+seg_41+seg_42",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})

	t.Run("new segment", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.segments = append(sg.segments, &segment{path: "seg_50", level: 0, size: 20 * MiB})
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

func TestSegmentGroup_CompactionCandidates_MaxSize300To600_Resize08_Leftover(t *testing.T) {
	compactionResizeFactor := float32(.8)
	sg := &SegmentGroup{
		segments:                createSegments(),
		maxSegmentSize:          300 * GiB,
		compactLeftOverSegments: true,
	}

	t.Run("compact with 300 GiB limit", func(t *testing.T) {
		for pair, level := sg.findCompactionCandidates(); pair != nil; pair, level = sg.findCompactionCandidates() {
			compactSegments(sg, pair, level, compactionResizeFactor)
		}
	})

	t.Run("compact again with 600 GiB limit", func(t *testing.T) {
		testCases := []testCaseCompactionCandidates{
			{
				expectedPair:  []int{3, 4},
				expectedLevel: 13,
				controlPath:   "seg_04+seg_05+seg_06+seg_07",
			},
			{
				expectedPair:  []int{4, 5},
				expectedLevel: 13,
				controlPath:   "seg_08+seg_09+seg_10+seg_11+seg_12+seg_13",
			},
			{
				expectedPair:  []int{5, 6},
				expectedLevel: 13,
				controlPath:   "seg_14+seg_15+seg_16+seg_17+seg_18+seg_19+seg_20",
			},
			{
				expectedPair:  []int{6, 7},
				expectedLevel: 13,
				controlPath:   "seg_21+seg_22+seg_23+seg_24+seg_25",
			},
			{
				expectedPair:  []int{7, 8},
				expectedLevel: 13,
				controlPath:   "seg_26+seg_27+seg_28+seg_29+seg_30+seg_31+seg_32+seg_33",
			},
			{
				expectedPair:  []int{8, 9},
				expectedLevel: 13,
				controlPath:   "seg_34+seg_35+seg_36+seg_37+seg_38",
			},
			{
				expectedPair:  []int{9, 10},
				expectedLevel: 13,
				controlPath:   "seg_39+seg_40+seg_41+seg_42",
			},
			{
				expectedPair:  []int{1, 2},
				expectedLevel: 14,
				controlPath:   "seg_02+seg_03",
			},
			{
				expectedPair:  nil,
				expectedLevel: 0,
			},
		}

		sg.maxSegmentSize = 600 * GiB
		runCompactionCandidatesTestCases(t, testCases, sg, compactionResizeFactor)
	})
}

type testCaseCompactionCandidates struct {
	expectedPair  []int
	expectedLevel uint16
	controlPath   string
}

func runCompactionCandidatesTestCases(t *testing.T, testCases []testCaseCompactionCandidates,
	sg *SegmentGroup, compactionResizeFactor float32,
) {
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("find candidates %s", tc.controlPath), func(t *testing.T) {
			pair, level := sg.findCompactionCandidates()
			require.ElementsMatch(t, tc.expectedPair, pair)
			require.Equal(t, tc.expectedLevel, level)

			if pair != nil {
				t.Run("compact", func(t *testing.T) {
					compactSegments(sg, pair, tc.expectedLevel, compactionResizeFactor)
					assert.Equal(t, tc.controlPath, sg.segments[pair[0]].getPath())
				})
			}
		})
	}
}

func compactSegments(sg *SegmentGroup, pair []int, newLevel uint16, resizeFactor float32) {
	leftId, rightId := pair[0], pair[1]
	left, right := sg.segments[leftId], sg.segments[rightId]

	seg := &segment{
		path:             left.getPath() + "+" + right.getPath(),
		size:             int64(float32(left.getSize()+right.getSize()) * resizeFactor),
		level:            newLevel,
		observeMetaWrite: func(n int64) {},
	}

	sg.segments[leftId] = seg
	sg.segments = append(sg.segments[:rightId], sg.segments[rightId+1:]...)
}

func createSegments() []Segment {
	return []Segment{
		&segment{path: "seg_01", level: 14, size: 836263427894},
		&segment{path: "seg_02", level: 13, size: 374869132170},
		&segment{path: "seg_03", level: 13, size: 208332808374},
		&segment{path: "seg_04", level: 12, size: 239015897301},
		&segment{path: "seg_05", level: 12, size: 106610102545},
		&segment{path: "seg_06", level: 12, size: 23426179335},
		&segment{path: "seg_07", level: 12, size: 87965523667},
		&segment{path: "seg_08", level: 12, size: 191582236181},
		&segment{path: "seg_09", level: 12, size: 210767274757},
		&segment{path: "seg_10", level: 12, size: 59578965712},
		&segment{path: "seg_11", level: 12, size: 64190979390},
		&segment{path: "seg_12", level: 12, size: 82209515753},
		&segment{path: "seg_13", level: 12, size: 75902833663},
		&segment{path: "seg_14", level: 12, size: 118868567716},
		&segment{path: "seg_15", level: 12, size: 127672461922},
		&segment{path: "seg_16", level: 12, size: 98975345366},
		&segment{path: "seg_17", level: 12, size: 68258824385},
		&segment{path: "seg_18", level: 12, size: 100849005187},
		&segment{path: "seg_19", level: 12, size: 102541173132},
		&segment{path: "seg_20", level: 12, size: 95981553544},
		&segment{path: "seg_21", level: 12, size: 159801966562},
		&segment{path: "seg_22", level: 12, size: 124441347108},
		&segment{path: "seg_23", level: 12, size: 134382829443},
		&segment{path: "seg_24", level: 12, size: 120928049419},
		&segment{path: "seg_25", level: 12, size: 96456793734},
		&segment{path: "seg_26", level: 12, size: 83607439705},
		&segment{path: "seg_27", level: 12, size: 96770548809},
		&segment{path: "seg_28", level: 12, size: 75610476308},
		&segment{path: "seg_29", level: 12, size: 90640520486},
		&segment{path: "seg_30", level: 12, size: 70865888540},
		&segment{path: "seg_31", level: 12, size: 210224834736},
		&segment{path: "seg_32", level: 12, size: 73153660353},
		&segment{path: "seg_33", level: 12, size: 76174252244},
		&segment{path: "seg_34", level: 12, size: 151728889040},
		&segment{path: "seg_35", level: 12, size: 128444521806},
		&segment{path: "seg_36", level: 12, size: 117679144581},
		&segment{path: "seg_37", level: 12, size: 75389068382},
		&segment{path: "seg_38", level: 12, size: 166442398845},
		&segment{path: "seg_39", level: 12, size: 131302230624},
		&segment{path: "seg_40", level: 12, size: 161545213956},
		&segment{path: "seg_41", level: 12, size: 85106406717},
		&segment{path: "seg_42", level: 12, size: 121845832221},
		&segment{path: "seg_43", level: 8, size: 7567704640},
		&segment{path: "seg_44", level: 7, size: 3025167714},
		&segment{path: "seg_45", level: 4, size: 372239668},
		&segment{path: "seg_46", level: 3, size: 176198587},
		&segment{path: "seg_47", level: 2, size: 92733242},
		&segment{path: "seg_48", level: 1, size: 45556463},
		&segment{path: "seg_49", level: 0, size: 24278171},
	}
}

func Test_IsSimilarSegmentSizes(t *testing.T) {
	type testCase struct {
		leftSize, rightSize int64
		expected            bool
	}

	testCases := []testCase{
		{
			leftSize:  10 * KiB,
			rightSize: 999 * KiB,
			expected:  true,
		},
		{
			leftSize:  2 * KiB,
			rightSize: 9 * MiB,
			expected:  true,
		},
		{
			leftSize:  88 * KiB,
			rightSize: 99 * KiB,
			expected:  true,
		},
		{
			leftSize:  2 * MiB,
			rightSize: 3 * MiB,
			expected:  true,
		},
		{
			leftSize:  1 * KiB,
			rightSize: 10 * MiB,
			expected:  true,
		},
		{
			leftSize:  1 * KiB,
			rightSize: 11 * MiB,
			expected:  false,
		},
		{
			leftSize:  1 * MiB,
			rightSize: 11 * MiB,
			expected:  false,
		},
		{
			leftSize:  2 * MiB,
			rightSize: 11 * MiB,
			expected:  true,
		},
		{
			leftSize:  2 * MiB,
			rightSize: 21 * MiB,
			expected:  false,
		},
		{
			leftSize:  15 * MiB,
			rightSize: 29 * MiB,
			expected:  true,
		},
		{
			leftSize:  9 * MiB,
			rightSize: 90 * MiB,
			expected:  true,
		},
		{
			leftSize:  9 * MiB,
			rightSize: 91 * MiB,
			expected:  false,
		},
		{
			leftSize:  10 * MiB,
			rightSize: 100 * MiB,
			expected:  true,
		},
		{
			leftSize:  11 * MiB,
			rightSize: 110 * MiB,
			expected:  false,
		},
		{
			leftSize:  22 * MiB,
			rightSize: 110 * MiB,
			expected:  true,
		},
		{
			leftSize:  199 * MiB,
			rightSize: 999 * MiB,
			expected:  false,
		},
		{
			leftSize:  200 * MiB,
			rightSize: 999 * MiB,
			expected:  true,
		},
		{
			leftSize:  777 * MiB,
			rightSize: 888 * MiB,
			expected:  true,
		},
		{
			leftSize:  500 * MiB,
			rightSize: 2 * GiB,
			expected:  false,
		},
		{
			leftSize:  700 * MiB,
			rightSize: 2 * GiB,
			expected:  true,
		},
		{
			leftSize:  2 * GiB,
			rightSize: 7 * GiB,
			expected:  false,
		},
		{
			leftSize:  3 * GiB,
			rightSize: 7 * GiB,
			expected:  true,
		},
		{
			leftSize:  5 * GiB,
			rightSize: 6 * GiB,
			expected:  true,
		},
		{
			leftSize:  4 * GiB,
			rightSize: 10 * GiB,
			expected:  true,
		},
		{
			leftSize:  4 * GiB,
			rightSize: 11 * GiB,
			expected:  false,
		},
		{
			leftSize:  5 * GiB,
			rightSize: 11 * GiB,
			expected:  false,
		},
		{
			leftSize:  6 * GiB,
			rightSize: 11 * GiB,
			expected:  true,
		},
		{
			leftSize:  24 * GiB,
			rightSize: 50 * GiB,
			expected:  false,
		},
		{
			leftSize:  25 * GiB,
			rightSize: 50 * GiB,
			expected:  true,
		},
		{
			leftSize:  111 * GiB,
			rightSize: 234 * GiB,
			expected:  false,
		},
		{
			leftSize:  123 * GiB,
			rightSize: 234 * GiB,
			expected:  true,
		},
		{
			leftSize:  666 * GiB,
			rightSize: 777 * GiB,
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d + %d", tc.leftSize, tc.rightSize), func(t *testing.T) {
			if tc.expected {
				assert.True(t, isSimilarSegmentSizes(tc.leftSize, tc.rightSize))
				assert.True(t, isSimilarSegmentSizes(tc.rightSize, tc.leftSize))
			} else {
				assert.False(t, isSimilarSegmentSizes(tc.leftSize, tc.rightSize))
				assert.False(t, isSimilarSegmentSizes(tc.rightSize, tc.leftSize))
			}
		})
	}
}

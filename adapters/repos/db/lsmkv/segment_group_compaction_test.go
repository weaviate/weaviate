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

func TestSegmentGroup_CompactionPairToFixLevelsOrder(t *testing.T) {
	testCases := []struct {
		name     string
		segments []Segment
		expPair  []string
		expLvl   uint16
	}{
		/*
			s08 s07 s06 s05 s04 s03 s02 s01
			 00  01  02  03  04  03  02  01
			 01__..  02  03  04  03  02  01
			 02______..  03  04  03  02  01
			 04__________..  04  03  02  01
			 05______________..  03  02  01
		*/
		{
			name: "1.1",
			segments: []Segment{
				&segment{path: "seg_08", level: 0},
				&segment{path: "seg_07", level: 1},
				&segment{path: "seg_06", level: 2},
				&segment{path: "seg_05", level: 3},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: []string{"seg_08", "seg_07"},
			expLvl:  1,
		},
		{
			name: "1.2",
			segments: []Segment{
				&segment{path: "seg_0807", level: 1},
				&segment{path: "seg_06", level: 2},
				&segment{path: "seg_05", level: 3},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: []string{"seg_0807", "seg_06"},
			expLvl:  2,
		},
		{
			name: "1.3",
			segments: []Segment{
				&segment{path: "seg_080706", level: 2},
				&segment{path: "seg_05", level: 3},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: []string{"seg_080706", "seg_05"},
			expLvl:  4,
		},
		{
			name: "1.4",
			segments: []Segment{
				&segment{path: "seg_08070605", level: 4},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: []string{"seg_08070605", "seg_04"},
			expLvl:  5,
		},
		{
			name: "1.5",
			segments: []Segment{
				&segment{path: "seg_0807060504", level: 5},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s05 s04 s03 s02 s01
			 02  04  03  02  01
			 04__..  03  02  01
		*/
		{
			name: "2.1",
			segments: []Segment{
				&segment{path: "seg_05", level: 2},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: []string{"seg_05", "seg_04"},
			expLvl:  4,
		},
		{
			name: "2.2",
			segments: []Segment{
				&segment{path: "seg_0504", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 2},
				&segment{path: "seg_01", level: 1},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s09 s08 s07 s06 s05 s04 s03 s02 s01
			 07  05  03  09  07  05  08  06  04
			 07  05  03  09  07__..  08  06  04
			 07  05  03  09__..      08  06  04
			 07  05__..  09          08  06  04
			 09__..      09          08  06  04
			 10__________..          08  06  04
		*/
		{
			name: "3.1",
			segments: []Segment{
				&segment{path: "seg_09", level: 7},
				&segment{path: "seg_08", level: 5},
				&segment{path: "seg_07", level: 3},
				&segment{path: "seg_06", level: 9},
				&segment{path: "seg_05", level: 7},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_05", "seg_04"},
			expLvl:  7,
		},
		{
			name: "3.2",
			segments: []Segment{
				&segment{path: "seg_09", level: 7},
				&segment{path: "seg_08", level: 5},
				&segment{path: "seg_07", level: 3},
				&segment{path: "seg_06", level: 9},
				&segment{path: "seg_0504", level: 7},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_06", "seg_0504"},
			expLvl:  9,
		},
		{
			name: "3.3",
			segments: []Segment{
				&segment{path: "seg_09", level: 7},
				&segment{path: "seg_08", level: 5},
				&segment{path: "seg_07", level: 3},
				&segment{path: "seg_060504", level: 9},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_08", "seg_07"},
			expLvl:  5,
		},
		{
			name: "3.4",
			segments: []Segment{
				&segment{path: "seg_09", level: 7},
				&segment{path: "seg_0807", level: 5},
				&segment{path: "seg_060504", level: 9},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_09", "seg_0807"},
			expLvl:  9,
		},
		{
			name: "3.5",
			segments: []Segment{
				&segment{path: "seg_090807", level: 9},
				&segment{path: "seg_060504", level: 9},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_090807", "seg_060504"},
			expLvl:  10,
		},
		{
			name: "3.6",
			segments: []Segment{
				&segment{path: "seg_090807060504", level: 10},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s09 s08 s07 s06 s05 s04 s03 s02 s01
			 09  07  05  07  05  03  08  06  04
			 09  07  05  07  05__..  08  06  04
			 09  07  05  07__..      08  06  04
			 09  07__..  07          08  06  04
			 09  08______..          08  06  04
			 09  09__________________..  06  04
			 10__..                      06  04
		*/
		{
			name: "4.1",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_06", level: 7},
				&segment{path: "seg_05", level: 5},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_05", "seg_04"},
			expLvl:  5,
		},
		{
			name: "4.2",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_06", level: 7},
				&segment{path: "seg_0504", level: 5},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_06", "seg_0504"},
			expLvl:  7,
		},
		{
			name: "4.3",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_060504", level: 7},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_08", "seg_07"},
			expLvl:  7,
		},
		{
			name: "4.4",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_0807", level: 7},
				&segment{path: "seg_060504", level: 7},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_0807", "seg_060504"},
			expLvl:  8,
		},
		{
			name: "4.5",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_0807060504", level: 8},
				&segment{path: "seg_03", level: 8},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_0807060504", "seg_03"},
			expLvl:  9,
		},
		{
			name: "4.6",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_080706050403", level: 9},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: []string{"seg_09", "seg_080706050403"},
			expLvl:  10,
		},
		{
			name: "4.7",
			segments: []Segment{
				&segment{path: "seg_09080706050403", level: 10},
				&segment{path: "seg_02", level: 6},
				&segment{path: "seg_01", level: 4},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s09 s08 s07 s06 s05 s04 s03 s02 s01
			 09  07  05  08  06  04  07  05  03
			 09  07  05  08  06__..  07  05  03
			 09  07  05  08__..      07  05  03
			 09  07__..  08          07  05  03
			 09__..      08          07  05  03
		*/
		{
			name: "5.1",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_06", level: 8},
				&segment{path: "seg_05", level: 6},
				&segment{path: "seg_04", level: 4},
				&segment{path: "seg_03", level: 7},
				&segment{path: "seg_02", level: 5},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_05", "seg_04"},
			expLvl:  6,
		},
		{
			name: "5.2",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_06", level: 8},
				&segment{path: "seg_0504", level: 6},
				&segment{path: "seg_03", level: 7},
				&segment{path: "seg_02", level: 5},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_06", "seg_0504"},
			expLvl:  8,
		},
		{
			name: "5.3",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 5},
				&segment{path: "seg_060504", level: 8},
				&segment{path: "seg_03", level: 7},
				&segment{path: "seg_02", level: 5},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_08", "seg_07"},
			expLvl:  7,
		},
		{
			name: "5.4",
			segments: []Segment{
				&segment{path: "seg_09", level: 9},
				&segment{path: "seg_0807", level: 7},
				&segment{path: "seg_060504", level: 8},
				&segment{path: "seg_03", level: 7},
				&segment{path: "seg_02", level: 5},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_09", "seg_0807"},
			expLvl:  9,
		},
		{
			name: "5.5",
			segments: []Segment{
				&segment{path: "seg_090807", level: 9},
				&segment{path: "seg_060504", level: 8},
				&segment{path: "seg_03", level: 7},
				&segment{path: "seg_02", level: 5},
				&segment{path: "seg_01", level: 3},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s10 s09 s08 s07 s06 s05 s04 s03 s02 s01
			 04  03  02  01  01  01  02  03  04  03
			 04  03  02  02__..  01  02  03  04  03
			 04  03  02  02______..  02  03  04  03
			 04  03  03__..          02  03  04  03
			 04  03  03______________..  03  04  03
			 04  04__..                  03  04  03
			 04  04______________________..  04  03
			 05__..                          04  03
		*/
		{
			name: "6.1",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08", level: 2},
				&segment{path: "seg_07", level: 1},
				&segment{path: "seg_06", level: 1},
				&segment{path: "seg_05", level: 1},
				&segment{path: "seg_04", level: 2},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_07", "seg_06"},
			expLvl:  2,
		},
		{
			name: "6.2",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08", level: 2},
				&segment{path: "seg_0706", level: 2},
				&segment{path: "seg_05", level: 1},
				&segment{path: "seg_04", level: 2},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_0706", "seg_05"},
			expLvl:  2,
		},
		{
			name: "6.3",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08", level: 2},
				&segment{path: "seg_070605", level: 2},
				&segment{path: "seg_04", level: 2},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_08", "seg_070605"},
			expLvl:  3,
		},
		{
			name: "6.4",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08070605", level: 3},
				&segment{path: "seg_04", level: 2},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_08070605", "seg_04"},
			expLvl:  3,
		},
		{
			name: "6.5",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_0807060504", level: 3},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_09", "seg_0807060504"},
			expLvl:  4,
		},
		{
			name: "6.6",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_090807060504", level: 4},
				&segment{path: "seg_03", level: 3},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_090807060504", "seg_03"},
			expLvl:  4,
		},
		{
			name: "6.7",
			segments: []Segment{
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09080706050403", level: 4},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: []string{"seg_10", "seg_09080706050403"},
			expLvl:  5,
		},
		{
			name: "6.8",
			segments: []Segment{
				&segment{path: "seg_1009080706050403", level: 5},
				&segment{path: "seg_02", level: 4},
				&segment{path: "seg_01", level: 3},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s20 s19 s18 s17 s16 s15 s14 s13 s12 s11 s10 s09 s08 s07 s06 s05 s04 s03 s02 s01
			 06  07  09  08  07  06  05  04  03  05  04  03  02  07  06  12  11  10  09  08
			 06  07  09  08  07  06  05  04  03  05  04  03  02  07__..  12  11  10  09  08
			 06  07  09  08  07  06  05  04  03  05  04  03__..  07      12  11  10  09  08
			 06  07  09  08  07  06  05  04  03  05  04__..      07      12  11  10  09  08
			 06  07  09  08  07  06  05  04  03  05__..          07      12  11  10  09  08
			 06  07  09  08  07  06  05  04__..  05              07      12  11  10  09  08
			 06  07  09  08  07  06  05__..      05              07      12  11  10  09  08
			 06  07  09  08  07  06  06__________..              07      12  11  10  09  08
			 06  07  09  08  07  07__..                          07      12  11  10  09  08
			 06  07  09  08  08__..                              07      12  11  10  09  08
			 06  07  09  08  08__________________________________..      12  11  10  09  08
			 06  07  09  09__..                                          12  11  10  09  08
			 06  07  10__..                         	                 12  11  10  09  08
			 07__..  10                                                  12  11  10  09  08
			 12______..                                                  12  11  10  09  08
			 13__________________________________________________________..  11  10  09  08
		*/
		{
			name: "7.1",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_13", level: 4},
				&segment{path: "seg_12", level: 3},
				&segment{path: "seg_11", level: 5},
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08", level: 2},
				&segment{path: "seg_07", level: 7},
				&segment{path: "seg_06", level: 6},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_07", "seg_06"},
			expLvl:  7,
		},
		{
			name: "7.2",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_13", level: 4},
				&segment{path: "seg_12", level: 3},
				&segment{path: "seg_11", level: 5},
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_09", level: 3},
				&segment{path: "seg_08", level: 2},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_09", "seg_08"},
			expLvl:  3,
		},
		{
			name: "7.3",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_13", level: 4},
				&segment{path: "seg_12", level: 3},
				&segment{path: "seg_11", level: 5},
				&segment{path: "seg_10", level: 4},
				&segment{path: "seg_0908", level: 3},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_10", "seg_0908"},
			expLvl:  4,
		},
		{
			name: "7.4",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_13", level: 4},
				&segment{path: "seg_12", level: 3},
				&segment{path: "seg_11", level: 5},
				&segment{path: "seg_100908", level: 4},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_11", "seg_100908"},
			expLvl:  5,
		},
		{
			name: "7.5",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_13", level: 4},
				&segment{path: "seg_12", level: 3},
				&segment{path: "seg_11100908", level: 5},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_13", "seg_12"},
			expLvl:  4,
		},
		{
			name: "7.6",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14", level: 5},
				&segment{path: "seg_1312", level: 4},
				&segment{path: "seg_11100908", level: 5},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_14", "seg_1312"},
			expLvl:  5,
		},
		{
			name: "7.7",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_141312", level: 5},
				&segment{path: "seg_11100908", level: 5},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_141312", "seg_11100908"},
			expLvl:  6,
		},
		{
			name: "7.8",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_15", level: 6},
				&segment{path: "seg_14131211100908", level: 6},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_15", "seg_14131211100908"},
			expLvl:  7,
		},
		{
			name: "7.9",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_16", level: 7},
				&segment{path: "seg_1514131211100908", level: 7},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_16", "seg_1514131211100908"},
			expLvl:  8,
		},
		{
			name: "7.10",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_161514131211100908", level: 8},
				&segment{path: "seg_0706", level: 7},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_161514131211100908", "seg_0706"},
			expLvl:  8,
		},
		{
			name: "7.11",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_17", level: 8},
				&segment{path: "seg_1615141312111009080706", level: 8},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_17", "seg_1615141312111009080706"},
			expLvl:  9,
		},
		{
			name: "7.12",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18", level: 9},
				&segment{path: "seg_171615141312111009080706", level: 9},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_18", "seg_171615141312111009080706"},
			expLvl:  10,
		},
		{
			name: "7.13",
			segments: []Segment{
				&segment{path: "seg_20", level: 6},
				&segment{path: "seg_19", level: 7},
				&segment{path: "seg_18171615141312111009080706", level: 10},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_20", "seg_19"},
			expLvl:  7,
		},
		{
			name: "7.14",
			segments: []Segment{
				&segment{path: "seg_2019", level: 7},
				&segment{path: "seg_18171615141312111009080706", level: 10},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_2019", "seg_18171615141312111009080706"},
			expLvl:  12,
		},
		{
			name: "7.15",
			segments: []Segment{
				&segment{path: "seg_201918171615141312111009080706", level: 12},
				&segment{path: "seg_05", level: 12},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: []string{"seg_201918171615141312111009080706", "seg_05"},
			expLvl:  13,
		},
		{
			name: "7.16",
			segments: []Segment{
				&segment{path: "seg_20191817161514131211100908070605", level: 13},
				&segment{path: "seg_04", level: 11},
				&segment{path: "seg_03", level: 10},
				&segment{path: "seg_02", level: 9},
				&segment{path: "seg_01", level: 8},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s15 s14 s13 s12 s11 s10 s09 s08 s07 s06 s05 s04 s03 s02 s01
			 17  16  15  18  14  12  11  11  11  11  08  05  04  01  00
			 17  16  15  18  14  12  12__..  11  11  08  05  04  01  00
			 17  16  15  18  14  12  12      12__..  08  05  04  01  00
			 17  16  15  18  14  13__..      12      08  05  04  01  00
			 17  16__..  18  14  13          12      08  05  04  01  00
			 18__..      18  14  13          12      08  05  04  01  00
			 19__________..  14  13          12      08  05  04  01  00
		*/
		{
			name: "8.1",
			segments: []Segment{
				&segment{path: "seg_15", level: 17},
				&segment{path: "seg_14", level: 16},
				&segment{path: "seg_13", level: 15},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_10", level: 12},
				&segment{path: "seg_09", level: 11},
				&segment{path: "seg_08", level: 11},
				&segment{path: "seg_07", level: 11},
				&segment{path: "seg_06", level: 11},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_09", "seg_08"},
			expLvl:  12,
		},
		{
			name: "8.2",
			segments: []Segment{
				&segment{path: "seg_15", level: 17},
				&segment{path: "seg_14", level: 16},
				&segment{path: "seg_13", level: 15},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_10", level: 12},
				&segment{path: "seg_0908", level: 12},
				&segment{path: "seg_07", level: 11},
				&segment{path: "seg_06", level: 11},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_07", "seg_06"},
			expLvl:  12,
		},
		{
			name: "8.3",
			segments: []Segment{
				&segment{path: "seg_15", level: 17},
				&segment{path: "seg_14", level: 16},
				&segment{path: "seg_13", level: 15},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_10", level: 12},
				&segment{path: "seg_0908", level: 12},
				&segment{path: "seg_0706", level: 12},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_10", "seg_0908"},
			expLvl:  13,
		},
		{
			name: "8.4",
			segments: []Segment{
				&segment{path: "seg_15", level: 17},
				&segment{path: "seg_14", level: 16},
				&segment{path: "seg_13", level: 15},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_100908", level: 13},
				&segment{path: "seg_0706", level: 12},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_14", "seg_13"},
			expLvl:  16,
		},
		{
			name: "8.5",
			segments: []Segment{
				&segment{path: "seg_15", level: 17},
				&segment{path: "seg_1413", level: 16},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_100908", level: 13},
				&segment{path: "seg_0706", level: 12},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_15", "seg_1413"},
			expLvl:  18,
		},
		{
			name: "8.6",
			segments: []Segment{
				&segment{path: "seg_151413", level: 18},
				&segment{path: "seg_12", level: 18},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_100908", level: 13},
				&segment{path: "seg_0706", level: 12},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_151413", "seg_12"},
			expLvl:  19,
		},
		{
			name: "8.7",
			segments: []Segment{
				&segment{path: "seg_15141312", level: 19},
				&segment{path: "seg_11", level: 14},
				&segment{path: "seg_100908", level: 13},
				&segment{path: "seg_0706", level: 12},
				&segment{path: "seg_05", level: 8},
				&segment{path: "seg_04", level: 5},
				&segment{path: "seg_03", level: 4},
				&segment{path: "seg_02", level: 1},
				&segment{path: "seg_01", level: 0},
			},
			expPair: nil,
			expLvl:  0,
		},

		/*
			s19 s18 s17 s16 s15 s14 s13 s12 s11 s10 s09 s08 s07 s06 s05 s04 s03 s02 s01
			 06  05  03  02  01  00  13  11  10  09  08  07  06  05  04  03  01  00  00
			 06  05  03  02  01  00  13  11  10  09  08  07  06  05  04  03  01  01__..
			 06  05  03  02  01  00  13  11  10  09  08  07  06  05  04  03  02__..
			 06  05  03  02  01__..  13  11  10  09  08  07  06  05  04  03  02
			 06  05  03  02__..      13  11  10  09  08  07  06  05  04  03  02
			 06  05  03__..          13  11  10  09  08  07  06  05  04  03  02
			 06  05__..              13  11  10  09  08  07  06  05  04  03  02
			 13__..                  13  11  10  09  08  07  06  05  04  03  02
			 14______________________..  11  10  09  08  07  06  05  04  03  02
		*/
		{
			name: "9.1",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17", level: 3},
				&segment{path: "seg_16", level: 2},
				&segment{path: "seg_15", level: 1},
				&segment{path: "seg_14", level: 0},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_03", level: 1},
				&segment{path: "seg_02", level: 0},
				&segment{path: "seg_01", level: 0},
			},
			expPair: []string{"seg_02", "seg_01"},
			expLvl:  1,
		},
		{
			name: "9.2",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17", level: 3},
				&segment{path: "seg_16", level: 2},
				&segment{path: "seg_15", level: 1},
				&segment{path: "seg_14", level: 0},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_03", level: 1},
				&segment{path: "seg_0201", level: 1},
			},
			expPair: []string{"seg_03", "seg_0201"},
			expLvl:  2,
		},
		{
			name: "9.3",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17", level: 3},
				&segment{path: "seg_16", level: 2},
				&segment{path: "seg_15", level: 1},
				&segment{path: "seg_14", level: 0},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_15", "seg_14"},
			expLvl:  1,
		},
		{
			name: "9.4",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17", level: 3},
				&segment{path: "seg_16", level: 2},
				&segment{path: "seg_1514", level: 1},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_16", "seg_1514"},
			expLvl:  2,
		},
		{
			name: "9.5",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17", level: 3},
				&segment{path: "seg_161514", level: 2},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_17", "seg_161514"},
			expLvl:  3,
		},
		{
			name: "9.6",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_18", level: 5},
				&segment{path: "seg_17161514", level: 3},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_18", "seg_17161514"},
			expLvl:  5,
		},
		{
			name: "9.7",
			segments: []Segment{
				&segment{path: "seg_19", level: 6},
				&segment{path: "seg_1817161514", level: 5},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_19", "seg_1817161514"},
			expLvl:  13,
		},
		{
			name: "9.8",
			segments: []Segment{
				&segment{path: "seg_191817161514", level: 13},
				&segment{path: "seg_13", level: 13},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: []string{"seg_191817161514", "seg_13"},
			expLvl:  14,
		},
		{
			name: "9.9",
			segments: []Segment{
				&segment{path: "seg_19181716151413", level: 14},
				&segment{path: "seg_12", level: 11},
				&segment{path: "seg_11", level: 10},
				&segment{path: "seg_10", level: 9},
				&segment{path: "seg_09", level: 8},
				&segment{path: "seg_08", level: 7},
				&segment{path: "seg_07", level: 6},
				&segment{path: "seg_06", level: 5},
				&segment{path: "seg_05", level: 4},
				&segment{path: "seg_04", level: 3},
				&segment{path: "seg_030201", level: 2},
			},
			expPair: nil,
			expLvl:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sg := &SegmentGroup{segments: tc.segments}
			pair, lvl := sg.findCompactionCandidates()

			if tc.expPair == nil {
				assert.Nil(t, pair)
			} else {
				require.NotNil(t, pair)
				lPath := tc.segments[pair[0]].getPath()
				rPath := tc.segments[pair[1]].getPath()
				assert.Equal(t, tc.expPair, []string{lPath, rPath})
			}
			assert.Equal(t, tc.expLvl, lvl)
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
		size:             int64(float32(left.Size()+right.Size()) * resizeFactor),
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

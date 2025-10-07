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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommitLogCombined_PartitionFiles(t *testing.T) {
	combiner := &CommitLogCombiner{}

	testcases := []struct {
		name       string
		fileNames  []string
		partitions []string
		expected   [][]string
	}{
		{
			name:       "no files, no partitions",
			fileNames:  []string{},
			partitions: []string{},
			expected:   [][]string{},
		},
		{
			name:       "no files, partitions",
			fileNames:  []string{},
			partitions: []string{"0001"},
			expected:   [][]string{},
		},
		{
			name:       "files, no partitions",
			fileNames:  []string{"0001.condensed", "0002"},
			partitions: []string{},
			expected: [][]string{
				{"0001.condensed", "0002"},
			},
		},
		{
			name:       "files, partitions (last file greater)",
			fileNames:  []string{"0001.condensed", "0002", "0003", "0004", "0005.condensed"},
			partitions: []string{"0002", "0004"},
			expected: [][]string{
				{"0001.condensed", "0002"},
				{"0003", "0004"},
				{"0005.condensed"},
			},
		},
		{
			name:       "files, partitions (last file equal)",
			fileNames:  []string{"0001.condensed", "0002", "0003", "0004", "0005.condensed"},
			partitions: []string{"0002", "0005"},
			expected: [][]string{
				{"0001.condensed", "0002"},
				{"0003", "0004", "0005.condensed"},
			},
		},
		{
			name:       "files, partitions (last file lower)",
			fileNames:  []string{"0001.condensed", "0002", "0003", "0004", "0005.condensed"},
			partitions: []string{"0002", "0006"},
			expected: [][]string{
				{"0001.condensed", "0002"},
				{"0003", "0004", "0005.condensed"},
			},
		},
		{
			name: "files, partitions (gaps)",
			fileNames: []string{
				"0001.condensed", "0003", "0005.condensed", "0010", "0011", "0013.condensed",
				"0017.condensed", "0018", "0019",
			},
			partitions: []string{"0002", "0006", "0008", "0012", "0017", "0020"},
			expected: [][]string{
				{"0001.condensed"},
				{"0003", "0005.condensed"},
				{"0010", "0011"},
				{"0013.condensed", "0017.condensed"},
				{"0018", "0019"},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			partitioned := combiner.partitonFileNames(tc.fileNames, tc.partitions)

			assert.ElementsMatch(t, tc.expected, partitioned)
		})
	}
}

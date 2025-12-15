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

//go:build integrationTest

package lsmkv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsCommitLogCompatible(t *testing.T) {
	pathReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757496896327170000.wal")
	pathMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757496219190885000.wal")
	pathInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757433233549050000.wal")
	pathSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757433233550212000.wal")
	pathRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757433233551248000.wal")
	pathRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757496903516930000.wal")
	pathRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757496903517104000.wal")

	testcases := []struct {
		name               string
		path               string
		expectedStrategies []string
	}{
		{
			name:               "strategy replace",
			path:               pathReplace,
			expectedStrategies: []string{StrategyReplace},
		},
		{
			name:               "strategy map collection",
			path:               pathMapCollection,
			expectedStrategies: []string{StrategyMapCollection, StrategyInverted, StrategySetCollection},
		},
		{
			name:               "strategy inverted",
			path:               pathInverted,
			expectedStrategies: []string{StrategyMapCollection, StrategyInverted, StrategySetCollection},
		},
		{
			name:               "strategy set collection",
			path:               pathSetCollection,
			expectedStrategies: []string{StrategySetCollection},
		},
		{
			name:               "strategy roaring set (bool)",
			path:               pathRoaringSet_bool,
			expectedStrategies: []string{StrategyRoaringSet},
		},
		{
			name:               "strategy roaring set (number)",
			path:               pathRoaringSet_number,
			expectedStrategies: []string{StrategyRoaringSet, StrategyRoaringSetRange},
		},
		{
			name:               "strategy roaring set range (number)",
			path:               pathRoaringSetRange_number,
			expectedStrategies: []string{StrategyRoaringSet, StrategyRoaringSetRange},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.Open(tc.path)
			require.NoError(t, err)
			defer file.Close()

			for _, expStrategy := range tc.expectedStrategies {
				t.Run(expStrategy, func(t *testing.T) {
					check := commitlogStrategyCompatibilityChecks[expStrategy]
					ok, err := check(file)

					assert.NoError(t, err)
					assert.True(t, ok)
				})
			}
			for _, invStrategy := range invertStrategies(tc.expectedStrategies) {
				t.Run(invStrategy, func(t *testing.T) {
					check := commitlogStrategyCompatibilityChecks[invStrategy]
					ok, err := check(file)

					assert.Error(t, err)
					assert.False(t, ok)
				})
			}
		})
	}
}

func TestGuessCommitLogStrategyAmong(t *testing.T) {
	pathReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757496896327170000.wal")
	pathMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757496219190885000.wal")
	pathInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757433233549050000.wal")
	pathSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757433233550212000.wal")
	pathRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757433233551248000.wal")
	pathRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757496903516930000.wal")
	pathRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757496903517104000.wal")

	t.Run("guess strategy with strategies", func(t *testing.T) {
		testcases := []struct {
			name             string
			path             string
			strategies       []string
			expectedStrategy string
		}{
			{
				name:             "strategy map collection",
				path:             pathMapCollection,
				strategies:       []string{StrategyMapCollection, StrategyInverted, StrategySetCollection},
				expectedStrategy: StrategyMapCollection,
			},
			{
				name:             "strategy map collection as inverted",
				path:             pathMapCollection,
				strategies:       []string{StrategyInverted, StrategySetCollection, StrategyMapCollection},
				expectedStrategy: StrategyInverted,
			},
			{
				name:             "strategy map collection as set collection",
				path:             pathMapCollection,
				strategies:       []string{StrategySetCollection, StrategyMapCollection, StrategyInverted},
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy inverted",
				path:             pathInverted,
				strategies:       []string{StrategyInverted, StrategyMapCollection, StrategySetCollection},
				expectedStrategy: StrategyInverted,
			},
			{
				name:             "strategy inverted as map collection",
				path:             pathInverted,
				strategies:       []string{StrategyMapCollection, StrategySetCollection, StrategyInverted},
				expectedStrategy: StrategyMapCollection,
			},
			{
				name:             "strategy inverted as set collection",
				path:             pathInverted,
				strategies:       []string{StrategySetCollection, StrategyInverted, StrategyMapCollection},
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy set collection",
				path:             pathSetCollection,
				strategies:       []string{StrategySetCollection, StrategyInverted, StrategyMapCollection},
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy set collection (2)",
				path:             pathSetCollection,
				strategies:       []string{StrategyInverted, StrategyMapCollection, StrategySetCollection},
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy set collection (3)",
				path:             pathSetCollection,
				strategies:       []string{StrategyMapCollection, StrategySetCollection, StrategyInverted},
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy roaringset",
				path:             pathRoaringSet_number,
				strategies:       []string{StrategyRoaringSet, StrategyRoaringSetRange},
				expectedStrategy: StrategyRoaringSet,
			},
			{
				name:             "strategy roaringset as roaringsetrange",
				path:             pathRoaringSet_number,
				strategies:       []string{StrategyRoaringSetRange, StrategyRoaringSet},
				expectedStrategy: StrategyRoaringSetRange,
			},
			{
				name:             "strategy roaringset (2)",
				path:             pathRoaringSet_bool,
				strategies:       []string{StrategyRoaringSet, StrategyRoaringSetRange},
				expectedStrategy: StrategyRoaringSet,
			},
			{
				name:             "strategy roaringset (3)",
				path:             pathRoaringSet_bool,
				strategies:       []string{StrategyRoaringSetRange, StrategyRoaringSet},
				expectedStrategy: StrategyRoaringSet,
			},
			{
				name:             "strategy roaringsetrange",
				path:             pathRoaringSetRange_number,
				strategies:       []string{StrategyRoaringSetRange, StrategyRoaringSet},
				expectedStrategy: StrategyRoaringSetRange,
			},
			{
				name:             "strategy roaringsetrange as roaringset",
				path:             pathRoaringSetRange_number,
				strategies:       []string{StrategyRoaringSet, StrategyRoaringSetRange},
				expectedStrategy: StrategyRoaringSet,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				file, err := os.Open(tc.path)
				require.NoError(t, err)
				defer file.Close()

				strategy, err := guessCommitLogStrategyAmong(file, tc.strategies)

				assert.NoError(t, err)
				assert.Equal(t, tc.expectedStrategy, strategy)
			})
		}
	})

	t.Run("guess strategy with no strategies", func(t *testing.T) {
		file, err := os.Open(pathReplace)
		require.NoError(t, err)
		defer file.Close()

		strategy, err := guessCommitLogStrategyAmong(file, nil)

		assert.ErrorContains(t, err, "not compatible")
		assert.Empty(t, strategy)
	})
}

func TestGuessCommitLogStrategy(t *testing.T) {
	pathReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757496896327170000.wal")
	pathMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757496219190885000.wal")
	pathInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757433233549050000.wal")
	pathSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757433233550212000.wal")
	pathRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757433233551248000.wal")
	pathRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757496903516930000.wal")
	pathRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757496903517104000.wal")

	t.Run("guess strategy", func(t *testing.T) {
		testcases := []struct {
			name             string
			path             string
			expectedStrategy string
		}{
			{
				name:             "strategy replace",
				path:             pathReplace,
				expectedStrategy: StrategyReplace,
			},
			{
				name:             "strategy map collection",
				path:             pathMapCollection,
				expectedStrategy: StrategyMapCollection,
			},
			{
				name:             "strategy inverted",
				path:             pathInverted,
				expectedStrategy: StrategyMapCollection,
			},
			{
				name:             "strategy set collection",
				path:             pathSetCollection,
				expectedStrategy: StrategySetCollection,
			},
			{
				name:             "strategy roaring set (bool)",
				path:             pathRoaringSet_bool,
				expectedStrategy: StrategyRoaringSet,
			},
			{
				name:             "strategy roaring set (number)",
				path:             pathRoaringSet_number,
				expectedStrategy: StrategyRoaringSet,
			},
			{
				name:             "strategy roaring set range (number)",
				path:             pathRoaringSetRange_number,
				expectedStrategy: StrategyRoaringSet,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				file, err := os.Open(tc.path)
				require.NoError(t, err)
				defer file.Close()

				strategy, err := guessCommitLogStrategy(file)

				assert.NoError(t, err)
				assert.Equal(t, tc.expectedStrategy, strategy)
			})
		}
	})
}

func invertStrategies(strategies []string) []string {
	selected := map[string]struct{}{}
	for i := range strategies {
		selected[strategies[i]] = struct{}{}
	}

	inverted := []string{}
	for strategy := range commitlogStrategyCompatibilityChecks {
		if _, ok := selected[strategy]; !ok {
			inverted = append(inverted, strategy)
		}
	}
	return inverted
}

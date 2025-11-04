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

//go:build integrationTest

package lsmkv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetermineUnloadedBucketStrategy(t *testing.T) {
	pathWalReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757496896327170000.wal")
	pathWalMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757496219190885000.wal")
	pathWalInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757433233549050000.wal")
	pathWalSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757433233550212000.wal")
	pathWalRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757433233551248000.wal")
	pathWalRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757496903516930000.wal")
	pathWalRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757496903517104000.wal")

	pathSegReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757605006102787000.db")
	pathSegMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757605013813574000.db")
	pathSegInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757673130782244000.db")
	pathSegSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757605013815208000.db")
	pathSegRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757605013811353000.db")
	pathSegRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757605013812425000.db")
	pathSegRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757605013813462000.db")

	t.Run("non existent bucket", func(t *testing.T) {
		strategy, err := DetermineUnloadedBucketStrategy("some/bucket/path/that/does/not/exist")

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("empty bucket", func(t *testing.T) {
		pathBucket := t.TempDir()

		strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("bucket with neither segments nor wals", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "file1.db.tmp"), []byte("unrelevant1"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "file2.some"), []byte("unrelevant2"), 0o644))

		strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("bucket with empty wal", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.wal"), []byte{}, 0o644))

		strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("bucket with empty segment", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.db"), []byte{}, 0o644))

		strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("bucket with segments and wals", func(t *testing.T) {
		testcases := []struct {
			name                  string
			walPath               string
			segPath               string
			expectedStrategyByWal string
			expectedStrategy      string
		}{
			{
				name:                  "map collection",
				walPath:               pathWalMapCollection,
				segPath:               pathSegMapCollection,
				expectedStrategyByWal: StrategyMapCollection,
				expectedStrategy:      StrategyMapCollection,
			},
			{
				name:                  "inverted",
				walPath:               pathWalInverted,
				segPath:               pathSegInverted,
				expectedStrategyByWal: StrategyMapCollection,
				expectedStrategy:      StrategyInverted,
			},
			{
				name:                  "set collection",
				walPath:               pathWalSetCollection,
				segPath:               pathSegSetCollection,
				expectedStrategyByWal: StrategySetCollection,
				expectedStrategy:      StrategySetCollection,
			},
			{
				name:                  "replace",
				walPath:               pathWalReplace,
				segPath:               pathSegReplace,
				expectedStrategyByWal: StrategyReplace,
				expectedStrategy:      StrategyReplace,
			},
			{
				name:                  "roaring set bool",
				walPath:               pathWalRoaringSet_bool,
				segPath:               pathSegRoaringSet_bool,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSet,
			},
			{
				name:                  "roaring set number",
				walPath:               pathWalRoaringSet_number,
				segPath:               pathSegRoaringSet_number,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSet,
			},
			{
				name:                  "roaring set range number",
				walPath:               pathWalRoaringSetRange_number,
				segPath:               pathSegRoaringSetRange_number,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSetRange,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				pathBucket := t.TempDir()

				// valid wal
				wal, err := os.ReadFile(tc.walPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, filepath.Base(tc.walPath)), wal, 0o644))
				// corrupted wal
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1000000000000000001.wal"),
					[]byte("i am corrupted wal and 1st one to be examined for strategy due to my name"), 0o644))
				// corrupted segment
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1000000000000000000.db"),
					[]byte("i am corrupted segment and 1st one to be examined for strategy due to my name"), 0o644))

				t.Run("by wal", func(t *testing.T) {
					strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

					assert.NoError(t, err)
					assert.Equal(t, tc.expectedStrategyByWal, strategy)
				})

				// valid segment
				seg, err := os.ReadFile(tc.segPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, filepath.Base(tc.segPath)), seg, 0o644))

				t.Run("by segment", func(t *testing.T) {
					strategy, err := DetermineUnloadedBucketStrategy(pathBucket)

					assert.NoError(t, err)
					assert.Equal(t, tc.expectedStrategy, strategy)
				})
			})
		}
	})
}

func TestDetermineUnloadedBucketStrategyAmong(t *testing.T) {
	pathWalReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757496896327170000.wal")
	pathWalMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757496219190885000.wal")
	pathWalInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757433233549050000.wal")
	pathWalSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757433233550212000.wal")
	pathWalRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757433233551248000.wal")
	pathWalRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757496903516930000.wal")
	pathWalRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757496903517104000.wal")

	pathSegReplace := filepath.Join("testdata", "strategy", "replace", "segment-1757605006102787000.db")
	pathSegMapCollection := filepath.Join("testdata", "strategy", "mapcollection", "segment-1757605013813574000.db")
	pathSegInverted := filepath.Join("testdata", "strategy", "inverted", "segment-1757673130782244000.db")
	pathSegSetCollection := filepath.Join("testdata", "strategy", "setcollection", "segment-1757605013815208000.db")
	pathSegRoaringSet_bool := filepath.Join("testdata", "strategy", "roaringset", "segment-1757605013811353000.db")
	pathSegRoaringSet_number := filepath.Join("testdata", "strategy", "roaringset", "segment-1757605013812425000.db")
	pathSegRoaringSetRange_number := filepath.Join("testdata", "strategy", "roaringsetrange", "segment-1757605013813462000.db")

	t.Run("no strategies given", func(t *testing.T) {
		strategy, err := DetermineUnloadedBucketStrategyAmong("some/bucket/path/that/does/not/exist", nil)

		assert.ErrorContains(t, err, "no prioritizedStrategies given")
		assert.Empty(t, strategy)
	})

	t.Run("non existent bucket, 1st strategy returned", func(t *testing.T) {
		strategy, err := DetermineUnloadedBucketStrategyAmong("some/bucket/path/that/does/not/exist",
			[]string{StrategyReplace, StrategyRoaringSet})

		assert.NoError(t, err)
		assert.Equal(t, StrategyReplace, strategy)
	})

	t.Run("empty bucket, 1st strategy returned", func(t *testing.T) {
		pathBucket := t.TempDir()

		strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyMapCollection, StrategySetCollection})

		assert.NoError(t, err)
		assert.Equal(t, StrategyMapCollection, strategy)
	})

	t.Run("bucket with neither segments nor wals, 1st strategy returned", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "file1.db.tmp"), []byte("unrelevant1"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "file2.some"), []byte("unrelevant2"), 0o644))

		strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyRoaringSet, StrategyMapCollection})

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSet, strategy)
	})

	t.Run("bucket with empty wal, 1st strategy returned", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.wal"), []byte{}, 0o644))

		strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyRoaringSetRange, StrategyMapCollection})

		assert.NoError(t, err)
		assert.Equal(t, StrategyRoaringSetRange, strategy)
	})

	t.Run("bucket with 2 wals and matching strategies", func(t *testing.T) {
		pathBucket := t.TempDir()

		data, err := os.ReadFile(pathWalMapCollection)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.wal"), data, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885001.wal"), data, 0o644))

		strategy1, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyMapCollection, StrategyRoaringSet})
		assert.NoError(t, err)
		assert.Equal(t, StrategyMapCollection, strategy1)

		strategy2, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyInverted, StrategyMapCollection, StrategyRoaringSet})
		assert.NoError(t, err)
		assert.Equal(t, StrategyInverted, strategy2)

		strategy3, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyRoaringSet, StrategySetCollection, StrategyInverted, StrategyMapCollection})
		assert.NoError(t, err)
		assert.Equal(t, StrategySetCollection, strategy3)
	})

	t.Run("bucket with 2 wals and not matching strategies", func(t *testing.T) {
		pathBucket := t.TempDir()

		data, err := os.ReadFile(pathWalMapCollection)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.wal"), data, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885001.wal"), data, 0o644))

		strategy1, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyReplace})
		assert.ErrorContains(t, err, "does not match")
		assert.Empty(t, strategy1)

		strategy2, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyRoaringSet, StrategyReplace})
		assert.ErrorContains(t, err, "does not match")
		assert.Empty(t, strategy2)

		strategy3, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyRoaringSetRange, StrategyRoaringSet, StrategyReplace})
		assert.ErrorContains(t, err, "does not match")
		assert.Empty(t, strategy3)
	})

	t.Run("bucket with empty segment, 1st strategy returned", func(t *testing.T) {
		pathBucket := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1757496219190885000.db"), []byte{}, 0o644))

		strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket,
			[]string{StrategyReplace, StrategyMapCollection})

		assert.NoError(t, err)
		assert.Equal(t, StrategyReplace, strategy)
	})

	t.Run("bucket with segments and wals", func(t *testing.T) {
		testcases := []struct {
			name                  string
			walPath               string
			segPath               string
			expectedStrategyByWal string
			expectedStrategy      string
		}{
			{
				name:                  "map collection",
				walPath:               pathWalMapCollection,
				segPath:               pathSegMapCollection,
				expectedStrategyByWal: StrategyMapCollection,
				expectedStrategy:      StrategyMapCollection,
			},
			{
				name:                  "inverted",
				walPath:               pathWalInverted,
				segPath:               pathSegInverted,
				expectedStrategyByWal: StrategyMapCollection,
				expectedStrategy:      StrategyInverted,
			},
			{
				name:                  "set collection",
				walPath:               pathWalSetCollection,
				segPath:               pathSegSetCollection,
				expectedStrategyByWal: StrategySetCollection,
				expectedStrategy:      StrategySetCollection,
			},
			{
				name:                  "replace",
				walPath:               pathWalReplace,
				segPath:               pathSegReplace,
				expectedStrategyByWal: StrategyReplace,
				expectedStrategy:      StrategyReplace,
			},
			{
				name:                  "roaring set bool",
				walPath:               pathWalRoaringSet_bool,
				segPath:               pathSegRoaringSet_bool,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSet,
			},
			{
				name:                  "roaring set number",
				walPath:               pathWalRoaringSet_number,
				segPath:               pathSegRoaringSet_number,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSet,
			},
			{
				name:                  "roaring set range number",
				walPath:               pathWalRoaringSetRange_number,
				segPath:               pathSegRoaringSetRange_number,
				expectedStrategyByWal: StrategyRoaringSet,
				expectedStrategy:      StrategyRoaringSetRange,
			},
		}

		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				pathBucket := t.TempDir()

				// valid wal
				wal, err := os.ReadFile(tc.walPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, filepath.Base(tc.walPath)), wal, 0o644))
				// corrupted wal
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1000000000000000001.wal"),
					[]byte("i am corrupted wal and 1st one to be examined for strategy due to my name"), 0o644))
				// corrupted segment
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, "segment-1000000000000000000.db"),
					[]byte("i am corrupted segment and 1st one to be examined for strategy due to my name"), 0o644))

				t.Run("by wal", func(t *testing.T) {
					t.Run("one prioritized strategy", func(t *testing.T) {
						strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket, []string{tc.expectedStrategy})

						assert.NoError(t, err)
						assert.Equal(t, tc.expectedStrategy, strategy)
					})

					t.Run("all prioritized strategies", func(t *testing.T) {
						strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket, prioritizedAllStrategies)

						assert.NoError(t, err)
						assert.Equal(t, tc.expectedStrategyByWal, strategy)
					})
				})

				// valid segment
				seg, err := os.ReadFile(tc.segPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(pathBucket, filepath.Base(tc.segPath)), seg, 0o644))

				t.Run("by segment", func(t *testing.T) {
					t.Run("all prioritized strategies", func(t *testing.T) {
						strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket, prioritizedAllStrategies)

						assert.NoError(t, err)
						assert.Equal(t, tc.expectedStrategy, strategy)
					})

					t.Run("no matching strategy", func(t *testing.T) {
						invStrategies := invertStrategies([]string{tc.expectedStrategy})

						strategy, err := DetermineUnloadedBucketStrategyAmong(pathBucket, invStrategies)

						assert.ErrorContains(t, err, "does not match")
						assert.Empty(t, strategy)
					})
				})
			})
		}
	})
}

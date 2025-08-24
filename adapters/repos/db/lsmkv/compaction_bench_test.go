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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func BenchmarkCompaction(b *testing.B) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	valuesPerSegment := 10000

	for _, strategy := range []string{StrategyMapCollection, StrategyReplace} {
		for _, pread := range []bool{true, false} {
			opts := []BucketOption{
				WithStrategy(strategy),
				WithKeepTombstones(true),
				WithPread(pread),
			}

			b.Run(fmt.Sprintf("strategy %s with pread: %t", strategy, pread), func(b *testing.B) {
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// avoid page cache by having a unique dir per run and have the content of the segments unique
					b.StopTimer()
					tmpDir := b.TempDir()

					bu, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
						cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
					require.NoError(b, err)

					for j := 0; j < valuesPerSegment; j++ {
						addData(b, bu, i, j, "key", "key2", strategy)
					}
					require.NoError(b, bu.FlushMemtable())

					fileTypes := getFiles(b, tmpDir)
					require.Equal(b, 1, fileTypes[".db"])

					for j := 0; j < valuesPerSegment; j++ {
						addData(b, bu, i, j, "key2", "key", strategy)
					}
					require.NoError(b, bu.FlushMemtable())

					fileTypes = getFiles(b, tmpDir)
					require.Equal(b, 2, fileTypes[".db"])
					b.StartTimer()

					once, err := bu.disk.compactOnce()
					require.NoError(b, err)
					require.True(b, once)

					require.NoError(b, bu.Shutdown(ctx))
				}
			})
		}
	}
}

func addData(b testing.TB, bu *Bucket, i, j int, prefixInsert, prefixDelete, strategy string) {
	if strategy == StrategyReplace {
		require.NoError(b, bu.Put([]byte(fmt.Sprintf("%s%d_%d", prefixInsert, j, i)), []byte(fmt.Sprintf("value%d_%d", j, i))))
	} else if strategy == StrategySetCollection {
		require.NoError(b, bu.SetAdd([]byte(fmt.Sprintf("%s%d_%d", prefixInsert, j, i)), [][]byte{[]byte(fmt.Sprintf("value%d_%d", j, i))}))
	} else if strategy == StrategyRoaringSet {
		require.NoError(b, bu.RoaringSetAddOne([]byte(fmt.Sprintf("%s%d_%d", prefixInsert, j, i)), uint64(i+j)))
	} else if strategy == StrategyMapCollection {
		require.NoError(b, bu.MapSet(
			[]byte(fmt.Sprintf("%s%d_%d", prefixInsert, j, i)), MapPair{Key: []byte(fmt.Sprintf("%sMP%d_%d", prefixInsert, j, i)), Value: []byte(fmt.Sprintf("value%d_%d", j, i))}))
		if j%10 == 0 {
			require.NoError(b, bu.MapDeleteKey([]byte(fmt.Sprintf("%s%d_%d", prefixDelete, j, i)), []byte(fmt.Sprintf("%sMP%d_%d", prefixDelete, j, i))))
		}
	} else if strategy == StrategyRoaringSetRange {
		require.NoError(b, bu.RoaringSetRangeAdd(uint64(i+j), uint64(i+j)))
	} else {
		require.Fail(b, "unknown strategy %s", strategy)
	}
}

func getFiles(t testing.TB, dirName string) map[string]int {
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	fileTypes := map[string]int{}
	for _, entry := range entries {
		fileTypes[filepath.Ext(entry.Name())] += 1
	}
	return fileTypes
}

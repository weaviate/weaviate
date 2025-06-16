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

	opts := []BucketOption{
		WithStrategy(StrategyMapCollection),
		WithKeepTombstones(true),
		WithPread(true),
	}

	valuesPerSegment := 10000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// avoid page cache by having a unique dir per run and have the content of the segments unique
		b.StopTimer()
		tmpDir := b.TempDir()

		bu, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(b, err)

		for j := 0; j < valuesPerSegment; j++ {
			require.NoError(b, bu.MapSet(
				[]byte(fmt.Sprintf("key%d_%d", j, i)), MapPair{Key: []byte(fmt.Sprintf("keyMP%d_%d", j, i)), Value: []byte(fmt.Sprintf("value_%d_%d", j, i))}))
			if j%10 == 0 {
				require.NoError(b, bu.MapDeleteKey([]byte(fmt.Sprintf("key2_%d_%d", j, i)), []byte(fmt.Sprintf("keyMP2_%d_%d", j, i))))
			}
		}
		require.NoError(b, bu.FlushMemtable())

		fileTypes := getFiles(b, tmpDir)
		require.Equal(b, 1, fileTypes[".db"])

		for j := 0; j < valuesPerSegment; j++ {
			require.NoError(b, bu.MapSet(
				[]byte(fmt.Sprintf("key2%d_%d", j, i)), MapPair{Key: []byte(fmt.Sprintf("keyMP2%d_%d", j, i)), Value: []byte(fmt.Sprintf("value%d_%d", j, i))}))
			if j%10 == 0 {
				require.NoError(b, bu.MapDeleteKey([]byte(fmt.Sprintf("key%d_%d", j, i)), []byte(fmt.Sprintf("keyMP%d_%d", j, i))))
			}
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

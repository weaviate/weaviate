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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestBucketWalReload(t *testing.T) {
	for _, strategy := range []string{StrategyReplace, StrategySetCollection, StrategyMapCollection, StrategyRoaringSet, StrategyRoaringSetRange} {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			dirName := t.TempDir()

			logger, _ := test.NewNullLogger()

			secondaryIndicesCount := uint16(0)
			if strategy == StrategyReplace {
				secondaryIndicesCount = 1
			}

			// initial bucket, always create segment, even if it is just a single entry
			b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)

			switch strategy {
			case StrategyReplace:
				require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1"))))
			case StrategySetCollection:
				require.NoError(t, b.SetAdd([]byte("hello1"), [][]byte{[]byte("world1")}))
			case StrategyRoaringSet:
				require.NoError(t, b.RoaringSetAddOne([]byte("hello1"), uint64(1)))
			case StrategyMapCollection:
				require.NoError(t, b.MapSet([]byte("hello1"), MapPair{Key: []byte("hello1"), Value: []byte("world1")}))
			case StrategyRoaringSetRange:
				require.NoError(t, b.RoaringSetRangeAdd(uint64(1), uint64(1)))
			default:
				require.Fail(t, "unknown strategy %s", strategy)
			}
			testBucketContent(t, strategy, b, 2)

			require.NoError(t, b.Shutdown(ctx))

			entries, err := os.ReadDir(dirName)
			require.NoError(t, err)
			require.Len(t, entries, 1, "single wal file should be created")

			testBucketContent(t, strategy, b, 2)

			// start fresh with a new memtable, new entries will stay in wal until size is reached
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)

			switch strategy {
			case StrategyReplace:
				require.NoError(t, b.Put([]byte("hello2"), []byte("world2"), WithSecondaryKey(0, []byte("bonjour2"))))
			case StrategySetCollection:
				require.NoError(t, b.SetAdd([]byte("hello2"), [][]byte{[]byte("world2")}))
			case StrategyRoaringSet:
				require.NoError(t, b.RoaringSetAddOne([]byte("hello2"), uint64(2)))
			case StrategyMapCollection:
				require.NoError(t, b.MapSet([]byte("hello2"), MapPair{Key: []byte("hello2"), Value: []byte("world2")}))
			case StrategyRoaringSetRange:
				require.NoError(t, b.RoaringSetRangeAdd(uint64(2), uint64(2)))
			}
			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			fileTypes := map[string]int{}
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 0, fileTypes[".db"], "no segment file")
			require.Equal(t, 1, fileTypes[".wal"], "single wal file")

			// will load wal and reuse memtable
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(1),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 3)

			switch strategy {
			case StrategyReplace:
				require.NoError(t, b.Put([]byte("hello3"), []byte("world3"), WithSecondaryKey(0, []byte("bonjour3"))))
			case StrategySetCollection:
				require.NoError(t, b.SetAdd([]byte("hello3"), [][]byte{[]byte("world3")}))
			case StrategyRoaringSet:
				require.NoError(t, b.RoaringSetAddOne([]byte("hello3"), uint64(3)))
			case StrategyMapCollection:
				require.NoError(t, b.MapSet([]byte("hello3"), MapPair{Key: []byte("hello3"), Value: []byte("world3")}))
			case StrategyRoaringSetRange:
				require.NoError(t, b.RoaringSetRangeAdd(uint64(3), uint64(3)))
				require.NoError(t, err)
			}
			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			clear(fileTypes)
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 0, fileTypes[".db"], "no segment file")
			require.Equal(t, 1, fileTypes[".wal"], "single wal file")

			// now add a lot of entries to hit .wal file limit
			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 4)

			for i := 4; i < 120; i++ { // larger than min .wal threshold
				switch strategy {
				case StrategyReplace:
					require.NoError(t, b.Put([]byte(fmt.Sprintf("hello%d", i)), []byte(fmt.Sprintf("world%d", i)), WithSecondaryKey(0, []byte(fmt.Sprintf("bonjour%d", i)))))
				case StrategySetCollection:
					require.NoError(t, b.SetAdd([]byte(fmt.Sprintf("hello%d", i)), [][]byte{[]byte(fmt.Sprintf("world%d", i))}))
				case StrategyRoaringSet:
					require.NoError(t, b.RoaringSetAddOne([]byte(fmt.Sprintf("hello%d", i)), uint64(i)))
				case StrategyMapCollection:
					require.NoError(t, b.MapSet([]byte(fmt.Sprintf("hello%d", i)), MapPair{Key: []byte(fmt.Sprintf("hello%d", i)), Value: []byte(fmt.Sprintf("world%d", i))}))
				case StrategyRoaringSetRange:
					require.NoError(t, b.RoaringSetRangeAdd(uint64(4), uint64(4)))
				}
			}
			testBucketContent(t, strategy, b, 120)

			require.NoError(t, b.Shutdown(ctx))

			entries, err = os.ReadDir(dirName)
			require.NoError(t, err)
			clear(fileTypes)
			for _, entry := range entries {
				fileTypes[filepath.Ext(entry.Name())] += 1
			}
			require.Equal(t, 1, fileTypes[".db"], "no segment file")
			require.Equal(t, 0, fileTypes[".wal"], "single wal file")

			b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithSecondaryIndices(secondaryIndicesCount),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)

			testBucketContent(t, strategy, b, 120)
		})
	}
}

func TestBucketRecovery(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctx := context.Background()
	dirName := t.TempDir()
	tmpDir := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(true), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1"))))
	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 0)
	require.Equal(t, walFiles, 1)

	// move .wal file somewhere else to recover later
	var oldPath, tmpPath string
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".wal" {
			oldPath = dirName + "/" + entry.Name()
			tmpPath = tmpDir + "/" + entry.Name()
			require.NoError(t, os.Rename(oldPath, tmpPath))
		}
	}
	_, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, walFiles, 0)

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(true), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("hello2"), []byte("world2"), WithSecondaryKey(0, []byte("bonjour2"))))
	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 0)
	require.Equal(t, walFiles, 1)

	// move .wal file back so we can recover one
	require.NoError(t, os.Rename(tmpPath, oldPath))

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithWriteSegmentInfoIntoFileName(true), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	get, err := b.Get([]byte("hello1"))
	require.NoError(t, err)
	require.Equal(t, []byte("world1"), get)
}

func testBucketContent(t *testing.T, strategy string, b *Bucket, maxObject int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i < maxObject; i++ {
		key := []byte(fmt.Sprintf("hello%d", i))
		val := []byte(fmt.Sprintf("world%d", i))
		switch strategy {
		case StrategyReplace:
			get, err := b.Get(key)
			require.NoError(t, err)
			require.Equal(t, val, get)

			secondary, err := b.GetBySecondary(ctx, 0, []byte(fmt.Sprintf("bonjour%d", i)))
			require.NoError(t, err)
			require.Equal(t, val, secondary)
		case StrategySetCollection:
			get, err := b.SetList(key)
			require.NoError(t, err)
			require.Equal(t, val, get[0])
		case StrategyRoaringSet:
			get, release, err := b.RoaringSetGet(key)
			require.NoError(t, err)
			defer release()
			require.True(t, get.Contains(uint64(i)))
		case StrategyRoaringSetRange:
			//_, err :=  b.Rang
			//require.NoError(t,err)
		case StrategyMapCollection:
			get, err := b.MapList(ctx, key)
			require.NoError(t, err)
			require.Equal(t, val, get[0].Value)
		}
	}
}

func TestBucketReloadAfterWalDamange(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctx := context.Background()
	dirName := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)

	require.NoError(t, b.Put([]byte("hello1"), []byte("world1"), WithSecondaryKey(0, []byte("bonjour1")), WithSecondaryKey(1, []byte("hallo1"))))
	require.NoError(t, b.Put([]byte("hello2"), []byte("world2"), WithSecondaryKey(0, []byte("bonjour2")), WithSecondaryKey(1, []byte("hallo2"))))

	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, count, 2)

	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 0)
	require.Equal(t, walFiles, 1)

	// damage .wal file
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".wal" {
			require.NoError(t, corruptBloomFileByTruncatingIt(dirName+"/"+entry.Name()))
		}
	}

	// now reload bucket
	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)

	count, err = b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// write more data
	require.NoError(t, b.Put([]byte("hello3"), []byte("world3"), WithSecondaryKey(0, []byte("bonjour3")), WithSecondaryKey(1, []byte("hallo3"))))
	require.NoError(t, b.Put([]byte("hello4"), []byte("world4"), WithSecondaryKey(0, []byte("bonjour4")), WithSecondaryKey(1, []byte("hallo4"))))
	require.NoError(t, b.Put([]byte("hello5"), []byte("world5"), WithSecondaryKey(0, []byte("bonjour5")), WithSecondaryKey(1, []byte("hallo5"))))

	count, err = b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	require.NoError(t, b.Shutdown(ctx))
	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 1)

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)

	count, err = b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	require.NoError(t, b.FlushMemtable())

	require.NoError(t, b.Shutdown(ctx))

	b, err = NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithCalcCountNetAdditions(true), WithSecondaryIndices(2), WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	count, err = b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	for i := 1; i <= 5; i++ {
		if i == 2 {
			continue // this one was lost due to the damage
		}
		val, err := b.Get([]byte(fmt.Sprintf("hello%d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("world%d", i)), val)

		secondary0, err := b.GetBySecondary(ctx, 0, []byte(fmt.Sprintf("bonjour%d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("world%d", i)), secondary0)

		secondary1, err := b.GetBySecondary(ctx, 1, []byte(fmt.Sprintf("hallo%d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("world%d", i)), secondary1)
	}
}

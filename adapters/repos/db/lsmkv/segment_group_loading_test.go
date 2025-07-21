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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompactionCleanupBothSegmentsPresent(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctx := context.Background()
	dirName := t.TempDir()
	tmpDir := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithUseBloomFilter(false), WithWriteSegmentInfoIntoFileName(true), WithMinWalThreshold(4096),
	)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("hello%d", i)), []byte(fmt.Sprintf("world%d", i))))
	}
	require.NoError(t, b.FlushMemtable())
	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 0)

	for i := 10; i < 20; i++ {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("hello%d", i)), []byte(fmt.Sprintf("world%d", i))))
	}
	require.NoError(t, b.Put([]byte("hello1"), []byte("newworld")))
	require.NoError(t, b.FlushMemtable())
	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 2)
	require.Equal(t, walFiles, 0)

	// copy segments to safe place
	var segments []string
	entriesTmp, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, entry := range entriesTmp {
		if filepath.Ext(entry.Name()) == ".db" {
			copyFile(t, dirName+"/"+entry.Name(), tmpDir+"/"+entry.Name())
			segments = append(segments, segmentID(entry.Name()))
		}
	}

	once, err := b.disk.compactOnce()
	require.NoError(t, err)
	require.True(t, once)
	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 0)
	require.NoError(t, b.Shutdown(ctx))

	// move compacted segment to safe place
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".db" && strings.Contains(entry.Name(), ".l1.") {
			require.NoError(t, os.Rename(dirName+"/"+entry.Name(), tmpDir+"/"+"segment-"+segments[0]+"_"+segments[1]+".l1.s0.db.tmp"))
		}
	}

	// order after sorting is:
	// 0: left segment
	// 1: combined segment
	// 2: right segment
	entriesTmp, err = os.ReadDir(tmpDir)
	sort.Slice(entriesTmp, func(i, j int) bool {
		return entriesTmp[i].Name() < entriesTmp[j].Name()
	})
	require.NoError(t, err)

	// Tests that various states of the compaction being aborted are handled correctly
	// There are 3 files involved:
	// 1. The combined segment that is still a tmp file
	// 2+3. The two source segment files
	tests := []struct {
		name      string
		copyLeft  bool
		copyRight bool
		expectErr bool
	}{
		{name: "only left present", copyLeft: true, copyRight: false, expectErr: true},
		{name: "only right present", copyLeft: false, copyRight: true, expectErr: false},
		{name: "nothing present", copyLeft: false, copyRight: false, expectErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			if tt.copyLeft {
				copyFile(t, tmpDir+"/"+entriesTmp[0].Name(), testDir+"/"+entriesTmp[0].Name())
			}
			if tt.copyRight {
				copyFile(t, tmpDir+"/"+entriesTmp[2].Name(), testDir+"/"+entriesTmp[2].Name())
			}
			// always copy the combined file
			copyFile(t, tmpDir+"/"+entriesTmp[1].Name(), testDir+"/"+entriesTmp[1].Name())

			b2, err := NewBucketCreator().NewBucket(ctx, testDir, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithUseBloomFilter(false), WithWriteSegmentInfoIntoFileName(true), WithCalcCountNetAdditions(true),
			)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 20, b2.Count())
			}
		})
	}
}

func copyFile(t *testing.T, src, dest string) {
	t.Helper()
	target, err := os.Create(dest)
	require.NoError(t, err)

	source, err := os.Open(src)
	require.NoError(t, err)

	_, err = io.Copy(target, source)
	require.NoError(t, err)
	require.NoError(t, source.Sync())
	require.NoError(t, source.Close())
	require.NoError(t, target.Sync())
	require.NoError(t, target.Close())
}

func TestWalFilePresent(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctx := context.Background()
	dirName := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithUseBloomFilter(false), WithWriteSegmentInfoIntoFileName(true), WithMinWalThreshold(4096),
	)

	// create "incomplete" segment
	require.NoError(t, err)
	require.NoError(t, b.Put([]byte("hello0"), []byte("world0")))
	require.NoError(t, b.Put([]byte("hello1"), []byte("world1")))
	require.NoError(t, b.FlushMemtable())

	// create wal file with more entries
	require.NoError(t, b.Put([]byte("hello0"), []byte("world0")))
	require.NoError(t, b.Put([]byte("hello1"), []byte("world1")))
	require.NoError(t, b.Put([]byte("hello2"), []byte("world2")))
	require.NoError(t, b.Shutdown(ctx))

	dbFiles, walFiles := countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 1)
	require.Equal(t, walFiles, 1)

	// .wal file needs same (base)name as segment file
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	var segmentId string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".db" {
			segmentId = segmentID(entry.Name())
		}
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".wal" {
			require.NoError(t, os.Rename(dirName+"/"+entry.Name(), dirName+"/"+"segment-"+segmentId+".wal"))
		}
	}

	// incomplete segment will be deleted and memtable is reconstructed from .wal
	b2, err := NewBucketCreator().NewBucket(ctx, dirName, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithUseBloomFilter(false), WithWriteSegmentInfoIntoFileName(true), WithMinWalThreshold(4096),
	)
	require.NoError(t, err)

	val, err := b2.Get([]byte("hello2"))
	require.NoError(t, err)
	require.Equal(t, string(val), "world2")

	dbFiles, walFiles = countDbAndWalFiles(t, dirName)
	require.Equal(t, dbFiles, 0)
	require.Equal(t, walFiles, 1)
}

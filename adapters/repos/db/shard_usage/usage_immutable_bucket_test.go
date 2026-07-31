//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package shardusage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// Reporting usage opens the dimensions bucket of an unloaded tenant. That must stay a read:
// the tenant is not ours to write to, and a write-ahead-log left there by the last shutdown
// would otherwise be fsynced, flushed into a new segment or deleted on every collection.
func TestCalculateUnloadedDimensionsUsageDoesNotWrite(t *testing.T) {
	const (
		tenantName   = "tenant"
		targetVector = "text"
		dims         = 128
	)

	tests := []struct {
		name string
		// docIDs is how many documents get their dimensions recorded; 0 leaves the bucket
		// directory uncreated
		docIDs int
		// flush moves the dimensions out of the write-ahead-log into a segment
		flush bool
		// minWALSize is the smallest write-ahead-log the seeded bucket may leave
		// behind; 0 expects none
		minWALSize int64
		// residue adds the leftovers of a crash to the seeded bucket directory
		residue func(t *testing.T, bucketPath string)
	}{
		{name: "dimensions flushed to a segment", docIDs: 3, flush: true},
		{name: "dimensions in a small write-ahead-log", docIDs: 3, minWALSize: 1},
		{
			// above this size the log is flushed into a new segment instead of reused
			name:       "dimensions in a write-ahead-log over 4KB",
			docIDs:     2000,
			minWALSize: 4097,
		},
		{name: "no dimensions bucket at all"},
		{
			// a crash between writing a segment and removing the log it was written from
			// leaves a segment that may be incomplete, so the log is what counts
			name:       "partially written segment next to its write-ahead-log",
			docIDs:     3,
			minWALSize: 1,
			residue:    writePartialSegmentForWAL,
		},
		{
			name:    "leftovers of an interrupted segment cleanup next to a flushed segment",
			docIDs:  3,
			flush:   true,
			residue: writeCleanupLeftovers,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			ctx := context.Background()
			indexPath := t.TempDir()
			bucketPath := shardPathDimensionsLSM(indexPath, tenantName)

			want := types.Dimensionality{}
			if tt.docIDs > 0 {
				seedDimensionsBucket(t, bucketPath, targetVector, dims, tt.docIDs, tt.flush)
				want = types.Dimensionality{Dimensions: dims, Count: tt.docIDs}
			}
			if tt.residue != nil {
				tt.residue(t, bucketPath)
			}

			walSize := totalWALSize(t, bucketPath)
			if tt.minWALSize == 0 {
				require.Zero(t, walSize, "the case only holds without a write-ahead-log")
			} else {
				require.GreaterOrEqual(t, walSize, tt.minWALSize,
					"the case only holds with a write-ahead-log of at least that size")
			}

			before := dirSnapshot(t, bucketPath)

			got, err := CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, tenantName, targetVector)
			require.NoError(t, err)
			require.Equal(t, want, got)

			require.Equal(t, before, dirSnapshot(t, bucketPath),
				"reporting usage must not add, change or remove a file")
		})
	}
}

// seedDimensionsBucket records the dimensions of docIDs documents and shuts the bucket down
// again. Without flush the entries stay in the write-ahead-log.
func seedDimensionsBucket(t *testing.T, bucketPath, targetVector string, dims uint32, docIDs int, flush bool) {
	t.Helper()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	// a reuse threshold no log can reach leaves the log behind on shutdown, whatever its size
	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, bucketPath, "", logger, nil, noopCB, noopCB,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet), lsmkv.WithMinWalThreshold(1<<40))
	require.NoError(t, err)

	key := make([]byte, len(targetVector)+4)
	copy(key, targetVector)
	binary.LittleEndian.PutUint32(key[len(targetVector):], dims)
	for docID := 1; docID <= docIDs; docID++ {
		require.NoError(t, b.RoaringSetAddOne(key, uint64(docID)))
	}

	if flush {
		require.NoError(t, b.FlushMemtable())
	}
	require.NoError(t, b.Shutdown(ctx))
}

// writePartialSegmentForWAL puts a segment file next to the write-ahead-log it would have
// been written from, holding too few bytes to be readable.
func writePartialSegmentForWAL(t *testing.T, bucketPath string) {
	t.Helper()

	logs, err := filepath.Glob(filepath.Join(bucketPath, "*.wal"))
	require.NoError(t, err)
	require.Len(t, logs, 1)

	segment := strings.TrimSuffix(logs[0], ".wal") + ".db"
	require.NoError(t, os.WriteFile(segment, []byte("partially written"), 0o600))
}

// writeCleanupLeftovers adds what a crash in the middle of a segment cleanup leaves behind:
// the rewritten segment, a bloom filter marked for deletion and a scratch directory.
func writeCleanupLeftovers(t *testing.T, bucketPath string) {
	t.Helper()

	segments, err := filepath.Glob(filepath.Join(bucketPath, "*.db"))
	require.NoError(t, err)
	require.Len(t, segments, 1)

	contents, err := os.ReadFile(segments[0])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(segments[0]+".tmp", contents, 0o600))

	bloom := strings.TrimSuffix(segments[0], ".db") + ".bloom.0000000000000" + lsmkv.DeleteMarkerSuffix
	require.NoError(t, os.WriteFile(bloom, []byte("marked for deletion"), 0o600))

	require.NoError(t, os.Mkdir(filepath.Join(bucketPath, "segment-x.scratch.d"), 0o700))
}

func totalWALSize(t *testing.T, dir string) int64 {
	t.Helper()

	names, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)

	total := int64(0)
	for _, name := range names {
		info, err := os.Stat(name)
		require.NoError(t, err)
		total += info.Size()
	}
	return total
}

// dirSnapshot maps every file in dir to a hash of its contents and every subdirectory to a
// fixed marker. A directory that does not exist maps to nil.
func dirSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)

	snapshot := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() {
			snapshot[entry.Name()] = "directory"
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		snapshot[entry.Name()] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return snapshot
}

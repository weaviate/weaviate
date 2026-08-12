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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// A bucket can be deleted between the lsm directory listing and the size calculation, e.g.
// while a property index is being dropped. The vanished bucket must be skipped so the rest
// of the shard is still counted.
func TestUnloadedSizesSkipVanishedBucket(t *testing.T) {
	const bucketSizeBytes = 1000

	tests := []struct {
		name string
		// calc is the function under test, over the lsm path and the bucket names.
		calc func(lsmPath string, directories []string) (uint64, error)
		// present buckets are created on disk, vanished ones only appear in the listing.
		present  []string
		vanished []string
		want     uint64
	}{
		{
			name:    "vectors, all buckets present",
			calc:    vectorsSize,
			present: []string{"vectors", "vectors_compressed"},
			want:    2 * bucketSizeBytes,
		},
		{
			name:     "vectors, one bucket vanished",
			calc:     vectorsSize,
			present:  []string{"vectors"},
			vanished: []string{"vectors_compressed"},
			want:     bucketSizeBytes,
		},
		{
			name:     "vectors, every bucket vanished",
			calc:     vectorsSize,
			vanished: []string{"vectors", "vectors_compressed"},
			want:     0,
		},
		{
			name:    "indices, all buckets present",
			calc:    CalculateUnloadedIndicesSize,
			present: []string{helpers.BucketFromPropNameLSM("title"), helpers.DimensionsBucketLSM},
			want:    2 * bucketSizeBytes,
		},
		{
			name:     "indices, one bucket vanished",
			calc:     CalculateUnloadedIndicesSize,
			present:  []string{helpers.BucketFromPropNameLSM("title")},
			vanished: []string{helpers.BucketFromPropNameLSM("author")},
			want:     bucketSizeBytes,
		},
		{
			name:     "indices, every bucket vanished",
			calc:     CalculateUnloadedIndicesSize,
			vanished: []string{helpers.BucketFromPropNameLSM("title"), helpers.DimensionsBucketLSM},
			want:     0,
		},
		{
			name:     "vanished bucket the filter ignores is not read at all",
			calc:     vectorsSize,
			present:  []string{"vectors"},
			vanished: []string{helpers.BucketFromPropNameLSM("title")},
			want:     bucketSizeBytes,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := filepath.Join(t.TempDir(), "lsm")
			require.NoError(t, os.MkdirAll(lsmPath, 0o700))

			for _, bucket := range tc.present {
				bucketPath := filepath.Join(lsmPath, bucket)
				require.NoError(t, os.Mkdir(bucketPath, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(bucketPath, "segment-1.db"),
					make([]byte, bucketSizeBytes), 0o600))
			}

			// The listing names every bucket, including the ones no longer on disk.
			directories := append(append([]string{}, tc.present...), tc.vanished...)

			got, err := tc.calc(lsmPath, directories)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// vectorsSize adapts CalculateUnloadedVectorsMetrics to the table's uint64 signature.
func vectorsSize(lsmPath string, directories []string) (uint64, error) {
	size, err := CalculateUnloadedVectorsMetrics(lsmPath, directories)
	return uint64(size), err
}

// writeCountNetAdditions writes a .cna file: a little-endian CRC32 of the
// payload followed by the count.
func writeCountNetAdditions(t *testing.T, path string, count int64) {
	t.Helper()
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[4:], uint64(count))
	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[4:]))
	require.NoError(t, os.WriteFile(path, buf, 0o600))
}

// A segment's sidecar can be deleted between the objects directory listing and
// the count read — a load recovering the write-ahead log, or a compaction
// retiring the segment. Counting the shard without it beats failing the whole
// count, which reports the shard as empty. Any other read error still surfaces.
func TestUnloadedObjectsMetricsSkipVanishedSidecar(t *testing.T) {
	tests := []struct {
		name string
		// counts are written as readable .cna files, one per entry.
		counts []int64
		// vanished sidecars are listed by the directory read but resolve to nothing.
		vanished int
		corrupt  bool
		want     int64
		wantErr  bool
	}{
		{name: "all sidecars present", counts: []int64{3, 4}, want: 7},
		{name: "one sidecar vanished", counts: []int64{3, 4}, vanished: 1, want: 7},
		{name: "every sidecar vanished", vanished: 2},
		{name: "corrupt sidecar still fails", counts: []int64{3}, corrupt: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			indexPath := t.TempDir()
			objectStore := shardPathObjectsLSM(indexPath, "shard1")
			require.NoError(t, os.MkdirAll(objectStore, 0o700))

			for i, count := range tc.counts {
				writeCountNetAdditions(t, filepath.Join(objectStore,
					fmt.Sprintf("segment-present-%d.cna", i)), count)
			}
			// A dangling symlink is listed by Readdir but cannot be opened, which is
			// what a reader sees when the segment is retired mid-scan.
			for i := 0; i < tc.vanished; i++ {
				require.NoError(t, os.Symlink(filepath.Join(objectStore, "gone.db"),
					filepath.Join(objectStore, fmt.Sprintf("segment-vanished-%d.cna", i))))
			}
			if tc.corrupt {
				require.NoError(t, os.WriteFile(filepath.Join(objectStore, "segment-corrupt.cna"),
					make([]byte, 12), 0o600))
			}

			usage, err := CalculateUnloadedObjectsMetrics(logrus.New(), indexPath, "shard1", true)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, usage.Count)
		})
	}
}

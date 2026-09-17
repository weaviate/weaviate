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
	"github.com/weaviate/weaviate/entities/diskio"
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
	metrics, err := CalculateUnloadedVectorsMetrics(lsmPath, directories)
	return uint64(metrics.StorageBytes), err
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

// A segment's sidecar can be deleted between the objects directory listing and the count read,
// by a load recovering the write-ahead log or a compaction retiring the segment. Counting the
// shard without it beats failing the whole count, which reports the shard as empty. Any other
// failure to read the store still surfaces.
func TestUnloadedObjectsMetricsSkipVanishedSidecar(t *testing.T) {
	tests := []struct {
		name string
		// denyStore takes read access from the objects directory before the count.
		denyStore bool
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
		{name: "unreadable objects directory still fails", denyStore: true, wantErr: true},
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
			if tc.denyStore {
				denyRead(t, objectStore)
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

// denyRead takes read access away from dirPath, so opening it fails with an error that is
// not fs.ErrNotExist.
func denyRead(t *testing.T, dirPath string) {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("chmod-based permission denial is a no-op for root")
	}
	require.NoError(t, os.Chmod(dirPath, 0o000))
	t.Cleanup(func() { _ = os.Chmod(dirPath, 0o700) })
}

// A vector index directory can be deleted between the shard root listing and the walk that reads
// it, e.g. while a named vector is being dropped. Only that directory drops out of the total, and
// any other failure to open one still surfaces.
func TestNonLSMStorageSkipVanishedSubdir(t *testing.T) {
	// These sum the fixture below, in the two buckets the walk reports.
	const (
		wantCommitLogs = 1000 + 3000 // main.hnsw.commitlog.d + main.hfresh.d/sub.queue.d
		wantOther      = 2000 + 4000 // other_dir + the loose file in main.hfresh.d
	)

	tests := []struct {
		name string
		// remove is deleted after the root listing, as a drop racing the walk does.
		remove string
		// deny takes read access instead of deleting, for the errors that are not a vanish.
		deny           string
		wantCommitLogs uint64
		wantOther      uint64
		wantErr        bool
	}{
		{
			name:           "every subdirectory present",
			wantCommitLogs: wantCommitLogs,
			wantOther:      wantOther,
		},
		{
			name:           "commit log directory vanished",
			remove:         "main.hnsw.commitlog.d",
			wantCommitLogs: wantCommitLogs - 1000,
			wantOther:      wantOther,
		},
		{
			name:           "plain directory vanished",
			remove:         "other_dir",
			wantCommitLogs: wantCommitLogs,
			wantOther:      wantOther - 2000,
		},
		{
			name:           "hfresh directory vanished with its subdirectories",
			remove:         "main.hfresh.d",
			wantCommitLogs: wantCommitLogs - 3000,
			wantOther:      wantOther - 4000,
		},
		{name: "unreadable commit log directory still fails", deny: "main.hnsw.commitlog.d", wantErr: true},
		{name: "unreadable hfresh subdirectory still fails", deny: "main.hfresh.d/sub.queue.d", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shardPath := filepath.Join(t.TempDir(), "shard1")
			writeSizedFile(t, filepath.Join(shardPath, "main.hnsw.commitlog.d", "f"), 1000)
			writeSizedFile(t, filepath.Join(shardPath, "other_dir", "f"), 2000)
			writeSizedFile(t, filepath.Join(shardPath, "main.hfresh.d", "sub.queue.d", "f"), 3000)
			writeSizedFile(t, filepath.Join(shardPath, "main.hfresh.d", "f"), 4000)

			files, dirs, err := diskio.GetFileWithSizes(shardPath)
			require.NoError(t, err)

			// the listing is already taken, so the walk below meets the shard mid-drop
			if tc.remove != "" {
				require.NoError(t, os.RemoveAll(filepath.Join(shardPath, tc.remove)))
			}
			if tc.deny != "" {
				denyRead(t, filepath.Join(shardPath, tc.deny))
			}

			commitLogs, other, err := nonLSMStorage(shardPath, files, dirs)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantCommitLogs, commitLogs)
			require.Equal(t, tc.wantOther, other)
		})
	}
}

// writeSizedFile writes a file of the given size, creating its parent directories.
func writeSizedFile(t *testing.T, path string, size int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
}

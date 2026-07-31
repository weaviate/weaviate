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
	"os"
	"path/filepath"
	"testing"

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

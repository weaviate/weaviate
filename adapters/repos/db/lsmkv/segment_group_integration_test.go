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

//go:build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestSegmentGroup_DropAwaiting(t *testing.T) {
	ctx := context.Background()

	key := func(id int) []byte {
		return []byte(fmt.Sprintf("key-%d", id))
	}
	seckey := func(id int) []byte {
		return []byte(fmt.Sprintf("seckey-%d", id))
	}
	val := func(id int) []byte {
		return []byte(fmt.Sprintf("val-%d", id))
	}
	createBucket := func(t *testing.T) *Bucket {
		dirName := t.TempDir()

		bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace),
			WithSecondaryIndices(1),
			WithWriteSegmentInfoIntoFileName(true),
		)
		require.Nil(t, err)

		return bucket
	}
	createSegment := func(t *testing.T, bucket *Bucket, id int) {
		t.Helper()

		require.NoError(t, bucket.Put(key(id), val(id), WithSecondaryKey(0, seckey(id))))
		require.NoError(t, bucket.FlushAndSwitch())
	}
	extractFiles := func(t *testing.T, dirName, ext string) []string {
		t.Helper()

		dbFiles := []string{}
		entries, err := os.ReadDir(dirName)
		require.NoError(t, err)

		for _, entry := range entries {
			if !entry.IsDir() {
				if filepath.Ext(entry.Name()) == ext {
					dbFiles = append(dbFiles, entry.Name())
				}
			}
		}
		return dbFiles
	}
	assertDataExists := func(t *testing.T, bucket *Bucket, count int) {
		t.Helper()

		for i := range count {
			value, err := bucket.Get(key(i))
			require.NoError(t, err)
			require.Equal(t, val(i), value)

			value, err = bucket.GetBySecondary(context.Background(), 0, seckey(i))
			require.NoError(t, err)
			require.Equal(t, val(i), value)
		}
	}

	segments := 4
	// s0[0], s1[0], s2[0], s3[0]
	// s01[1], s2[0], s3[0]
	// s01[1], s23[1]
	// s0123[2]
	expectedCompactions := 3
	// (db + prim bloom + sec bloom)
	expectedDeleteMePerSegment := 3

	t.Run("marked files are named after original segments", func(t *testing.T) {
		bucket := createBucket(t)
		t.Cleanup(func() {
			require.NoError(t, bucket.Shutdown(ctx))
		})

		var seg1Id string
		var seg2Id string
		var seg3Id string
		var seg4Id string
		var seg12Id string
		var seg34Id string

		t.Run("populate", func(t *testing.T) {
			for i := range segments {
				createSegment(t, bucket, i)
			}

			// segments are created
			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments)

			seg1Id = segmentID(dbFiles[0])
			seg2Id = segmentID(dbFiles[1])
			seg3Id = segmentID(dbFiles[2])
			seg4Id = segmentID(dbFiles[3])
		})

		t.Run("1st compaction", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 2*expectedDeleteMePerSegment)

			for i := range expectedDeleteMePerSegment {
				assert.Equal(t, seg1Id, segmentID(deletemeFiles[i]))
			}
			for i := expectedDeleteMePerSegment; i < 2*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg2Id, segmentID(deletemeFiles[i]))
			}

			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments-1)
		})

		t.Run("2nd compaction", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 4*expectedDeleteMePerSegment)

			for i := range expectedDeleteMePerSegment {
				assert.Equal(t, seg1Id, segmentID(deletemeFiles[i]))
			}
			for i := expectedDeleteMePerSegment; i < 2*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg2Id, segmentID(deletemeFiles[i]))
			}
			for i := 2 * expectedDeleteMePerSegment; i < 3*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg3Id, segmentID(deletemeFiles[i]))
			}
			for i := 3 * expectedDeleteMePerSegment; i < 4*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg4Id, segmentID(deletemeFiles[i]))
			}

			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments-2)

			seg12Id = segmentID(dbFiles[0])
			seg34Id = segmentID(dbFiles[1])
		})

		t.Run("3rd compaction", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 6*expectedDeleteMePerSegment)

			for i := range expectedDeleteMePerSegment {
				assert.Equal(t, seg1Id, segmentID(deletemeFiles[i]))
			}
			for i := expectedDeleteMePerSegment; i < 2*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg2Id, segmentID(deletemeFiles[i]))
			}
			for i := 2 * expectedDeleteMePerSegment; i < 3*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg12Id, segmentID(deletemeFiles[i]))
			}
			for i := 3 * expectedDeleteMePerSegment; i < 4*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg3Id, segmentID(deletemeFiles[i]))
			}
			for i := 4 * expectedDeleteMePerSegment; i < 5*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg4Id, segmentID(deletemeFiles[i]))
			}
			for i := 5 * expectedDeleteMePerSegment; i < 6*expectedDeleteMePerSegment; i++ {
				assert.Equal(t, seg34Id, segmentID(deletemeFiles[i]))
			}

			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments-3)
		})
	})

	t.Run("marked files are dropped after compaction", func(t *testing.T) {
		bucket := createBucket(t)
		t.Cleanup(func() {
			require.NoError(t, bucket.Shutdown(ctx))
		})

		t.Run("populate", func(t *testing.T) {
			for i := range segments {
				createSegment(t, bucket, i)
			}

			// segments are created
			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments)
		})

		t.Run("deleteme files are created after compaction", func(t *testing.T) {
			compactions := 0
			for {
				compacted, err := bucket.disk.compactOnce()
				require.NoError(t, err)

				if !compacted {
					// deleteme files do not exist
					deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
					require.Empty(t, deletemeFiles)
					break
				}
				compactions++

				// deleteme files exist
				deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
				require.Len(t, deletemeFiles, 2*expectedDeleteMePerSegment)

				// drop segments awaiting
				dropped, err := bucket.disk.dropSegmentsAwaiting()
				require.NoError(t, err)
				require.Equal(t, 2, dropped)

				// deleteme files do not exist
				deletemeFiles = extractFiles(t, bucket.dir, ".deleteme")
				require.Empty(t, deletemeFiles)

				assertDataExists(t, bucket, segments)
			}
			require.Equal(t, expectedCompactions, compactions)
		})
	})

	t.Run("undropped marked files are dropped on shutdown", func(t *testing.T) {
		bucket := createBucket(t)

		t.Run("populate", func(t *testing.T) {
			for i := range segments {
				createSegment(t, bucket, i)
			}

			// segments are created
			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments)
		})

		t.Run("compact", func(t *testing.T) {
			compactions := 0
			for {
				compacted, err := bucket.disk.compactOnce()
				require.NoError(t, err)

				if !compacted {
					break
				}
				compactions++
			}
			require.Equal(t, 3, compactions)
		})

		t.Run("deleteme files exist", func(t *testing.T) {
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, expectedCompactions*2*expectedDeleteMePerSegment)

			assertDataExists(t, bucket, segments)
			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, 1)
		})

		t.Run("deleteme files do not exist after shutdown", func(t *testing.T) {
			require.NoError(t, bucket.Shutdown(ctx))

			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Empty(t, deletemeFiles)

			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, 1)
		})
	})

	t.Run("marked files with refs are not dropped", func(t *testing.T) {
		bucket := createBucket(t)

		t.Run("populate", func(t *testing.T) {
			for i := range segments {
				createSegment(t, bucket, i)
			}

			// segments are created
			dbFiles := extractFiles(t, bucket.dir, ".db")
			require.Len(t, dbFiles, segments)
		})

		cur_1_2_3_4 := bucket.Cursor()

		t.Run("deleteme files are not dropped due to active cursor", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 2*expectedDeleteMePerSegment)

			// cur_1_2_3_4 holds segments 1,2,3,4. marked 1 and 2 can not be dropped

			// drop segments awaiting
			dropped, err := bucket.disk.dropSegmentsAwaiting()
			require.NoError(t, err)
			require.Equal(t, 0, dropped)

			// deleteme files still exist
			deletemeFiles = extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 2*expectedDeleteMePerSegment)
		})

		cur_12_3_4 := bucket.Cursor()

		t.Run("deleteme files are not dropped due to active cursor", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 4*expectedDeleteMePerSegment)

			// cur_1_2_3_4 holds segments 1,2,3,4. marked 1 and 2 can not be dropped
			// cur_12_3_4 holds segments 12, 3, 4. marked 3 and 4 can not be dropped

			// drop segments awaiting
			dropped, err := bucket.disk.dropSegmentsAwaiting()
			require.NoError(t, err)
			require.Equal(t, 0, dropped)

			// deleteme files still exist
			deletemeFiles = extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 4*expectedDeleteMePerSegment)
		})

		cur_12_34 := bucket.Cursor()
		cur_1_2_3_4.Close()

		t.Run("deleteme files are not dropped due to active cursor", func(t *testing.T) {
			compacted, err := bucket.disk.compactOnce()
			require.NoError(t, err)
			require.True(t, compacted)

			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 6*expectedDeleteMePerSegment)

			// cur_1_2_3_4 released segments 1,2,3,4. marked 1 and 2 can be dropped now
			// cur_12_3_4 holds segments 12, 3, 4. marked 3 and 4 can not be dropped
			// cur_12_34 holds segments 12, 34. marked 12 and 34 can not be dropped

			// drop segments awaiting
			dropped, err := bucket.disk.dropSegmentsAwaiting()
			require.NoError(t, err)
			require.Equal(t, 2, dropped)

			// deleteme files still exist
			deletemeFiles = extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 4*expectedDeleteMePerSegment)
		})

		cur_12_3_4.Close()
		cur_12_34.Close()

		t.Run("deleteme files are dropped due to no refs", func(t *testing.T) {
			// deleteme files exist
			deletemeFiles := extractFiles(t, bucket.dir, ".deleteme")
			require.Len(t, deletemeFiles, 4*expectedDeleteMePerSegment)

			// cur_12_3_4 released segments 12, 3, 4. marked 3 and 4 can be dropped
			// cur_12_34 released segments 12, 34. marked 12 and 34 can be dropped

			// drop segments awaiting
			dropped, err := bucket.disk.dropSegmentsAwaiting()
			require.NoError(t, err)
			require.Equal(t, 4, dropped)

			// deleteme files do not exist
			deletemeFiles = extractFiles(t, bucket.dir, ".deleteme")
			require.Empty(t, deletemeFiles)
		})
	})
}

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
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestSegmentGroup_CleanupCandidates(t *testing.T) {
	assertSegment := func(t *testing.T, sg *SegmentGroup, idx int, expectedName string, expectedSize int64) {
		seg := sg.segments[idx]
		assert.Equal(t, filepath.Join(sg.dir, expectedName), seg.path)
		assert.Equal(t, expectedSize, seg.size)
	}
	requireCandidateFound := func(t *testing.T, idx, expectedIdx, startIdx, expectedStartIdx int) {
		require.Equal(t, expectedIdx, idx)
		require.Equal(t, expectedStartIdx, startIdx)
	}
	requireCandidateNotFound := func(t *testing.T, idx, startIdx int) {
		require.Equal(t, noIdx, idx)
		require.Equal(t, noIdx, startIdx)
	}
	assertBoltDbKeys := func(t *testing.T, db *bolt.DB, expectedIds []int64) {
		ids := make([]int64, 0, len(expectedIds))

		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupSegmentsBucket)
			c := b.Cursor()

			for ck, _ := c.First(); ck != nil; ck, _ = c.Next() {
				ids = append(ids, int64(binary.BigEndian.Uint64(ck)))
			}
			return nil
		})

		assert.ElementsMatch(t, expectedIds, ids)
	}

	t.Run("no segments", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir:             dir,
			segments:        []*segment{},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		idx, startIdx, onCompleted, err := sc.findCandidate()
		require.NoError(t, err)
		requireCandidateNotFound(t, idx, startIdx)
		assert.Nil(t, onCompleted)
		assertBoltDbKeys(t, sc.db, []int64{})
	})

	t.Run("single segment", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		idx, startIdx, onCompleted, err := sc.findCandidate()
		require.NoError(t, err)
		requireCandidateNotFound(t, idx, startIdx)
		assert.Nil(t, onCompleted)
		assertBoltDbKeys(t, sc.db, []int64{})
	})

	t.Run("multilpe segments, segments in order oldest to newest, last one skipped", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
				{
					path: filepath.Join(dir, "segment-0002.db"),
					size: 10002,
				},
				{
					path: filepath.Join(dir, "segment-0003.db"),
					size: 10003,
				},
				{
					path: filepath.Join(dir, "segment-0004.db"),
					size: 10004,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		sc.init()
		defer sc.close()

		idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
		require.NoError(t, err1)
		requireCandidateFound(t, idx1, 0, startIdx1, 1)
		assertSegment(t, sg, idx1, "segment-0001.db", 10001)
		require.NotNil(t, onCompleted1)
		onCompleted1(9001)
		sg.segments[idx1].size = 9001

		idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
		require.NoError(t, err2)
		requireCandidateFound(t, idx2, 1, startIdx2, 2)
		assertSegment(t, sg, idx2, "segment-0002.db", 10002)
		require.NotNil(t, onCompleted2)
		onCompleted2(9002)
		sg.segments[idx2].size = 9002

		idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
		require.NoError(t, err3)
		requireCandidateFound(t, idx3, 2, startIdx3, 3)
		assertSegment(t, sg, idx3, "segment-0003.db", 10003)
		require.NotNil(t, onCompleted3)
		onCompleted3(9003)
		sg.segments[idx3].size = 9003

		idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
		require.NoError(t, err4)
		requireCandidateNotFound(t, idx4, startIdx4)
		assert.Nil(t, onCompleted4)

		assertBoltDbKeys(t, sc.db, []int64{1, 2, 3})
	})

	t.Run("multilpe segments, no candidates after interval if no new segments", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
				{
					path: filepath.Join(dir, "segment-0002.db"),
					size: 10002,
				},
				{
					path: filepath.Join(dir, "segment-0003.db"),
					size: 10003,
				},
				{
					path: filepath.Join(dir, "segment-0004.db"),
					size: 10004,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		t.Run("1st round, all but last cleaned", func(t *testing.T) {
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 0, startIdx1, 1)
			assertSegment(t, sg, idx1, "segment-0001.db", 10001)
			require.NotNil(t, onCompleted1)
			onCompleted1(9001)
			sg.segments[idx1].size = 9001

			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0002.db", 10002)
			require.NotNil(t, onCompleted2)
			onCompleted2(9002)
			sg.segments[idx2].size = 9002

			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 2, startIdx3, 3)
			assertSegment(t, sg, idx3, "segment-0003.db", 10003)
			require.NotNil(t, onCompleted3)
			onCompleted3(9003)
			sg.segments[idx3].size = 9003

			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateNotFound(t, idx4, startIdx4)
			assert.Nil(t, onCompleted4)
		})

		t.Run("no candidates before interval", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("2nd round, no candiates due to no new segments", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		assertBoltDbKeys(t, sc.db, []int64{1, 2, 3})
	})

	t.Run("multilpe segments, candidates after interval if new segments created", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
				{
					path: filepath.Join(dir, "segment-0002.db"),
					size: 10002,
				},
				{
					path: filepath.Join(dir, "segment-0003.db"),
					size: 10003,
				},
				{
					path: filepath.Join(dir, "segment-0004.db"),
					size: 10004,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		t.Run("1st round, all but last cleaned", func(t *testing.T) {
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 0, startIdx1, 1)
			assertSegment(t, sg, idx1, "segment-0001.db", 10001)
			require.NotNil(t, onCompleted1)
			onCompleted1(9001)
			sg.segments[idx1].size = 9001

			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0002.db", 10002)
			require.NotNil(t, onCompleted2)
			onCompleted2(9002)
			sg.segments[idx2].size = 9002

			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 2, startIdx3, 3)
			assertSegment(t, sg, idx3, "segment-0003.db", 10003)
			require.NotNil(t, onCompleted3)
			onCompleted3(9003)
			sg.segments[idx3].size = 9003

			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateNotFound(t, idx4, startIdx4)
			assert.Nil(t, onCompleted4)
		})

		t.Run("no candidates before interval", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("new segments created", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0005.db"),
					size: 10005,
				},
				&segment{
					path: filepath.Join(dir, "segment-0006.db"),
					size: 10006,
				},
			)
		})

		t.Run("2nd round, new candidates then same candiates again", func(t *testing.T) {
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 3, startIdx1, 4)
			assertSegment(t, sg, idx1, "segment-0004.db", 10004)
			require.NotNil(t, onCompleted1)
			onCompleted1(9004)
			sg.segments[idx1].size = 9004

			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 4, startIdx2, 5)
			assertSegment(t, sg, idx2, "segment-0005.db", 10005)
			require.NotNil(t, onCompleted2)
			onCompleted2(9005)
			sg.segments[idx2].size = 9005

			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 0, startIdx3, 4)
			assertSegment(t, sg, idx3, "segment-0001.db", 9001)
			require.NotNil(t, onCompleted3)
			onCompleted3(8001)
			sg.segments[idx3].size = 8001

			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateFound(t, idx4, 1, startIdx4, 4)
			assertSegment(t, sg, idx4, "segment-0002.db", 9002)
			require.NotNil(t, onCompleted4)
			onCompleted4(8002)
			sg.segments[idx4].size = 8002

			idx5, startIdx5, onCompleted5, err5 := sc.findCandidate()
			require.NoError(t, err5)
			requireCandidateFound(t, idx5, 2, startIdx5, 4)
			assertSegment(t, sg, idx5, "segment-0003.db", 9003)
			require.NotNil(t, onCompleted5)
			onCompleted5(8003)
			sg.segments[idx5].size = 8003

			idx6, startIdx6, onCompleted6, err6 := sc.findCandidate()
			require.NoError(t, err6)
			requireCandidateNotFound(t, idx6, startIdx6)
			assert.Nil(t, onCompleted6)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("3rd round, no candidates due to no new segments", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		assertBoltDbKeys(t, sc.db, []int64{1, 2, 3, 4, 5})
	})

	t.Run("multilpe segments, candidates after interval dependant on new segments sizes", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
				{
					path: filepath.Join(dir, "segment-0002.db"),
					size: 10002,
				},
				{
					path: filepath.Join(dir, "segment-0003.db"),
					size: 10003,
				},
				{
					path: filepath.Join(dir, "segment-0004.db"),
					size: 10004,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		t.Run("1st round, all but last cleaned", func(t *testing.T) {
			// not cleaned before, cleaning considering 2+3+4
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 0, startIdx1, 1)
			assertSegment(t, sg, idx1, "segment-0001.db", 10001)
			require.NotNil(t, onCompleted1)
			onCompleted1(9001)
			sg.segments[idx1].size = 9001

			// not cleaned before, cleaning considering 3+4
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0002.db", 10002)
			require.NotNil(t, onCompleted2)
			onCompleted2(9002)
			sg.segments[idx2].size = 9002

			// not cleaned before, cleaning considering 4
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 2, startIdx3, 3)
			assertSegment(t, sg, idx3, "segment-0003.db", 10003)
			require.NotNil(t, onCompleted3)
			onCompleted3(9003)
			sg.segments[idx3].size = 9003

			// skipping 4 as last one
			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateNotFound(t, idx4, startIdx4)
			assert.Nil(t, onCompleted4)
		})

		t.Run("no candidates before interval", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("new segments created", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0005.db"),
					size: 405,
				},
				&segment{
					path: filepath.Join(dir, "segment-0006.db"),
					size: 406,
				},
			)
		})

		t.Run("2nd round, only new candidates due to sum of new sizes not big enough", func(t *testing.T) {
			// not cleaned before, cleaning considering 5+6
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 3, startIdx1, 4)
			assertSegment(t, sg, idx1, "segment-0004.db", 10004)
			require.NotNil(t, onCompleted1)
			onCompleted1(9004)
			sg.segments[idx1].size = 9004

			// not cleaned before, cleaning considering 6
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 4, startIdx2, 5)
			assertSegment(t, sg, idx2, "segment-0005.db", 405)
			require.NotNil(t, onCompleted2)
			onCompleted2(305)
			sg.segments[idx2].size = 305

			// skipping 6 as last one
			// skipping 1,2,3 due to sum of new sizes (5+6) not big enough compared to old segments' sizes
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateNotFound(t, idx3, startIdx3)
			assert.Nil(t, onCompleted3)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("3rd round, no candidates due to no new segments", func(t *testing.T) {
			// no changes in segments
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("new segments created", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0007.db"),
					size: 407,
				},
				&segment{
					path: filepath.Join(dir, "segment-0008.db"),
					size: 408,
				},
			)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("4th round, new and old candidates due to sum of new sizes big enough", func(t *testing.T) {
			// not cleaned before, cleaning considering 7+8
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 5, startIdx1, 6)
			assertSegment(t, sg, idx1, "segment-0006.db", 406)
			require.NotNil(t, onCompleted1)
			onCompleted1(306)
			sg.segments[idx1].size = 306

			// not cleaned before, cleaning considering 8
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 6, startIdx2, 7)
			assertSegment(t, sg, idx2, "segment-0007.db", 407)
			require.NotNil(t, onCompleted2)
			onCompleted2(307)
			sg.segments[idx2].size = 307

			// sum of sizes (5+6+7+8) big enough compared to segment's size, cleaning considering 5+6+7+8
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 0, startIdx3, 4)
			assertSegment(t, sg, idx3, "segment-0001.db", 9001)
			require.NotNil(t, onCompleted3)
			onCompleted3(8001)
			sg.segments[idx3].size = 8001

			// sum of sizes (5+6+7+8) big enough compared to segment's size, cleaning considering 5+6+7+8
			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateFound(t, idx4, 1, startIdx4, 4)
			assertSegment(t, sg, idx4, "segment-0002.db", 9002)
			require.NotNil(t, onCompleted4)
			onCompleted4(8002)
			sg.segments[idx4].size = 8002

			// sum of sizes (5+6+7+8) big enough compared to segment's size, cleaning considering 5+6+7+8
			idx5, startIdx5, onCompleted5, err5 := sc.findCandidate()
			require.NoError(t, err5)
			requireCandidateFound(t, idx5, 2, startIdx5, 4)
			assertSegment(t, sg, idx5, "segment-0003.db", 9003)
			require.NotNil(t, onCompleted5)
			onCompleted5(8003)
			sg.segments[idx5].size = 8003

			// skipping 4 due to sum of new sizes (7+8) not big enough compared to segment's size
			// sum of sizes (7+8) big enough compared to segment's size, cleaning considering 7+8
			idx6, startIdx6, onCompleted6, err6 := sc.findCandidate()
			require.NoError(t, err6)
			requireCandidateFound(t, idx6, 4, startIdx6, 6)
			assertSegment(t, sg, idx6, "segment-0005.db", 305)
			require.NotNil(t, onCompleted6)
			onCompleted6(205)
			sg.segments[idx6].size = 205

			idx7, startIdx7, onCompleted7, err7 := sc.findCandidate()
			require.NoError(t, err7)
			requireCandidateNotFound(t, idx7, startIdx7)
			assert.Nil(t, onCompleted7)
		})

		assertBoltDbKeys(t, sc.db, []int64{1, 2, 3, 4, 5, 6, 7})
	})

	t.Run("multilpe segments, cleanup and compaction", func(t *testing.T) {
		dir := t.TempDir()
		sg := &SegmentGroup{
			dir: dir,
			segments: []*segment{
				{
					path: filepath.Join(dir, "segment-0001.db"),
					size: 10001,
				},
				{
					path: filepath.Join(dir, "segment-0002.db"),
					size: 10002,
				},
				{
					path: filepath.Join(dir, "segment-0003.db"),
					size: 10003,
				},
				{
					path: filepath.Join(dir, "segment-0004.db"),
					size: 10004,
				},
				{
					path: filepath.Join(dir, "segment-0005.db"),
					size: 10005,
				},
			},
			cleanupInterval: time.Millisecond * 250,
		}
		sc := &segmentCleanerCommon{sg: sg}
		require.NoError(t, sc.init())
		defer sc.close()

		t.Run("1st round, all but last cleaned", func(t *testing.T) {
			// not cleaned before, cleaning considering 2+3+4+5
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 0, startIdx1, 1)
			assertSegment(t, sg, idx1, "segment-0001.db", 10001)
			require.NotNil(t, onCompleted1)
			onCompleted1(9001)
			sg.segments[idx1].size = 9001

			// not cleaned before, cleaning considering 3+4+5
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0002.db", 10002)
			require.NotNil(t, onCompleted2)
			onCompleted2(9002)
			sg.segments[idx2].size = 9002

			// not cleaned before, cleaning considering 4+5
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 2, startIdx3, 3)
			assertSegment(t, sg, idx3, "segment-0003.db", 10003)
			require.NotNil(t, onCompleted3)
			onCompleted3(9003)
			sg.segments[idx3].size = 9003

			// not cleaned before, cleaning considering 5
			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateFound(t, idx4, 3, startIdx4, 4)
			assertSegment(t, sg, idx4, "segment-0004.db", 10004)
			require.NotNil(t, onCompleted4)
			onCompleted4(9004)
			sg.segments[idx4].size = 9004

			// skipping 5 as last one
			idx5, startIdx5, onCompleted5, err5 := sc.findCandidate()
			require.NoError(t, err5)
			requireCandidateNotFound(t, idx5, startIdx5)
			assert.Nil(t, onCompleted5)
		})

		t.Run("no candidates before interval", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("compact", func(t *testing.T) {
			seg2 := sg.segments[1]
			seg2.size = 20002
			seg4 := sg.segments[3]
			seg4.size = 20004
			seg5 := sg.segments[4]

			sg.segments = []*segment{seg2, seg4, seg5}
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("2nd round, no candidates due to no new segments", func(t *testing.T) {
			// no new segments
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("new segments created", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0006.db"),
					size: 10006,
				},
			)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("3rd round, new segments cleaned and some old ones", func(t *testing.T) {
			// not cleaned before, cleaning considering 6
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 2, startIdx1, 3)
			assertSegment(t, sg, idx1, "segment-0005.db", 10005)
			require.NotNil(t, onCompleted1)
			onCompleted1(9005)
			sg.segments[idx1].size = 9005

			// size changed, cleanup considering all next segments, including new ones
			// sum of sizes (4+5+6) big enough compared to segment's size, cleaning considering 4+5+6
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 0, startIdx2, 1)
			assertSegment(t, sg, idx2, "segment-0002.db", 20002)
			require.NotNil(t, onCompleted2)
			onCompleted2(19002)
			sg.segments[idx2].size = 19002
		})

		t.Run("new segments created while 3rd round", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0007.db"),
					size: 10007,
				},
			)
		})

		t.Run("3rd round ongoing, new segments cleaned and some old ones", func(t *testing.T) {
			// not cleaned before, cleaning considering 7
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 3, startIdx1, 4)
			assertSegment(t, sg, idx1, "segment-0006.db", 10006)
			require.NotNil(t, onCompleted1)
			onCompleted1(9006)
			sg.segments[idx1].size = 9006

			// size changed, cleanup considering all next segments, including new ones
			// sum of sizes (5+6+7) big enough compared to segment's size, cleaning considering 5+6+7
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0004.db", 20004)
			require.NotNil(t, onCompleted2)
			onCompleted2(19004)
			sg.segments[idx2].size = 19004

			// skipping 7 as last one
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateNotFound(t, idx3, startIdx3)
			assert.Nil(t, onCompleted3)
		})

		t.Run("compact", func(t *testing.T) {
			seg4 := sg.segments[1]
			seg4.size = 40004
			seg6 := sg.segments[3]
			seg6.size = 20006
			seg7 := sg.segments[4]

			sg.segments = []*segment{seg4, seg6, seg7}
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("4th round, no candidates due to no new segments", func(t *testing.T) {
			idx, startIdx, onCompleted, err := sc.findCandidate()
			require.NoError(t, err)
			requireCandidateNotFound(t, idx, startIdx)
			assert.Nil(t, onCompleted)
		})

		t.Run("new segments created", func(t *testing.T) {
			sg.segments = append(sg.segments,
				&segment{
					path: filepath.Join(dir, "segment-0008.db"),
					size: 10008,
				},
			)
		})

		t.Run("wait interval for next round", func(t *testing.T) {
			time.Sleep(sg.cleanupInterval)
		})

		t.Run("5th round, new segments cleaned and some old ones", func(t *testing.T) {
			// not cleaned before, cleaning considering 8
			idx1, startIdx1, onCompleted1, err1 := sc.findCandidate()
			require.NoError(t, err1)
			requireCandidateFound(t, idx1, 2, startIdx1, 3)
			assertSegment(t, sg, idx1, "segment-0007.db", 10007)
			require.NotNil(t, onCompleted1)
			onCompleted1(9007)
			sg.segments[idx1].size = 9007

			// size changed, cleanup considering all next segments, including new ones
			// sum of sizes (7+8) big enough compared to segment's size, cleaning considering 7+8
			idx2, startIdx2, onCompleted2, err2 := sc.findCandidate()
			require.NoError(t, err2)
			requireCandidateFound(t, idx2, 1, startIdx2, 2)
			assertSegment(t, sg, idx2, "segment-0006.db", 20006)
			require.NotNil(t, onCompleted2)
			onCompleted2(19006)
			sg.segments[idx2].size = 19006

			// size changed, cleanup considering all next segments, including new ones
			// sum of sizes (6+7+8) big enough compared to segment's size, cleaning considering 6+7+8
			idx3, startIdx3, onCompleted3, err3 := sc.findCandidate()
			require.NoError(t, err3)
			requireCandidateFound(t, idx3, 0, startIdx3, 1)
			assertSegment(t, sg, idx3, "segment-0004.db", 40004)
			require.NotNil(t, onCompleted3)
			onCompleted3(39004)
			sg.segments[idx3].size = 39004

			// skipping 8 as last one
			idx4, startIdx4, onCompleted4, err4 := sc.findCandidate()
			require.NoError(t, err4)
			requireCandidateNotFound(t, idx4, startIdx4)
			assert.Nil(t, onCompleted4)
		})

		assertBoltDbKeys(t, sc.db, []int64{4, 6, 7})
	})
}

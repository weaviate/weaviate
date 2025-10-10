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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestInvertedDelete(t *testing.T) {
	size := 100

	addedDocIds := make(map[uint64]struct{})
	removedDocIds := make(map[uint64]struct{})

	// this segment is not part of the merge, but might still play a role in
	// overall results. For example if one of the later segments has a tombstone
	// for it
	var previous1 []kv
	var previous2 []kv

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var bucket *Bucket

	dirName := t.TempDir()

	opts := []BucketOption{
		WithStrategy(StrategyInverted),
	}

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 0.) created in the first segment, never touched again
		// 1.) created in the first segment, appended to it in the second
		// 2.) created in the first segment, first element updated in the second
		// 3.) created in the first segment, second element updated in the second
		// 4.) created in the first segment, first element deleted in the second
		// 5.) created in the first segment, second element deleted in the second
		// 6.) not present in the first segment, created in the second
		// 7.) present in an unrelated previous segment, deleted in the first
		// 8.) present in an unrelated previous segment, deleted in the second
		// 9.) present in an unrelated previous segment, never touched again
		for i := 0; i < size; i++ {
			rowKey := []byte(fmt.Sprintf("row-%03d", i))

			docId1 := uint64(i)
			docId2 := uint64(i + 10000)

			pair1 := NewMapPairFromDocIdAndTf(docId1, float32(i+1), float32(i+2), false)
			pair2 := NewMapPairFromDocIdAndTf(docId2, float32(i+1), float32(i+2), false)

			pairs := []MapPair{pair1, pair2}

			switch i % 10 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				addedDocIds[docId1] = struct{}{}
				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:    rowKey,
					values: pairs[:1],
				})
			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				// add extra pair in the second segment
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs[1:2],
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})
			case 2:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair1
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 3:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair2
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 4:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete first key in the second segment
				updated := pair1
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2},
				})

				removedDocIds[docId1] = struct{}{}
			case 5:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete second key in the second segment
				updated := pair2
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1},
				})

				removedDocIds[docId2] = struct{}{}

			case 6:
				// do not add to segment 2

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs,
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 7:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

				// should not have any values in expected at all, not even a key

			case 8:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				// should not have any values in expected at all, not even a key

			case 9:
				// only part of a previous segment
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}
			}
		}
	})

	ctx := context.Background()
	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)
		bucket = b
	})

	t.Run("import previous1 segments", func(t *testing.T) {
		for _, kvs := range previous1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)

				require.Nil(t, err)
			}
		}
	})

	err := bucket.FlushAndSwitch()
	require.Nil(t, err)

	// delete all docs in previous1 from the index
	t.Run("delete previous1", func(t *testing.T) {
		for _, pair := range previous1[0].values {
			docID := binary.BigEndian.Uint64(pair.Key)
			err := bucket.InvertedDeleteDocs([]uint64{docID})
			require.Nil(t, err)
		}
	})

	t.Run("import previous1 segments", func(t *testing.T) {
		for _, kvs := range previous2[:1] {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	err = bucket.FlushAndSwitch()
	require.Nil(t, err)
	time.Sleep(1 * time.Second)

	for i, s := range bucket.disk.segments {
		seg := s.(*segment)
		to, err := seg.ReadOnlyTombstones()
		require.Nil(t, err)
		fmt.Printf("Segment %v has %v tombstones\n", i, to.GetCardinality())
	}
	assert.Error(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, previous1[:1]))
}

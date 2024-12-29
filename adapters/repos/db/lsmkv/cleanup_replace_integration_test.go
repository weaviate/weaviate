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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func cleanupReplaceStrategy(ctx context.Context, t *testing.T, opts []BucketOption) {
	dir := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer bucket.Shutdown(context.Background())

	type kvt struct {
		pkey string
		val  string
		tomb bool
	}

	t.Run("create segments", func(t *testing.T) {
		/*
			SEG1	SEG2	SEG3	SEG4	SEG5
			------------------------------------
			c101
			c102	u102
			c103			u103
			c104					u104
			c105							u105
			c106	d106
			c107			d107
			c108					d108
			c109							d109
			c110	u110	d110
			c111			u111	d111
			c112					u112	d112
			c113	d113	u113
			c114			d114	u114
			c115					d115	u115
			------------------------------------
					c201
					c202	u202
					c203			u203
					c204					u204
					c205	d205
					c206			d206
					c207					d207
					c208	u208	d208
					c209			u209	d209
					c210	d210	u210
					c211			d211	u211
			------------------------------------
							c301
							c302	u302
							c303			u303
							c304	d304
							c305			d305
							c306	u306	d306
							c307	d307	u307
			------------------------------------
									c401
									c402	u402
									c403	d403
			------------------------------------
											c501
		*/

		put := func(t *testing.T, pkey, value string) {
			require.NoError(t, bucket.Put([]byte(pkey), []byte(value)))
		}
		delete := func(t *testing.T, pkey string) {
			require.NoError(t, bucket.Delete([]byte(pkey)))
		}

		t.Run("segment 1", func(t *testing.T) {
			put(t, "key101_created1", "created")
			put(t, "key102_updated2", "created")
			put(t, "key103_updated3", "created")
			put(t, "key104_updated4", "created")
			put(t, "key105_updated5", "created")
			put(t, "key106_deleted2", "created")
			put(t, "key107_deleted3", "created")
			put(t, "key108_deleted4", "created")
			put(t, "key109_deleted5", "created")
			put(t, "key110_updated2_deleted3", "created")
			put(t, "key111_updated3_deleted4", "created")
			put(t, "key112_updated4_deleted5", "created")
			put(t, "key113_deleted2_updated3", "created")
			put(t, "key114_deleted3_updated4", "created")
			put(t, "key115_deleted4_updated5", "created")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 2", func(t *testing.T) {
			put(t, "key201_created2", "created")
			put(t, "key202_updated3", "created")
			put(t, "key203_updated4", "created")
			put(t, "key204_updated5", "created")
			put(t, "key205_deleted3", "created")
			put(t, "key206_deleted4", "created")
			put(t, "key207_deleted5", "created")
			put(t, "key208_updated3_deleted4", "created")
			put(t, "key209_updated4_deleted5", "created")
			put(t, "key210_deleted3_updated4", "created")
			put(t, "key211_deleted4_updated5", "created")

			put(t, "key102_updated2", "updated")
			put(t, "key110_updated2_deleted3", "updated")

			delete(t, "key106_deleted2")
			delete(t, "key113_deleted2_updated3")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 3", func(t *testing.T) {
			put(t, "key301_created3", "created")
			put(t, "key302_updated4", "created")
			put(t, "key303_updated5", "created")
			put(t, "key304_deleted4", "created")
			put(t, "key305_deleted5", "created")
			put(t, "key306_updated4_deleted5", "created")
			put(t, "key307_deleted4_updated5", "created")

			put(t, "key103_updated3", "updated")
			put(t, "key111_updated3_deleted4", "updated")
			put(t, "key113_deleted2_updated3", "updated")
			put(t, "key202_updated3", "updated")
			put(t, "key208_updated3_deleted4", "updated")

			delete(t, "key107_deleted3")
			delete(t, "key110_updated2_deleted3")
			delete(t, "key114_deleted3_updated4")
			delete(t, "key205_deleted3")
			delete(t, "key210_deleted3_updated4")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 4", func(t *testing.T) {
			put(t, "key401_created4", "created")
			put(t, "key402_updated5", "created")
			put(t, "key403_deleted5", "created")

			put(t, "key104_updated4", "updated")
			put(t, "key112_updated4_deleted5", "updated")
			put(t, "key114_deleted3_updated4", "updated")
			put(t, "key203_updated4", "updated")
			put(t, "key209_updated4_deleted5", "updated")
			put(t, "key210_deleted3_updated4", "updated")
			put(t, "key302_updated4", "updated")
			put(t, "key306_updated4_deleted5", "updated")

			delete(t, "key108_deleted4")
			delete(t, "key111_updated3_deleted4")
			delete(t, "key115_deleted4_updated5")
			delete(t, "key206_deleted4")
			delete(t, "key208_updated3_deleted4")
			delete(t, "key211_deleted4_updated5")
			delete(t, "key304_deleted4")
			delete(t, "key307_deleted4_updated5")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 5", func(t *testing.T) {
			put(t, "key501_created5", "created")

			put(t, "key105_updated5", "updated")
			put(t, "key115_deleted4_updated5", "updated")
			put(t, "key204_updated5", "updated")
			put(t, "key211_deleted4_updated5", "updated")
			put(t, "key303_updated5", "updated")
			put(t, "key307_deleted4_updated5", "updated")
			put(t, "key402_updated5", "updated")

			delete(t, "key109_deleted5")
			delete(t, "key112_updated4_deleted5")
			delete(t, "key207_deleted5")
			delete(t, "key209_updated4_deleted5")
			delete(t, "key305_deleted5")
			delete(t, "key306_updated4_deleted5")
			delete(t, "key403_deleted5")

			require.NoError(t, bucket.FlushAndSwitch())
		})
	})

	t.Run("clean segments", func(t *testing.T) {
		shouldAbort := func() bool { return false }
		count := 5 // 5 segments total

		// all but last segments should be cleaned
		for i := 0; i < count; i++ {
			cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(shouldAbort)
			assert.NoError(t, err)

			if i != count-1 {
				assert.True(t, cleaned)
			} else {
				assert.False(t, cleaned)
			}
		}
	})

	t.Run("verify segments' contents", func(t *testing.T) {
		assertContents := func(t *testing.T, segIdx int, expected []*kvt) {
			seg := bucket.disk.segments[segIdx]
			cur := seg.newCursor()

			i := 0
			var k, v []byte
			var err error
			for k, v, err = cur.first(); k != nil && i < len(expected); k, v, err = cur.next() {
				assert.Equal(t, []byte(expected[i].pkey), k)
				if expected[i].tomb {
					assert.ErrorIs(t, err, lsmkv.Deleted)
					assert.Nil(t, v)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []byte(expected[i].val), v)
				}
				i++
			}
			assert.ErrorIs(t, err, lsmkv.NotFound, "cursor not finished")
			assert.Equal(t, i, len(expected), "more entries expected")
		}

		t.Run("segment 1", func(t *testing.T) {
			assertContents(t, 0, []*kvt{
				{pkey: "key101_created1", val: "created"},
			})
		})

		t.Run("segment 2", func(t *testing.T) {
			assertContents(t, 1, []*kvt{
				{pkey: "key102_updated2", val: "updated"},
				{pkey: "key106_deleted2", tomb: true},
				{pkey: "key201_created2", val: "created"},
			})
		})

		t.Run("segment 3", func(t *testing.T) {
			assertContents(t, 2, []*kvt{
				{pkey: "key103_updated3", val: "updated"},
				{pkey: "key107_deleted3", tomb: true},
				{pkey: "key110_updated2_deleted3", tomb: true},
				{pkey: "key113_deleted2_updated3", val: "updated"},
				{pkey: "key202_updated3", val: "updated"},
				{pkey: "key205_deleted3", tomb: true},
				{pkey: "key301_created3", val: "created"},
			})
		})

		t.Run("segment 4", func(t *testing.T) {
			assertContents(t, 3, []*kvt{
				{pkey: "key104_updated4", val: "updated"},
				{pkey: "key108_deleted4", tomb: true},
				{pkey: "key111_updated3_deleted4", tomb: true},
				{pkey: "key114_deleted3_updated4", val: "updated"},
				{pkey: "key203_updated4", val: "updated"},
				{pkey: "key206_deleted4", tomb: true},
				{pkey: "key208_updated3_deleted4", tomb: true},
				{pkey: "key210_deleted3_updated4", val: "updated"},
				{pkey: "key302_updated4", val: "updated"},
				{pkey: "key304_deleted4", tomb: true},
				{pkey: "key401_created4", val: "created"},
			})
		})

		t.Run("segment 5", func(t *testing.T) {
			assertContents(t, 4, []*kvt{
				{pkey: "key105_updated5", val: "updated"},
				{pkey: "key109_deleted5", tomb: true},
				{pkey: "key112_updated4_deleted5", tomb: true},
				{pkey: "key115_deleted4_updated5", val: "updated"},
				{pkey: "key204_updated5", val: "updated"},
				{pkey: "key207_deleted5", tomb: true},
				{pkey: "key209_updated4_deleted5", tomb: true},
				{pkey: "key211_deleted4_updated5", val: "updated"},
				{pkey: "key303_updated5", val: "updated"},
				{pkey: "key305_deleted5", tomb: true},
				{pkey: "key306_updated4_deleted5", tomb: true},
				{pkey: "key307_deleted4_updated5", val: "updated"},
				{pkey: "key402_updated5", val: "updated"},
				{pkey: "key403_deleted5", tomb: true},
				{pkey: "key501_created5", val: "created"},
			})
		})
	})

	t.Run("verify bucket's contents", func(t *testing.T) {
		expected := []*kvt{
			{pkey: "key101_created1", val: "created"},
			{pkey: "key102_updated2", val: "updated"},
			{pkey: "key103_updated3", val: "updated"},
			{pkey: "key104_updated4", val: "updated"},
			{pkey: "key105_updated5", val: "updated"},
			{pkey: "key106_deleted2", tomb: true},
			{pkey: "key107_deleted3", tomb: true},
			{pkey: "key108_deleted4", tomb: true},
			{pkey: "key109_deleted5", tomb: true},
			{pkey: "key110_updated2_deleted3", tomb: true},
			{pkey: "key111_updated3_deleted4", tomb: true},
			{pkey: "key112_updated4_deleted5", tomb: true},
			{pkey: "key113_deleted2_updated3", val: "updated"},
			{pkey: "key114_deleted3_updated4", val: "updated"},
			{pkey: "key115_deleted4_updated5", val: "updated"},

			{pkey: "key201_created2", val: "created"},
			{pkey: "key202_updated3", val: "updated"},
			{pkey: "key203_updated4", val: "updated"},
			{pkey: "key204_updated5", val: "updated"},
			{pkey: "key205_deleted3", tomb: true},
			{pkey: "key206_deleted4", tomb: true},
			{pkey: "key207_deleted5", tomb: true},
			{pkey: "key208_updated3_deleted4", tomb: true},
			{pkey: "key209_updated4_deleted5", tomb: true},
			{pkey: "key210_deleted3_updated4", val: "updated"},
			{pkey: "key211_deleted4_updated5", val: "updated"},

			{pkey: "key301_created3", val: "created"},
			{pkey: "key302_updated4", val: "updated"},
			{pkey: "key303_updated5", val: "updated"},
			{pkey: "key304_deleted4", tomb: true},
			{pkey: "key305_deleted5", tomb: true},
			{pkey: "key306_updated4_deleted5", tomb: true},
			{pkey: "key307_deleted4_updated5", val: "updated"},

			{pkey: "key401_created4", val: "created"},
			{pkey: "key402_updated5", val: "updated"},
			{pkey: "key403_deleted5", tomb: true},

			{pkey: "key501_created5", val: "created"},
		}
		expectedExising := []*kvt{}
		for i := range expected {
			if !expected[i].tomb {
				expectedExising = append(expectedExising, expected[i])
			}
		}

		t.Run("cursor", func(t *testing.T) {
			c := bucket.Cursor()
			defer c.Close()

			i := 0
			for k, v := c.First(); k != nil && i < len(expectedExising); k, v = c.Next() {
				assert.Equal(t, []byte(expectedExising[i].pkey), k)
				assert.Equal(t, []byte(expectedExising[i].val), v)
				i++
			}
			assert.Equal(t, i, len(expectedExising))
		})

		t.Run("get", func(t *testing.T) {
			for i := range expected {
				val, err := bucket.Get([]byte(expected[i].pkey))

				assert.NoError(t, err)
				if expected[i].tomb {
					assert.Nil(t, val)
				} else {
					assert.Equal(t, []byte(expected[i].val), val)
				}
			}
		})

		t.Run("net count", func(t *testing.T) {
			assert.Equal(t, len(expectedExising), bucket.Count())
		})
	})
}

func cleanupReplaceStrategy_WithSecondaryKeys(ctx context.Context, t *testing.T, opts []BucketOption) {
	dir := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	defer bucket.Shutdown(context.Background())

	secondaryKey0 := func(primaryKey string) string {
		return "secondary0-" + primaryKey
	}
	secondaryKey1 := func(primaryKey string) string {
		return "secondary1-" + primaryKey
	}

	type kvt struct {
		pkey string
		val  string
		tomb bool
	}

	t.Run("create segments", func(t *testing.T) {
		/*
			SEG1	SEG2	SEG3	SEG4	SEG5
			------------------------------------
			c101
			c102	u102
			c103			u103
			c104					u104
			c105							u105
			c106	d106
			c107			d107
			c108					d108
			c109							d109
			c110	u110	d110
			c111			u111	d111
			c112					u112	d112
			c113	d113	u113
			c114			d114	u114
			c115					d115	u115
			------------------------------------
					c201
					c202	u202
					c203			u203
					c204					u204
					c205	d205
					c206			d206
					c207					d207
					c208	u208	d208
					c209			u209	d209
					c210	d210	u210
					c211			d211	u211
			------------------------------------
							c301
							c302	u302
							c303			u303
							c304	d304
							c305			d305
							c306	u306	d306
							c307	d307	u307
			------------------------------------
									c401
									c402	u402
									c403	d403
			------------------------------------
											c501
		*/

		putWithSecondaryKeys := func(t *testing.T, pkey, value string) {
			err := bucket.Put(
				[]byte(pkey),
				[]byte(value),
				WithSecondaryKey(0, []byte(secondaryKey0(pkey))),
				WithSecondaryKey(1, []byte(secondaryKey1(pkey))),
			)
			require.NoError(t, err)
		}
		deleteWithSecondaryKeys := func(t *testing.T, pkey string) {
			err := bucket.Delete(
				[]byte(pkey),
				WithSecondaryKey(0, []byte(secondaryKey0(pkey))),
				WithSecondaryKey(1, []byte(secondaryKey1(pkey))),
			)
			require.NoError(t, err)
		}

		t.Run("segment 1", func(t *testing.T) {
			putWithSecondaryKeys(t, "key101_created1", "created")
			putWithSecondaryKeys(t, "key102_updated2", "created")
			putWithSecondaryKeys(t, "key103_updated3", "created")
			putWithSecondaryKeys(t, "key104_updated4", "created")
			putWithSecondaryKeys(t, "key105_updated5", "created")
			putWithSecondaryKeys(t, "key106_deleted2", "created")
			putWithSecondaryKeys(t, "key107_deleted3", "created")
			putWithSecondaryKeys(t, "key108_deleted4", "created")
			putWithSecondaryKeys(t, "key109_deleted5", "created")
			putWithSecondaryKeys(t, "key110_updated2_deleted3", "created")
			putWithSecondaryKeys(t, "key111_updated3_deleted4", "created")
			putWithSecondaryKeys(t, "key112_updated4_deleted5", "created")
			putWithSecondaryKeys(t, "key113_deleted2_updated3", "created")
			putWithSecondaryKeys(t, "key114_deleted3_updated4", "created")
			putWithSecondaryKeys(t, "key115_deleted4_updated5", "created")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 2", func(t *testing.T) {
			putWithSecondaryKeys(t, "key201_created2", "created")
			putWithSecondaryKeys(t, "key202_updated3", "created")
			putWithSecondaryKeys(t, "key203_updated4", "created")
			putWithSecondaryKeys(t, "key204_updated5", "created")
			putWithSecondaryKeys(t, "key205_deleted3", "created")
			putWithSecondaryKeys(t, "key206_deleted4", "created")
			putWithSecondaryKeys(t, "key207_deleted5", "created")
			putWithSecondaryKeys(t, "key208_updated3_deleted4", "created")
			putWithSecondaryKeys(t, "key209_updated4_deleted5", "created")
			putWithSecondaryKeys(t, "key210_deleted3_updated4", "created")
			putWithSecondaryKeys(t, "key211_deleted4_updated5", "created")

			putWithSecondaryKeys(t, "key102_updated2", "updated")
			putWithSecondaryKeys(t, "key110_updated2_deleted3", "updated")

			deleteWithSecondaryKeys(t, "key106_deleted2")
			deleteWithSecondaryKeys(t, "key113_deleted2_updated3")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 3", func(t *testing.T) {
			putWithSecondaryKeys(t, "key301_created3", "created")
			putWithSecondaryKeys(t, "key302_updated4", "created")
			putWithSecondaryKeys(t, "key303_updated5", "created")
			putWithSecondaryKeys(t, "key304_deleted4", "created")
			putWithSecondaryKeys(t, "key305_deleted5", "created")
			putWithSecondaryKeys(t, "key306_updated4_deleted5", "created")
			putWithSecondaryKeys(t, "key307_deleted4_updated5", "created")

			putWithSecondaryKeys(t, "key103_updated3", "updated")
			putWithSecondaryKeys(t, "key111_updated3_deleted4", "updated")
			putWithSecondaryKeys(t, "key113_deleted2_updated3", "updated")
			putWithSecondaryKeys(t, "key202_updated3", "updated")
			putWithSecondaryKeys(t, "key208_updated3_deleted4", "updated")

			deleteWithSecondaryKeys(t, "key107_deleted3")
			deleteWithSecondaryKeys(t, "key110_updated2_deleted3")
			deleteWithSecondaryKeys(t, "key114_deleted3_updated4")
			deleteWithSecondaryKeys(t, "key205_deleted3")
			deleteWithSecondaryKeys(t, "key210_deleted3_updated4")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 4", func(t *testing.T) {
			putWithSecondaryKeys(t, "key401_created4", "created")
			putWithSecondaryKeys(t, "key402_updated5", "created")
			putWithSecondaryKeys(t, "key403_deleted5", "created")

			putWithSecondaryKeys(t, "key104_updated4", "updated")
			putWithSecondaryKeys(t, "key112_updated4_deleted5", "updated")
			putWithSecondaryKeys(t, "key114_deleted3_updated4", "updated")
			putWithSecondaryKeys(t, "key203_updated4", "updated")
			putWithSecondaryKeys(t, "key209_updated4_deleted5", "updated")
			putWithSecondaryKeys(t, "key210_deleted3_updated4", "updated")
			putWithSecondaryKeys(t, "key302_updated4", "updated")
			putWithSecondaryKeys(t, "key306_updated4_deleted5", "updated")

			deleteWithSecondaryKeys(t, "key108_deleted4")
			deleteWithSecondaryKeys(t, "key111_updated3_deleted4")
			deleteWithSecondaryKeys(t, "key115_deleted4_updated5")
			deleteWithSecondaryKeys(t, "key206_deleted4")
			deleteWithSecondaryKeys(t, "key208_updated3_deleted4")
			deleteWithSecondaryKeys(t, "key211_deleted4_updated5")
			deleteWithSecondaryKeys(t, "key304_deleted4")
			deleteWithSecondaryKeys(t, "key307_deleted4_updated5")

			require.NoError(t, bucket.FlushAndSwitch())
		})

		t.Run("segment 5", func(t *testing.T) {
			putWithSecondaryKeys(t, "key501_created5", "created")

			putWithSecondaryKeys(t, "key105_updated5", "updated")
			putWithSecondaryKeys(t, "key115_deleted4_updated5", "updated")
			putWithSecondaryKeys(t, "key204_updated5", "updated")
			putWithSecondaryKeys(t, "key211_deleted4_updated5", "updated")
			putWithSecondaryKeys(t, "key303_updated5", "updated")
			putWithSecondaryKeys(t, "key307_deleted4_updated5", "updated")
			putWithSecondaryKeys(t, "key402_updated5", "updated")

			deleteWithSecondaryKeys(t, "key109_deleted5")
			deleteWithSecondaryKeys(t, "key112_updated4_deleted5")
			deleteWithSecondaryKeys(t, "key207_deleted5")
			deleteWithSecondaryKeys(t, "key209_updated4_deleted5")
			deleteWithSecondaryKeys(t, "key305_deleted5")
			deleteWithSecondaryKeys(t, "key306_updated4_deleted5")
			deleteWithSecondaryKeys(t, "key403_deleted5")

			require.NoError(t, bucket.FlushAndSwitch())
		})
	})

	t.Run("clean segments", func(t *testing.T) {
		shouldAbort := func() bool { return false }
		count := 5 // 5 segments total

		// all but last segments should be cleaned
		for i := 0; i < count; i++ {
			cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(shouldAbort)
			assert.NoError(t, err)

			if i != count-1 {
				assert.True(t, cleaned)
			} else {
				assert.False(t, cleaned)
			}
		}
	})

	t.Run("verify segments' contents", func(t *testing.T) {
		assertContents := func(t *testing.T, segIdx int, expected []*kvt) {
			seg := bucket.disk.segments[segIdx]
			cur := seg.newCursor()

			i := 0
			var n segmentReplaceNode
			var err error

			for n, err = cur.firstWithAllKeys(); !errors.Is(err, lsmkv.NotFound) && i < len(expected); n, err = cur.nextWithAllKeys() {
				assert.Equal(t, uint16(2), n.secondaryIndexCount)
				assert.Equal(t, []byte(expected[i].pkey), n.primaryKey)
				assert.Equal(t, []byte(secondaryKey0(expected[i].pkey)), []byte(n.secondaryKeys[0]))
				assert.Equal(t, []byte(secondaryKey1(expected[i].pkey)), []byte(n.secondaryKeys[1]))

				if expected[i].tomb {
					assert.ErrorIs(t, err, lsmkv.Deleted)
					assert.Equal(t, []byte{}, n.value)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, []byte(expected[i].val), n.value)
				}
				i++
			}
			assert.ErrorIs(t, err, lsmkv.NotFound, "cursor not finished")
			assert.Equal(t, i, len(expected), "more entries expected")
		}

		t.Run("segment 1", func(t *testing.T) {
			assertContents(t, 0, []*kvt{
				{pkey: "key101_created1", val: "created"},
			})
		})

		t.Run("segment 2", func(t *testing.T) {
			assertContents(t, 1, []*kvt{
				{pkey: "key102_updated2", val: "updated"},
				{pkey: "key106_deleted2", tomb: true},
				{pkey: "key201_created2", val: "created"},
			})
		})

		t.Run("segment 3", func(t *testing.T) {
			assertContents(t, 2, []*kvt{
				{pkey: "key103_updated3", val: "updated"},
				{pkey: "key107_deleted3", tomb: true},
				{pkey: "key110_updated2_deleted3", tomb: true},
				{pkey: "key113_deleted2_updated3", val: "updated"},
				{pkey: "key202_updated3", val: "updated"},
				{pkey: "key205_deleted3", tomb: true},
				{pkey: "key301_created3", val: "created"},
			})
		})

		t.Run("segment 4", func(t *testing.T) {
			assertContents(t, 3, []*kvt{
				{pkey: "key104_updated4", val: "updated"},
				{pkey: "key108_deleted4", tomb: true},
				{pkey: "key111_updated3_deleted4", tomb: true},
				{pkey: "key114_deleted3_updated4", val: "updated"},
				{pkey: "key203_updated4", val: "updated"},
				{pkey: "key206_deleted4", tomb: true},
				{pkey: "key208_updated3_deleted4", tomb: true},
				{pkey: "key210_deleted3_updated4", val: "updated"},
				{pkey: "key302_updated4", val: "updated"},
				{pkey: "key304_deleted4", tomb: true},
				{pkey: "key401_created4", val: "created"},
			})
		})

		t.Run("segment 5", func(t *testing.T) {
			assertContents(t, 4, []*kvt{
				{pkey: "key105_updated5", val: "updated"},
				{pkey: "key109_deleted5", tomb: true},
				{pkey: "key112_updated4_deleted5", tomb: true},
				{pkey: "key115_deleted4_updated5", val: "updated"},
				{pkey: "key204_updated5", val: "updated"},
				{pkey: "key207_deleted5", tomb: true},
				{pkey: "key209_updated4_deleted5", tomb: true},
				{pkey: "key211_deleted4_updated5", val: "updated"},
				{pkey: "key303_updated5", val: "updated"},
				{pkey: "key305_deleted5", tomb: true},
				{pkey: "key306_updated4_deleted5", tomb: true},
				{pkey: "key307_deleted4_updated5", val: "updated"},
				{pkey: "key402_updated5", val: "updated"},
				{pkey: "key403_deleted5", tomb: true},
				{pkey: "key501_created5", val: "created"},
			})
		})
	})

	t.Run("verify bucket's contents", func(t *testing.T) {
		expected := []*kvt{
			{pkey: "key101_created1", val: "created"},
			{pkey: "key102_updated2", val: "updated"},
			{pkey: "key103_updated3", val: "updated"},
			{pkey: "key104_updated4", val: "updated"},
			{pkey: "key105_updated5", val: "updated"},
			{pkey: "key106_deleted2", tomb: true},
			{pkey: "key107_deleted3", tomb: true},
			{pkey: "key108_deleted4", tomb: true},
			{pkey: "key109_deleted5", tomb: true},
			{pkey: "key110_updated2_deleted3", tomb: true},
			{pkey: "key111_updated3_deleted4", tomb: true},
			{pkey: "key112_updated4_deleted5", tomb: true},
			{pkey: "key113_deleted2_updated3", val: "updated"},
			{pkey: "key114_deleted3_updated4", val: "updated"},
			{pkey: "key115_deleted4_updated5", val: "updated"},

			{pkey: "key201_created2", val: "created"},
			{pkey: "key202_updated3", val: "updated"},
			{pkey: "key203_updated4", val: "updated"},
			{pkey: "key204_updated5", val: "updated"},
			{pkey: "key205_deleted3", tomb: true},
			{pkey: "key206_deleted4", tomb: true},
			{pkey: "key207_deleted5", tomb: true},
			{pkey: "key208_updated3_deleted4", tomb: true},
			{pkey: "key209_updated4_deleted5", tomb: true},
			{pkey: "key210_deleted3_updated4", val: "updated"},
			{pkey: "key211_deleted4_updated5", val: "updated"},

			{pkey: "key301_created3", val: "created"},
			{pkey: "key302_updated4", val: "updated"},
			{pkey: "key303_updated5", val: "updated"},
			{pkey: "key304_deleted4", tomb: true},
			{pkey: "key305_deleted5", tomb: true},
			{pkey: "key306_updated4_deleted5", tomb: true},
			{pkey: "key307_deleted4_updated5", val: "updated"},

			{pkey: "key401_created4", val: "created"},
			{pkey: "key402_updated5", val: "updated"},
			{pkey: "key403_deleted5", tomb: true},

			{pkey: "key501_created5", val: "created"},
		}
		expectedExising := []*kvt{}
		for i := range expected {
			if !expected[i].tomb {
				expectedExising = append(expectedExising, expected[i])
			}
		}

		t.Run("cursor", func(t *testing.T) {
			c := bucket.Cursor()
			defer c.Close()

			i := 0
			for k, v := c.First(); k != nil && i < len(expectedExising); k, v = c.Next() {
				assert.Equal(t, []byte(expectedExising[i].pkey), k)
				assert.Equal(t, []byte(expectedExising[i].val), v)
				i++
			}
			assert.Equal(t, i, len(expectedExising))
		})

		t.Run("get by primary", func(t *testing.T) {
			for i := range expected {
				val, err := bucket.Get([]byte(expected[i].pkey))

				assert.NoError(t, err)
				if expected[i].tomb {
					assert.Nil(t, val)
				} else {
					assert.Equal(t, []byte(expected[i].val), val)
				}
			}
		})

		t.Run("get by secondary 1", func(t *testing.T) {
			for i := range expected {
				val, err := bucket.GetBySecondary(0, []byte(secondaryKey0(expected[i].pkey)))

				assert.NoError(t, err)
				if expected[i].tomb {
					assert.Nil(t, val)
				} else {
					assert.Equal(t, []byte(expected[i].val), val)
				}
			}
		})

		t.Run("get by secondary 2", func(t *testing.T) {
			for i := range expected {
				val, err := bucket.GetBySecondary(1, []byte(secondaryKey1(expected[i].pkey)))

				assert.NoError(t, err)
				if expected[i].tomb {
					assert.Nil(t, val)
				} else {
					assert.Equal(t, []byte(expected[i].val), val)
				}
			}
		})

		t.Run("net count", func(t *testing.T) {
			assert.Equal(t, len(expectedExising), bucket.Count())
		})
	})
}

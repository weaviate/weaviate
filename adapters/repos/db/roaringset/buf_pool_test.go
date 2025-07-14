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

package roaringset

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufPoolFixedSync(t *testing.T) {
	t.Run("pool returns buffers of given cap", func(t *testing.T) {
		pool123 := NewBufPoolFixedSync(123)
		pool234 := NewBufPoolFixedSync(234)
		pool345 := NewBufPoolFixedSync(345)

		t.Run("buf1", func(t *testing.T) {
			buf1_123, put := pool123.Get()
			defer put()
			buf1_234, put := pool234.Get()
			defer put()
			buf1_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf1_123))
			assert.Equal(t, 123, cap(buf1_123))
			assert.Equal(t, 0, len(buf1_234))
			assert.Equal(t, 234, cap(buf1_234))
			assert.Equal(t, 0, len(buf1_345))
			assert.Equal(t, 345, cap(buf1_345))
		})

		t.Run("buf2", func(t *testing.T) {
			buf2_123, put := pool123.Get()
			defer put()
			buf2_234, put := pool234.Get()
			defer put()
			buf2_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf2_123))
			assert.Equal(t, 123, cap(buf2_123))
			assert.Equal(t, 0, len(buf2_234))
			assert.Equal(t, 234, cap(buf2_234))
			assert.Equal(t, 0, len(buf2_345))
			assert.Equal(t, 345, cap(buf2_345))
		})
	})
}

func TestBufPoolFixedInMemory(t *testing.T) {
	t.Run("pool returns buffers of given cap", func(t *testing.T) {
		pool123 := NewBufPoolFixedInMemory(123, 2)
		pool234 := NewBufPoolFixedInMemory(234, 2)
		pool345 := NewBufPoolFixedInMemory(345, 2)

		t.Run("buf1", func(t *testing.T) {
			buf1_123, put := pool123.Get()
			defer put()
			buf1_234, put := pool234.Get()
			defer put()
			buf1_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf1_123))
			assert.Equal(t, 123, cap(buf1_123))
			assert.Equal(t, 0, len(buf1_234))
			assert.Equal(t, 234, cap(buf1_234))
			assert.Equal(t, 0, len(buf1_345))
			assert.Equal(t, 345, cap(buf1_345))
		})

		t.Run("buf2", func(t *testing.T) {
			buf2_123, put := pool123.Get()
			defer put()
			buf2_234, put := pool234.Get()
			defer put()
			buf2_345, put := pool345.Get()
			defer put()

			assert.Equal(t, 0, len(buf2_123))
			assert.Equal(t, 123, cap(buf2_123))
			assert.Equal(t, 0, len(buf2_234))
			assert.Equal(t, 234, cap(buf2_234))
			assert.Equal(t, 0, len(buf2_345))
			assert.Equal(t, 345, cap(buf2_345))
		})
	})

	t.Run("pool reuses buffers up to given limit", func(t *testing.T) {
		val1 := uint16(1001)
		val2 := uint16(2002)
		val3 := uint16(3003)
		val4 := uint16(4004)
		val5 := uint16(5005)

		// pool has 3 buffers. first 3 buffers got from the pool are reused
		// (once written values stay in the buffers).
		// following buffers are created as temporary ones and are not put back to the pool
		limit := 3
		pool := NewBufPoolFixedInMemory(2, limit)

		t.Run("get buffers and write unique values", func(t *testing.T) {
			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)

			buf2_1, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_1[:2], val2)

			buf3_1, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_1[:2], val3)

			buf4_1, put4 := pool.Get()
			binary.BigEndian.PutUint16(buf4_1[:2], val4)

			buf5_1, put5 := pool.Get()
			binary.BigEndian.PutUint16(buf5_1[:2], val5)

			// put in order
			put1()
			put2()
			put3()
			put4() // should be discarded
			put5() // should be discarded
		})

		t.Run("get buffers - only 3 (limit) have values", func(t *testing.T) {
			buf1_2, put1 := pool.Get()
			val1_2 := binary.BigEndian.Uint16(buf1_2[:2])

			buf2_2, put2 := pool.Get()
			val2_2 := binary.BigEndian.Uint16(buf2_2[:2])

			buf3_2, put3 := pool.Get()
			val3_2 := binary.BigEndian.Uint16(buf3_2[:2])

			buf4_2, put4 := pool.Get()
			val4_2 := binary.BigEndian.Uint16(buf4_2[:2])

			buf5_2, put5 := pool.Get()
			val5_2 := binary.BigEndian.Uint16(buf5_2[:2])

			assert.Equal(t, val1, val1_2)
			assert.Equal(t, val2, val2_2)
			assert.Equal(t, val3, val3_2)
			assert.Equal(t, uint16(0), val4_2)
			assert.Equal(t, uint16(0), val5_2)

			// write again to temp buffers
			binary.BigEndian.PutUint16(buf4_2[:2], val4)
			binary.BigEndian.PutUint16(buf5_2[:2], val5)

			// put in reverse order
			put5()
			put4()
			put3()
			put2() // should be discarded
			put1() // should be discarded
		})

		t.Run("get buffers - only 3 (limit) have values (in reverse order)", func(t *testing.T) {
			buf5_3, put := pool.Get()
			val5_3 := binary.BigEndian.Uint16(buf5_3[:2])
			defer put()

			buf4_3, put := pool.Get()
			val4_3 := binary.BigEndian.Uint16(buf4_3[:2])
			defer put()

			buf3_3, put := pool.Get()
			val3_3 := binary.BigEndian.Uint16(buf3_3[:2])
			defer put()

			buf2_3, put := pool.Get()
			val2_3 := binary.BigEndian.Uint16(buf2_3[:2])
			defer put()

			buf1_3, put := pool.Get()
			val1_3 := binary.BigEndian.Uint16(buf1_3[:2])
			defer put()

			assert.Equal(t, uint16(0), val1_3)
			assert.Equal(t, uint16(0), val2_3)
			assert.Equal(t, val3, val3_3)
			assert.Equal(t, val4, val4_3)
			assert.Equal(t, val5, val5_3)
		})
	})

	t.Run("pool creates buffers lazily", func(t *testing.T) {
		val1 := uint16(1001)
		val2 := uint16(2002)
		val3 := uint16(3003)
		limit := 3

		t.Run("1 buffer used at once, 1 buffer is created", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			put1()

			buf1_2, put := pool.Get()
			val1_2 := binary.BigEndian.Uint16(buf1_2[:2])
			put()

			assert.Equal(t, val1, val1_2)
		})

		t.Run("2 buffers used at once, 2 buffers are created", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_3, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_3[:2], val1)
			buf2_3, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_3[:2], val2)
			put1()
			put2()

			buf1_4, put := pool.Get()
			val1_4 := binary.BigEndian.Uint16(buf1_4[:2])
			put()
			buf2_4, put := pool.Get()
			val2_4 := binary.BigEndian.Uint16(buf2_4[:2])
			put()

			assert.Equal(t, val1, val1_4)
			assert.Equal(t, val2, val2_4)
		})

		t.Run("3 buffers used at once, 3 buffers are created", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_5, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_5[:2], val1)
			buf2_5, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_5[:2], val2)
			buf3_5, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_5[:2], val3)
			put1()
			put2()
			put3()

			buf1_6, put := pool.Get()
			val1_6 := binary.BigEndian.Uint16(buf1_6[:2])
			put()
			buf2_6, put := pool.Get()
			val2_6 := binary.BigEndian.Uint16(buf2_6[:2])
			put()
			buf3_6, put := pool.Get()
			val3_6 := binary.BigEndian.Uint16(buf3_6[:2])
			put()

			assert.Equal(t, val1, val1_6)
			assert.Equal(t, val2, val2_6)
			assert.Equal(t, val3, val3_6)
		})
	})

	t.Run("pool cleanup unused buffers", func(t *testing.T) {
		val1 := uint16(1001)
		val2 := uint16(2002)
		val3 := uint16(3003)
		limit := 3

		t.Run("all buffers in use, nothing is cleaned up", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			buf2_1, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_1[:2], val2)
			buf3_1, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_1[:2], val3)

			cleaned := pool.Cleanup(limit)
			put1()
			put2()
			put3()

			buf1_2, put := pool.Get()
			val1_2 := binary.BigEndian.Uint16(buf1_2[:2])
			put()
			buf2_2, put := pool.Get()
			val2_2 := binary.BigEndian.Uint16(buf2_2[:2])
			put()
			buf3_2, put := pool.Get()
			val3_2 := binary.BigEndian.Uint16(buf3_2[:2])
			put()

			assert.Equal(t, 0, cleaned)
			assert.Equal(t, val1, val1_2)
			assert.Equal(t, val2, val2_2)
			assert.Equal(t, val3, val3_2)
		})

		t.Run("2 buffers in use, 1 is cleaned up", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			buf2_1, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_1[:2], val2)
			buf3_1, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_1[:2], val3)

			put1()
			cleaned := pool.Cleanup(limit)
			put2()
			put3()

			buf2_2, put := pool.Get()
			val2_2 := binary.BigEndian.Uint16(buf2_2[:2])
			put()
			buf3_2, put := pool.Get()
			val3_2 := binary.BigEndian.Uint16(buf3_2[:2])
			put()
			buf2_3, put := pool.Get()
			val2_3 := binary.BigEndian.Uint16(buf2_3[:2])
			put()
			buf3_3, put := pool.Get()
			val3_3 := binary.BigEndian.Uint16(buf3_3[:2])
			put()

			assert.Equal(t, 1, cleaned)
			assert.Equal(t, val2, val2_2)
			assert.Equal(t, val3, val3_2)
			assert.Equal(t, val2, val2_3)
			assert.Equal(t, val3, val3_3)
		})

		t.Run("1 buffer in use, 2 are cleaned up", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			buf2_1, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_1[:2], val2)
			buf3_1, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_1[:2], val3)

			put1()
			put2()
			cleaned := pool.Cleanup(limit)
			put3()

			buf3_2, put := pool.Get()
			val3_2 := binary.BigEndian.Uint16(buf3_2[:2])
			put()
			buf3_3, put := pool.Get()
			val3_3 := binary.BigEndian.Uint16(buf3_3[:2])
			put()

			assert.Equal(t, 2, cleaned)
			assert.Equal(t, val3, val3_2)
			assert.Equal(t, val3, val3_3)
		})

		t.Run("no buffers in use, all are cleaned up", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			buf2_1, put2 := pool.Get()
			binary.BigEndian.PutUint16(buf2_1[:2], val2)
			buf3_1, put3 := pool.Get()
			binary.BigEndian.PutUint16(buf3_1[:2], val3)

			put1()
			put2()
			put3()
			cleaned := pool.Cleanup(limit)

			buf0_2, put := pool.Get()
			val0_2 := binary.BigEndian.Uint16(buf0_2[:2])
			put()

			assert.Equal(t, 3, cleaned)
			assert.Equal(t, uint16(0), val0_2)
		})
	})
}

func TestBitmapBufPoolRanged(t *testing.T) {
	t.Run("pool returns buffers of next higher range", func(t *testing.T) {
		ranges := []int{32, 64, 128, 256, 512, 1024}

		testCases := []struct {
			cap         int
			expectedCap int
		}{
			{
				cap:         1,
				expectedCap: 32,
			},
			{
				cap:         16,
				expectedCap: 32,
			},
			{
				cap:         32,
				expectedCap: 32,
			},
			{
				cap:         33,
				expectedCap: 64,
			},
			{
				cap:         64,
				expectedCap: 64,
			},
			{
				cap:         65,
				expectedCap: 128,
			},
			{
				cap:         128,
				expectedCap: 128,
			},
			{
				cap:         129,
				expectedCap: 256,
			},
			{
				cap:         256,
				expectedCap: 256,
			},
			{
				cap:         257,
				expectedCap: 512,
			},
			{
				cap:         512,
				expectedCap: 512,
			},
			{
				cap:         513,
				expectedCap: 1024,
			},
			{
				cap:         1025,
				expectedCap: 1025,
			},
			{
				cap:         2345,
				expectedCap: 2345,
			},
		}

		t.Run("sync pool", func(t *testing.T) {
			syncMaxBufSize := 1024
			pool := NewBitmapBufPoolRanged(syncMaxBufSize, nil, ranges...)

			for i, tc := range testCases {
				t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
					buf, put := pool.Get(tc.cap)
					defer put()

					assert.Equal(t, 0, len(buf))
					assert.Equal(t, tc.expectedCap, cap(buf))
				})
			}
		})

		t.Run("sync + inmemo pools", func(t *testing.T) {
			syncMaxBufSize := 256
			pool := NewBitmapBufPoolRanged(syncMaxBufSize, nil, ranges...)

			for i, tc := range testCases {
				t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
					buf, put := pool.Get(tc.cap)
					defer put()

					assert.Equal(t, 0, len(buf))
					assert.Equal(t, tc.expectedCap, cap(buf))
				})
			}
		})
	})

	t.Run("inmemo buffers are cleaned up", func(t *testing.T) {
		syncMaxBufSize := 128
		limits := map[int]int{256: 4, 512: 3, 1024: 2}
		ranges := []int{32, 64, 128, 256, 512, 1024}
		pool := NewBitmapBufPoolRanged(syncMaxBufSize, limits, ranges...)

		// get and write to 3 buffers of each inmemo size
		buf256_1, put256_1 := pool.Get(254)
		binary.BigEndian.PutUint16(buf256_1[:2], 10254)
		buf256_2, put256_2 := pool.Get(255)
		binary.BigEndian.PutUint16(buf256_2[:2], 10255)
		buf256_3, put256_3 := pool.Get(255)
		binary.BigEndian.PutUint16(buf256_3[:2], 10256)
		buf512_1, put512_1 := pool.Get(512)
		binary.BigEndian.PutUint16(buf512_1[:2], 10510)
		buf512_2, put512_2 := pool.Get(512)
		binary.BigEndian.PutUint16(buf512_2[:2], 10511)
		buf512_3, put512_3 := pool.Get(512)
		binary.BigEndian.PutUint16(buf512_3[:2], 10512)
		buf1024_1, put1024_1 := pool.Get(1024)
		binary.BigEndian.PutUint16(buf1024_1[:2], 11022)
		buf1024_2, put1024_2 := pool.Get(1024)
		binary.BigEndian.PutUint16(buf1024_2[:2], 11023)
		buf1024_3tmp, put1024_3tmp := pool.Get(1024)
		binary.BigEndian.PutUint16(buf1024_3tmp[:2], 11024)
		put256_1()
		put256_2()
		put256_3()
		put512_1()
		put512_2()
		put512_3()
		put1024_1()
		put1024_2()
		put1024_3tmp()

		buf256_4, put256_4 := pool.Get(256)
		val256_4 := binary.BigEndian.Uint16(buf256_4[:2])
		buf256_5, put256_5 := pool.Get(256)
		val256_5 := binary.BigEndian.Uint16(buf256_5[:2])
		buf256_6, put256_6 := pool.Get(256)
		val256_6 := binary.BigEndian.Uint16(buf256_6[:2])
		buf512_4, put512_4 := pool.Get(512)
		val512_4 := binary.BigEndian.Uint16(buf512_4[:2])
		buf512_5, put512_5 := pool.Get(512)
		val512_5 := binary.BigEndian.Uint16(buf512_5[:2])
		buf512_6, put512_6 := pool.Get(512)
		val512_6 := binary.BigEndian.Uint16(buf512_6[:2])
		buf1024_4, put1024_4 := pool.Get(1024)
		val1024_4 := binary.BigEndian.Uint16(buf1024_4[:2])
		buf1024_5, put1024_5 := pool.Get(1024)
		val1024_5 := binary.BigEndian.Uint16(buf1024_5[:2])
		buf1024_6tmp, put1024_6tmp := pool.Get(1024)
		val1024_6tmp := binary.BigEndian.Uint16(buf1024_6tmp[:2])
		put256_4()
		put256_5()
		put256_6()
		put512_4()
		put512_5()
		put512_6()
		put1024_4()
		put1024_5()
		put1024_6tmp()

		assert.Equal(t, uint16(10254), val256_4)
		assert.Equal(t, uint16(10255), val256_5)
		assert.Equal(t, uint16(10256), val256_6)
		assert.Equal(t, uint16(10510), val512_4)
		assert.Equal(t, uint16(10511), val512_5)
		assert.Equal(t, uint16(10512), val512_6)
		assert.Equal(t, uint16(11022), val1024_4)
		assert.Equal(t, uint16(11023), val1024_5)
		assert.Equal(t, uint16(0), val1024_6tmp)

		cleaned := pool.cleanup(3)

		// 3 of 4 256s, 3 of 3 512s, 2 of 2 1024s buffers should be cleaned
		assert.Equal(t, map[int]int{256: 3, 512: 3, 1024: 2}, cleaned)

		buf256_7, put := pool.Get(256)
		val256_7 := binary.BigEndian.Uint16(buf256_7[:2])
		put()
		buf512_7, put := pool.Get(512)
		val512_7 := binary.BigEndian.Uint16(buf512_7[:2])
		put()
		buf1024_7, put := pool.Get(1024)
		val1024_7 := binary.BigEndian.Uint16(buf1024_7[:2])
		put()

		assert.Equal(t, uint16(0), val256_7)
		assert.Equal(t, uint16(0), val512_7)
		assert.Equal(t, uint16(0), val1024_7)
	})

	t.Run("inmemo buffers are cleaned up periodically", func(t *testing.T) {
		logger, _ := test.NewNullLogger()

		syncMaxBufSize := 128
		limits := map[int]int{256: 2, 512: 2, 1024: 2}
		ranges := []int{32, 64, 128, 256, 512, 1024}
		pool := NewBitmapBufPoolRanged(syncMaxBufSize, limits, ranges...)

		buf256_1, put256_1 := pool.Get(256)
		binary.BigEndian.PutUint16(buf256_1[:2], 10256)
		buf512_1, put512_1 := pool.Get(512)
		binary.BigEndian.PutUint16(buf512_1[:2], 10512)
		buf1024_1, put1024_1 := pool.Get(1024)
		binary.BigEndian.PutUint16(buf1024_1[:2], 11024)
		put256_1()
		put512_1()
		put1024_1()

		buf256_2, put := pool.Get(256)
		val256_2 := binary.BigEndian.Uint16(buf256_2[:2])
		put()
		buf512_2, put := pool.Get(512)
		val512_2 := binary.BigEndian.Uint16(buf512_2[:2])
		put()
		buf1024_2, put := pool.Get(1024)
		val1024_2 := binary.BigEndian.Uint16(buf1024_2[:2])
		put()

		assert.Equal(t, uint16(10256), val256_2)
		assert.Equal(t, uint16(10512), val512_2)
		assert.Equal(t, uint16(11024), val1024_2)

		stop := pool.StartPeriodicCleanup(logger, 2, 500*time.Microsecond)
		defer stop()

		// wait for cleanup
		time.Sleep(5 * time.Millisecond)

		buf256_3, put := pool.Get(256)
		val256_3 := binary.BigEndian.Uint16(buf256_3[:2])
		put()
		buf512_3, put := pool.Get(512)
		val512_3 := binary.BigEndian.Uint16(buf512_3[:2])
		put()
		buf1024_3, put := pool.Get(1024)
		val1024_3 := binary.BigEndian.Uint16(buf1024_3[:2])
		put()

		assert.Equal(t, uint16(0), val256_3)
		assert.Equal(t, uint16(0), val512_3)
		assert.Equal(t, uint16(0), val1024_3)
	})
}

func TestCalculateSyncBufferRanges(t *testing.T) {
	testCases := []struct {
		minRangeP2     int
		maxRangeP2     int
		expectedRanges []int
	}{
		{
			minRangeP2:     1,
			maxRangeP2:     5,
			expectedRanges: []int{2, 4, 8, 16, 32},
		},
		{
			minRangeP2:     7,
			maxRangeP2:     10,
			expectedRanges: []int{128, 256, 512, 1024},
		},
		{
			minRangeP2:     9,
			maxRangeP2:     20,
			expectedRanges: []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576},
		},
		{
			minRangeP2:     7,
			maxRangeP2:     10,
			expectedRanges: []int{128, 256, 512, 1024},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     0,
			expectedRanges: []int{1},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     1,
			expectedRanges: []int{1, 2},
		},
		{
			minRangeP2:     -1,
			maxRangeP2:     0,
			expectedRanges: []int{},
		},
		{
			minRangeP2:     0,
			maxRangeP2:     -1,
			expectedRanges: []int{},
		},
		{
			minRangeP2:     9,
			maxRangeP2:     7,
			expectedRanges: []int{},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
			ranges := calculateSyncBufferRanges(tc.minRangeP2, tc.maxRangeP2)
			require.Equal(t, tc.expectedRanges, ranges)
		})
	}
}

func TestCalculateInMemoBufferRangesAndLimits(t *testing.T) {
	MiB := 1 << 20
	GiB := 1 << 30

	testCases := []struct {
		maxSyncBufSize int
		minRangeP2     int
		maxBufSize     int
		maxMemoSize    int
		expectedRanges []int
		expectedLimits map[int]int
	}{
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     32768,
			maxMemoSize:    32768,
			expectedRanges: []int{2048, 4096, 8192, 16384},
			expectedLimits: map[int]int{2048: 2, 4096: 1, 8192: 1, 16384: 1},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     32768,
			maxMemoSize:    16384,
			expectedRanges: []int{2048, 4096, 8192},
			expectedLimits: map[int]int{2048: 2, 4096: 1, 8192: 1},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     32768,
			maxMemoSize:    65536,
			expectedRanges: []int{2048, 4096, 8192, 16384, 32768},
			expectedLimits: map[int]int{2048: 2, 4096: 1, 8192: 1, 16384: 1, 32768: 1},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     32768,
			maxMemoSize:    262144,
			expectedRanges: []int{2048, 4096, 8192, 16384, 32768},
			expectedLimits: map[int]int{2048: 6, 4096: 5, 8192: 4, 16384: 4, 32768: 4},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     40000,
			maxMemoSize:    262144,
			expectedRanges: []int{2048, 4096, 8192, 16384, 32768, 40000},
			expectedLimits: map[int]int{2048: 6, 4096: 5, 8192: 4, 16384: 3, 32768: 2, 40000: 2},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     65536,
			maxMemoSize:    262144,
			expectedRanges: []int{2048, 4096, 8192, 16384, 32768, 65536},
			expectedLimits: map[int]int{2048: 4, 4096: 2, 8192: 2, 16384: 2, 32768: 2, 65536: 2},
		},
		{
			maxSyncBufSize: 1 * MiB,
			minRangeP2:     21, // 2^21 = 2MiB
			maxBufSize:     128 * MiB,
			maxMemoSize:    2 * GiB,
			expectedRanges: []int{2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 32 * MiB, 64 * MiB, 128 * MiB},
			expectedLimits: map[int]int{2 * MiB: 10, 4 * MiB: 9, 8 * MiB: 9, 16 * MiB: 8, 32 * MiB: 8, 64 * MiB: 8, 128 * MiB: 8},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     1024,
			maxMemoSize:    32768,
			expectedRanges: []int{},
			expectedLimits: map[int]int{},
		},
		{
			maxSyncBufSize: 1024,
			minRangeP2:     11, // 2^11 = 2048
			maxBufSize:     2048,
			maxMemoSize:    32768,
			expectedRanges: []int{2048},
			expectedLimits: map[int]int{2048: 16},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
			ranges, limits := calculateInMemoBufferRangesAndLimits(tc.maxSyncBufSize, tc.minRangeP2, tc.maxBufSize, tc.maxMemoSize)
			require.Equal(t, tc.expectedRanges, ranges)
			require.Equal(t, tc.expectedLimits, limits)
		})
	}
}

func TestValidateBufferRanges(t *testing.T) {
	testCases := []struct {
		ranges         []int
		expectedRanges []int
	}{
		{
			ranges:         []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			ranges:         []int{1, 2, 3, 1, 2, 3, 1, 2, 3},
			expectedRanges: []int{1, 2, 3},
		},
		{
			ranges:         []int{-3, -2, -1, 0, 1, 2, 3},
			expectedRanges: []int{1, 2, 3},
		},
		{
			ranges:         []int{3, 2, 1, 0, -1, -2, -3},
			expectedRanges: []int{1, 2, 3},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
			ranges := validateBufferRanges(tc.ranges)
			require.Equal(t, tc.expectedRanges, ranges)
		})
	}
}

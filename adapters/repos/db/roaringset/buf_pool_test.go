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

package roaringset

import (
	"encoding/binary"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	metrics := &bufPoolNoopMetrics{}

	t.Run("pool returns buffers of given cap", func(t *testing.T) {
		pool123 := NewBufPoolFixedInMemory(metrics, 123, 2)
		pool234 := NewBufPoolFixedInMemory(metrics, 234, 2)
		pool345 := NewBufPoolFixedInMemory(metrics, 345, 2)

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
		pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

			buf1_1, put1 := pool.Get()
			binary.BigEndian.PutUint16(buf1_1[:2], val1)
			put1()

			buf1_2, put := pool.Get()
			val1_2 := binary.BigEndian.Uint16(buf1_2[:2])
			put()

			assert.Equal(t, val1, val1_2)
		})

		t.Run("2 buffers used at once, 2 buffers are created", func(t *testing.T) {
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
			pool := NewBufPoolFixedInMemory(metrics, 2, limit)

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
	var metrics *monitoring.PrometheusMetrics = nil

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

		t.Run("sync pools return buffers of sizes", func(t *testing.T) {
			syncMaxBufSize := 1024 // all sync pools
			pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, nil, ranges...)

			for i, tc := range testCases {
				t.Run(fmt.Sprintf("test case #%d", i), func(t *testing.T) {
					buf, put := pool.Get(tc.cap)
					defer put()

					assert.Equal(t, 0, len(buf))
					assert.Equal(t, tc.expectedCap, cap(buf))
				})
			}
		})

		t.Run("sync + inmemo pools return buffers of sizes", func(t *testing.T) {
			syncMaxBufSize := 256 // sync pools + inmemo pools (512, 1024)
			pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, nil, ranges...)

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
		pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, limits, ranges...)

		// get and write to 3 inmemo buffers of each size
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
		// only 2 buffers will be returned to 1024 pool (due to limit=2)
		put256_1()
		put256_2()
		put256_3()
		put512_1()
		put512_2()
		put512_3()
		put1024_1()
		put1024_2()
		put1024_3tmp()

		// read data from buffers (buffers are not reset, previous values should still be there)
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
		// return buffers to the pool
		put256_4()
		put256_5()
		put256_6()
		put512_4()
		put512_5()
		put512_6()
		put1024_4()
		put1024_5()
		put1024_6tmp()

		// 3rd 1024 buffer should be empty (newly created)
		assert.Equal(t, uint16(10254), val256_4)
		assert.Equal(t, uint16(10255), val256_5)
		assert.Equal(t, uint16(10256), val256_6)
		assert.Equal(t, uint16(10510), val512_4)
		assert.Equal(t, uint16(10511), val512_5)
		assert.Equal(t, uint16(10512), val512_6)
		assert.Equal(t, uint16(11022), val1024_4)
		assert.Equal(t, uint16(11023), val1024_5)
		assert.Equal(t, uint16(0), val1024_6tmp)

		// remove up to 3 buffers from the pool
		cleaned := pool.cleanup(3)

		// 3 of 4 256s, 3 of 3 512s, 2 of 2 1024s buffers should be cleaned
		assert.Equal(t, map[int]int{256: 3, 512: 3, 1024: 2}, cleaned)

		// take 1 buffer of each size
		buf256_7, put := pool.Get(256)
		val256_7 := binary.BigEndian.Uint16(buf256_7[:2])
		put()
		buf512_7, put := pool.Get(512)
		val512_7 := binary.BigEndian.Uint16(buf512_7[:2])
		put()
		buf1024_7, put := pool.Get(1024)
		val1024_7 := binary.BigEndian.Uint16(buf1024_7[:2])
		put()

		// all buffers should be empty (newly created)
		assert.Equal(t, uint16(0), val256_7)
		assert.Equal(t, uint16(0), val512_7)
		assert.Equal(t, uint16(0), val1024_7)
	})

	t.Run("inmemo buffers are cleaned up periodically", func(t *testing.T) {
		logger, _ := test.NewNullLogger()

		syncMaxBufSize := 128
		limits := map[int]int{256: 2, 512: 2, 1024: 2}
		ranges := []int{32, 64, 128, 256, 512, 1024}
		pool := NewBitmapBufPoolRanged(metrics, syncMaxBufSize, limits, ranges...)

		// get and write to 1 inmemo buffer of each size
		buf256_1, put256_1 := pool.Get(256)
		binary.BigEndian.PutUint16(buf256_1[:2], 10256)
		buf512_1, put512_1 := pool.Get(512)
		binary.BigEndian.PutUint16(buf512_1[:2], 10512)
		buf1024_1, put1024_1 := pool.Get(1024)
		binary.BigEndian.PutUint16(buf1024_1[:2], 11024)
		// return buffers to the pool
		put256_1()
		put512_1()
		put1024_1()

		// read data from buffers (buffers are not reset, previous values should still be there)
		buf256_2, put := pool.Get(256)
		val256_2 := binary.BigEndian.Uint16(buf256_2[:2])
		put()
		buf512_2, put := pool.Get(512)
		val512_2 := binary.BigEndian.Uint16(buf512_2[:2])
		put()
		buf1024_2, put := pool.Get(1024)
		val1024_2 := binary.BigEndian.Uint16(buf1024_2[:2])
		put()

		// buffers contain previous values
		assert.Equal(t, uint16(10256), val256_2)
		assert.Equal(t, uint16(10512), val512_2)
		assert.Equal(t, uint16(11024), val1024_2)

		// remove buffers from the pool (periodically)
		synctest.Test(t, func(t *testing.T) {
			stop := pool.StartPeriodicCleanup(logger, 2, time.Millisecond)
			defer stop()

			// wait for cleanup
			time.Sleep(time.Millisecond)
			synctest.Wait()
		})

		// read data from buffers
		buf256_3, put := pool.Get(256)
		val256_3 := binary.BigEndian.Uint16(buf256_3[:2])
		put()
		buf512_3, put := pool.Get(512)
		val512_3 := binary.BigEndian.Uint16(buf512_3[:2])
		put()
		buf1024_3, put := pool.Get(1024)
		val1024_3 := binary.BigEndian.Uint16(buf1024_3[:2])
		put()

		// all buffers should be empty (newly created)
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

func TestCloneToBufGrowthHeadroom(t *testing.T) {
	const MiB = 1 << 20

	// hands back a buffer of exactly the requested capacity, so the test sees
	// the raw request instead of a ladder's rounded-up size class.
	newRecording := func() (*recordingBufPool, BitmapBufPool) {
		p := &recordingBufPool{}
		return p, p
	}

	t.Run("clone is asked for more than the source length", func(t *testing.T) {
		rec, pool := newRecording()
		src := prefilledOfAtLeast(2 * MiB)

		cloned, put := pool.CloneToBuf(src)
		defer put()

		require.Equal(t, withGrowthHeadroom(src.LenInBytes(), bitmapCloneGrowthFactor), rec.lastMinCap)
		require.Greater(t, rec.lastMinCap, src.LenInBytes())
		require.Equal(t, src.GetCardinality(), cloned.GetCardinality())
	})

	t.Run("a merge that adds containers stays inside the buffer", func(t *testing.T) {
		rec, pool := newRecording()
		src := prefilledOfAtLeast(2 * MiB)
		grower := bitmapAbove(src, 16)

		cloned, put := pool.CloneToBuf(src)
		defer put()
		cloned.Or(grower)

		// Outgrowing the buffer is what makes sroar swap in an unpooled slice.
		require.LessOrEqual(t, cloned.LenInBytes(), rec.lastCap)
	})

	t.Run("factor wrapper does not stack with the clone factor", func(t *testing.T) {
		rec, base := newRecording()
		src := prefilledOfAtLeast(2 * MiB)

		// below the clone factor: the clone factor wins
		NewBitmapBufPoolFactorWrapper(base, 1.1).CloneToBuf(src)
		require.Equal(t, withGrowthHeadroom(src.LenInBytes(), bitmapCloneGrowthFactor), rec.lastMinCap)

		// above it: the wrapper's own factor wins, and is not multiplied by it
		NewBitmapBufPoolFactorWrapper(base, 2.0).CloneToBuf(src)
		require.Equal(t, withGrowthHeadroom(src.LenInBytes(), 2.0), rec.lastMinCap)
	})

	t.Run("Get is untouched", func(t *testing.T) {
		rec, pool := newRecording()

		buf, put := pool.Get(1234)
		defer put()

		require.Equal(t, 1234, rec.lastMinCap)
		require.Equal(t, 1234, cap(buf))
	})
}

type recordingBufPool struct {
	lastMinCap int
	lastCap    int
}

func (p *recordingBufPool) Get(minCap int) ([]byte, func()) {
	p.lastMinCap = minCap
	buf := make([]byte, 0, minCap)
	p.lastCap = cap(buf)
	return buf, func() {}
}

func (p *recordingBufPool) CloneToBuf(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	return cloneToBuf(p, bm)
}

func prefilledOfAtLeast(bytes int) *sroar.Bitmap {
	ids := uint64(1 << 16)
	bm := sroar.Prefill(ids)
	for bm.LenInBytes() < bytes {
		ids += 1 << 16
		bm = sroar.Prefill(ids)
	}
	return bm
}

func bitmapAbove(src *sroar.Bitmap, containers int) *sroar.Bitmap {
	base := src.Maximum() + 1<<16
	bm := sroar.NewBitmap()
	for c := 0; c < containers; c++ {
		start := base + uint64(c)<<16
		for i := uint64(0); i < 1<<16; i += 2 {
			bm.Set(start + i)
		}
	}
	return bm
}

func TestValidateBufPoolSizes(t *testing.T) {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
	)

	// The shipped defaults, as configure_api passes them.
	const (
		defaultMaxBufSize = 32 * MiB
		defaultMaxMemory  = 128 * MiB
	)

	testCases := []struct {
		name       string
		maxBufSize int
		maxMemory  int
		expErr     string
	}{
		{
			name:       "shipped defaults",
			maxBufSize: defaultMaxBufSize,
			maxMemory:  defaultMaxMemory,
		},
		{
			name:       "smallest usable pair",
			maxBufSize: 2 * MiB,
			maxMemory:  2 * MiB,
		},
		{
			name:       "budget covers fewer classes than requested",
			maxBufSize: 32 * MiB,
			maxMemory:  40 * MiB,
		},
		{
			name:       "non power of two max buf size",
			maxBufSize: 3 * MiB,
			maxMemory:  32 * MiB,
		},
		{
			name:       "max buf size below the sync tier ceiling",
			maxBufSize: 512 * KiB,
			maxMemory:  defaultMaxMemory,
			expErr:     "leaves no in-memory buffer class",
		},
		{
			name:       "max buf size exactly at the sync tier ceiling",
			maxBufSize: 1 * MiB,
			maxMemory:  defaultMaxMemory,
			expErr:     "leaves no in-memory buffer class",
		},
		{
			name:       "budget below the smallest in-memory class",
			maxBufSize: 2 * MiB,
			maxMemory:  1 * MiB,
			expErr:     "exceeds the total buffer budget",
		},
		{
			name:       "budget below the smallest in-memory class, matching buf size",
			maxBufSize: 1 * MiB,
			maxMemory:  1 * MiB,
			expErr:     "leaves no in-memory buffer class",
		},
		{
			name:       "default max buf size against a 1MiB budget",
			maxBufSize: defaultMaxBufSize,
			maxMemory:  1 * MiB,
			expErr:     "exceeds the total buffer budget",
		},
		{
			name:       "max buf size above the budget",
			maxBufSize: 32 * MiB,
			maxMemory:  4 * MiB,
			expErr:     "exceeds the total buffer budget",
		},
		{
			name:       "max buf size one byte above the budget",
			maxBufSize: 2*MiB + 1,
			maxMemory:  2 * MiB,
			expErr:     "exceeds the total buffer budget",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBufPoolSizes(tc.maxBufSize, tc.maxMemory)

			if tc.expErr != "" {
				require.ErrorContains(t, err, tc.expErr)
				return
			}
			require.NoError(t, err)

			// An accepted pair must build a pool with a non-empty in-memory tier.
			logger, _ := test.NewNullLogger()
			pool, stop := NewBitmapBufPoolDefault(logger, nil, tc.maxBufSize, tc.maxMemory)
			defer stop()
			ranged := pool.(*bitmapBufPoolRanged)
			require.NotEmpty(t, ranged.poolsInMemo)
			require.Greater(t, ranged.ranges[len(ranged.ranges)-1], 1<<bufPoolSyncMaxRangeP2)
		})
	}
}

// Gates growth headroom against the shipped defaults, not a test double or a
// non-default knob.
func TestDefaultConfigAppliesCloneGrowthHeadroom(t *testing.T) {
	const MiB = 1 << 20

	// Sized to sit in the top 20% of the 2MiB class, where 1.25x moves the
	// request into the next one. That is the whole behaviour change.
	const topOfClassIDs = 14_000_000

	newPool := func(t *testing.T) *bitmapBufPoolRanged {
		logger, _ := test.NewNullLogger()
		pool, stop := NewBitmapBufPoolDefault(logger, nil, 32*MiB, 128*MiB)
		t.Cleanup(stop)
		return pool.(*bitmapBufPoolRanged)
	}

	// servedClass reports the size class a returned buffer landed in, by
	// draining the in-memory pools after the buffer was put back.
	servedClass := func(p *bitmapBufPoolRanged) int {
		for i, inMemo := range p.poolsInMemo {
			select {
			case ptr := <-inMemo.bufsCh:
				require.Equal(t, p.ranges[p.firstInMemoRngIdx+i], cap(*ptr))
				return p.ranges[p.firstInMemoRngIdx+i]
			default:
			}
		}
		return 0
	}

	src := sroar.Prefill(topOfClassIDs)
	require.Greater(t, withGrowthHeadroom(src.LenInBytes(), bitmapCloneGrowthFactor), 2*MiB)
	require.LessOrEqual(t, src.LenInBytes(), 2*MiB)

	t.Run("the production pool clones a class above the source", func(t *testing.T) {
		p := newPool(t)

		_, put := p.Get(src.LenInBytes())
		put()
		require.Equal(t, 2*MiB, servedClass(p))

		_, put = p.CloneToBuf(src)
		put()
		require.Equal(t, 4*MiB, servedClass(p))
	})

	t.Run("the whole-shard clone inherits it", func(t *testing.T) {
		p := newPool(t)
		bmf := NewBitmapFactory(p, func() uint64 { return topOfClassIDs - defaultIdIncrement })

		bm, release := bmf.GetBitmap()
		require.Equal(t, topOfClassIDs-defaultIdIncrement, bm.Maximum())
		release()

		require.Equal(t, 4*MiB, servedClass(p))
	})
}

// Warns when a pool skips config validation, or a budget silently drops the
// largest requested class — nothing else surfaces it.
func TestBufPoolWarnsOnDegradedLadder(t *testing.T) {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
	)

	testCases := []struct {
		name       string
		maxBufSize int
		maxMemory  int
		expWarn    string
	}{
		{
			name:       "shipped defaults",
			maxBufSize: 32 * MiB,
			maxMemory:  128 * MiB,
		},
		{
			name:       "no in-memory class at all",
			maxBufSize: 512 * KiB,
			maxMemory:  128 * MiB,
			expWarn:    "leaves no in-memory buffer class",
		},
		{
			name:       "budget covers fewer classes than requested",
			maxBufSize: 32 * MiB,
			maxMemory:  40 * MiB,
			expWarn:    "only covers in-memory buffer classes up to 16 MiB",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			_, stop := NewBitmapBufPoolDefault(logger, nil, tc.maxBufSize, tc.maxMemory)
			defer stop()

			if tc.expWarn == "" {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			require.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
			require.Contains(t, hook.LastEntry().Message, tc.expWarn)
		})
	}
}

func TestCloneBufSize(t *testing.T) {
	const MiB = 1 << 20

	// A clone sized from a wider bound than its source has to end up asking for
	// the same thing CloneToBuf would have asked for at that size.
	rec := &recordingBufPool{}
	src := prefilledOfAtLeast(2 * MiB)
	wider := src.LenInBytes() * 2

	rec.Get(CloneBufSize(wider))
	viaHelper := rec.lastMinCap

	widerBm := prefilledOfAtLeast(wider)
	rec.CloneToBuf(widerBm)

	require.Equal(t, CloneBufSize(widerBm.LenInBytes()), rec.lastMinCap)
	require.Greater(t, viaHelper, wider)
}

// The disposable counter is the only one that fires in the degraded state the
// buffer-pool bounds exist to prevent, so its size label has to line up with
// the inmemo_* labels for the same request. It rounded an exact power of two up
// a second time, overstating by 2x-4x.
func TestSizeClassCeil(t *testing.T) {
	tests := []struct {
		sizeInBytes int
		want        uint64
		label       string
	}{
		{sizeInBytes: 0, want: 1, label: "1 B"},
		{sizeInBytes: 1, want: 1, label: "1 B"},
		{sizeInBytes: 2, want: 2, label: "2 B"},
		{sizeInBytes: 3, want: 4, label: "4 B"},
		{sizeInBytes: 512, want: 512, label: "512 B"},
		{sizeInBytes: 513, want: 1024, label: "1.0 KiB"},
		{sizeInBytes: 1 << 20, want: 1 << 20, label: "1.0 MiB"},
		{sizeInBytes: (1 << 20) + 1, want: 1 << 21, label: "2.0 MiB"},
		{sizeInBytes: 1 << 22, want: 1 << 22, label: "4.0 MiB"},
		{sizeInBytes: 3 << 20, want: 1 << 22, label: "4.0 MiB"},
	}

	for _, tt := range tests {
		t.Run(humanize.IBytes(uint64(tt.sizeInBytes)), func(t *testing.T) {
			require.Equal(t, tt.want, sizeClassCeil(tt.sizeInBytes))
			require.Equal(t, tt.label, humanize.IBytes(sizeClassCeil(tt.sizeInBytes)))
		})
	}
}

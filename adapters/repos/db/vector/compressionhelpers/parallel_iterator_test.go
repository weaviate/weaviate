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

package compressionhelpers

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompressedParallelIterator(t *testing.T) {
	tests := []iteratorTestCase{
		{
			name:      "single vector, many parallel routines",
			totalVecs: 1,
			parallel:  16,
		},
		{
			name:      "two vectors, many parallel routines",
			totalVecs: 2,
			parallel:  16,
		},
		{
			name:      "three vectors, many parallel routines",
			totalVecs: 3,
			parallel:  16,
		},
		{
			name:      "many vectors, many parallel routines",
			totalVecs: 1000,
			parallel:  16,
		},
		{
			name:      "many vectors, single routine",
			totalVecs: 1000,
			parallel:  1,
		},
		{
			name:      "many vectors, more than allocation size per routine, two routines",
			totalVecs: 2020,
			parallel:  2,
		},
		{
			name:      "one fewer vectors than routines",
			totalVecs: 5,
			parallel:  6,
		},
		{
			name:      "matching vectors and routines",
			totalVecs: 6,
			parallel:  6,
		},
		{
			name:      "one more vector than routines",
			totalVecs: 7,
			parallel:  6,
		},
	}

	quantization := []string{"pq", "bq", "sq"}
	testsWithQuantization := make([]iteratorTestCase, len(tests)*len(quantization))
	for i, test := range tests {
		for j, q := range quantization {
			test.quantization = q
			testsWithQuantization[i*len(quantization)+j] = test
		}
	}

	for _, test := range testsWithQuantization {
		t.Run(fmt.Sprintf("%s: %s", test.quantization, test.name), func(t *testing.T) {
			bucket := buildCompressedBucketForTest(t, test.totalVecs)
			defer bucket.Shutdown(context.Background())

			logger, _ := logrustest.NewNullLogger()
			loadId := binary.BigEndian.Uint64
			switch test.quantization {
			case "pq":
				assertValue := func(t *testing.T, vec VecAndID[byte]) {
					valAsUint64 := binary.LittleEndian.Uint64(vec.Vec)
					assert.Equal(t, vec.Id, valAsUint64)
				}
				q := &ProductQuantizer{}
				fromCompressed := q.FromCompressedBytesWithSubsliceBuffer
				cpi := NewParallelIterator(bucket, test.parallel, loadId, fromCompressed, logger)
				testIterator(t, cpi, test, assertValue)
			case "bq":
				assertValue := func(t *testing.T, vec VecAndID[uint64]) {
					assert.Equal(t, vec.Id, vec.Vec[0])
				}
				q := NewBinaryQuantizer(nil)
				fromCompressed := q.FromCompressedBytesWithSubsliceBuffer
				cpi := NewParallelIterator(bucket, test.parallel, loadId, fromCompressed, logger)
				testIterator(t, cpi, test, assertValue)

			case "sq":
				assertValue := func(t *testing.T, vec VecAndID[byte]) {
					valAsUint64 := binary.LittleEndian.Uint64(vec.Vec)
					assert.Equal(t, vec.Id, valAsUint64)
				}
				q := &ScalarQuantizer{}
				fromCompressed := q.FromCompressedBytesWithSubsliceBuffer
				cpi := NewParallelIterator(bucket, test.parallel, loadId, fromCompressed, logger)
				testIterator(t, cpi, test, assertValue)

			default:
				t.Fatalf("unknown quantization: %s", test.quantization)
			}
		})
	}
}

type iteratorTestCase struct {
	name         string
	totalVecs    int
	parallel     int
	quantization string
}

func testIterator[T uint64 | byte](t *testing.T, cpi *parallelIterator[T], test iteratorTestCase,
	assertValue func(t *testing.T, vec VecAndID[T]),
) {
	require.NotNil(t, cpi)

	ch := cpi.IterateAll()
	idsFound := make(map[uint64]struct{})
	for vecs := range ch {
		for _, vec := range vecs {
			if _, ok := idsFound[vec.Id]; ok {
				t.Errorf("id %d found more than once", vec.Id)
			}
			idsFound[vec.Id] = struct{}{}

			assertValue(t, vec)
		}
	}

	// assert all ids are found
	// we already know that the ids are unique, so we can just check the
	// length
	require.Len(t, idsFound, test.totalVecs)
}

func buildCompressedBucketForTest(t *testing.T, totalVecs int) *lsmkv.Bucket {
	ctx := context.Background()
	logger, _ := logrustest.NewNullLogger()
	bucket, err := lsmkv.NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithPread(true), lsmkv.WithSegmentsChecksumValidationEnabled(false))
	require.Nil(t, err)

	for i := 0; i < totalVecs; i++ {
		key := make([]byte, 8)
		val := make([]byte, 8)

		binary.BigEndian.PutUint64(key, uint64(i))
		// make the actual vector the same as the key that makes it easy to do some
		// basic checks
		binary.LittleEndian.PutUint64(val, uint64(i))

		err := bucket.Put(key, val)
		require.Nil(t, err)
	}

	require.Nil(t, bucket.FlushAndSwitch())

	return bucket
}

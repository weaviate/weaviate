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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompressedParallelIterator(t *testing.T) {
	type test struct {
		name      string
		totalVecs int
		parallel  int
	}

	tests := []test{
		{
			name:      "single vector, many parallel routines",
			totalVecs: 1,
			parallel:  16,
		},
		{
			name:      "two vectors, many parallel routines",
			totalVecs: 5,
			parallel:  1,
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bucket := buildCompressedBucketForTest(t, test.totalVecs)
			defer bucket.Shutdown(context.Background())
			logger, _ := logrustest.NewNullLogger()
			loadId := binary.BigEndian.Uint64
			fromCompressed := fakeFromCompressedPQ
			cpi := NewCompressedParallelIterator(bucket, test.parallel, loadId,
				fromCompressed, logger)
			require.NotNil(t, cpi)

			ch := cpi.IterateAll()
			idsFound := make(map[uint64]struct{})
			for vecs := range ch {
				for _, vec := range vecs {
					if _, ok := idsFound[vec.id]; ok {
						t.Errorf("id %d found more than once", vec.id)
					}
					idsFound[vec.id] = struct{}{}

					valAsUint64 := binary.BigEndian.Uint64(vec.vec)
					assert.Equal(t, vec.id, valAsUint64)
				}
			}

			// assert all ids are found
			// we already know that the ids are unique, so we can just check the
			// length
			require.Len(t, idsFound, test.totalVecs)
		})
	}
}

func fakeFromCompressedPQ(in []byte) []byte {
	return in
}

func buildCompressedBucketForTest(t *testing.T, totalVecs int) *lsmkv.Bucket {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	bucket, err := lsmkv.NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithPread(true))
	require.Nil(t, err)

	for i := 0; i < totalVecs; i++ {
		key := make([]byte, 8)
		val := make([]byte, 8)

		binary.BigEndian.PutUint64(key, uint64(i))
		// make the actual vector the same as the key that makes it easy to do some
		// basic checks
		binary.BigEndian.PutUint64(val, uint64(i))

		err := bucket.Put(key, val)
		require.Nil(t, err)
	}

	require.Nil(t, bucket.FlushAndSwitch())

	return bucket
}

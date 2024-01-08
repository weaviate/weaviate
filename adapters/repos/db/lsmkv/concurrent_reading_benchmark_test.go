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
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func BenchmarkConcurrentReading(b *testing.B) {
	bucket, cleanup := prepareBucket(b)
	defer cleanup()
	keys := populateBucket(b, bucket)

	b.ReportAllocs()
	b.ResetTimer()

	routines := 500

	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		for r := 0; r < routines; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, key := range keys {
					_, err := bucket.MapList(key)
					assert.Nil(b, err)
				}
			}()
		}
		wg.Wait()
	}
}

func prepareBucket(b *testing.B) (bucket *Bucket, cleanup func()) {
	dirName := fmt.Sprintf("./testdata/%d", mustRandIntn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	bucket, err := NewBucket(testCtxB(), dirName, "", nullLoggerB(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyMapCollection),
		WithMemtableThreshold(5000))
	require.Nil(b, err)

	return bucket, func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}
}

func populateBucket(b *testing.B, bucket *Bucket) (keys [][]byte) {
	amount := 2000
	valuesPerKey := 4
	sizePerKey := 8
	sizePerValue := 32

	keys = make([][]byte, amount)
	values := make([][]MapPair, amount)

	for i := range keys {
		uuid, err := uuid.New().MarshalBinary()
		require.Nil(b, err)
		keys[i] = uuid

		values[i] = make([]MapPair, valuesPerKey)
		for j := range values[i] {
			values[i][j] = MapPair{
				Key:   make([]byte, sizePerKey),
				Value: make([]byte, sizePerValue),
			}
			rand.Read(values[i][j].Key)
			rand.Read(values[i][j].Value)
		}
	}

	wg := sync.WaitGroup{}
	for i := range keys {
		for j := 0; j < valuesPerKey; j++ {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(rowIndex, valueIndex int) {
				defer wg.Done()
				err := bucket.MapSet(keys[rowIndex], values[rowIndex][valueIndex])
				assert.Nil(b, err)
			}(i, j)
		}
	}
	wg.Wait()

	return
}

func testCtxB() context.Context {
	return context.Background()
}

func nullLoggerB() logrus.FieldLogger {
	log, _ := test.NewNullLogger()
	return log
}

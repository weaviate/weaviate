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

//go:build !race

package compressionhelpers_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_NoRaceRandomLASQDistanceByteToByte(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider()}
	vSize := 100
	qSize := 10
	dims := 150
	k := 10
	data, queries := testinghelpers.RandomVecsFixedSeed(vSize, qSize, dims)
	testinghelpers.Normalize(data)
	testinghelpers.Normalize(queries)
	for _, distancer := range distancers {
		sq := compressionhelpers.NewLocallyAdaptiveScalarQuantizer(data, distancer)
		neighbors := make([][]uint64, qSize)
		for j, y := range queries {
			neighbors[j], _ = testinghelpers.BruteForce(logrus.New(), data, y, k, distancerWrapper(distancer))
		}
		xCompressed := make([][]byte, vSize)
		for i, x := range data {
			xCompressed[i] = sq.Encode(x)
		}
		var relevant uint64
		mutex := sync.Mutex{}
		ellapsed := time.Duration(0)
		compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
			heap := priorityqueue.NewMax[any](k)
			cQuery := sq.Encode(queries[i])
			for j := range xCompressed {
				before := time.Now()
				d, _ := sq.DistanceBetweenCompressedVectors(xCompressed[j], cQuery)
				ell := time.Since(before)
				mutex.Lock()
				ellapsed += ell
				mutex.Unlock()
				if heap.Len() < k || heap.Top().Dist > d {
					if heap.Len() == k {
						heap.Pop()
					}
					heap.Insert(uint64(j), d)
				}
			}
			results := make([]uint64, 0, k)
			for heap.Len() > 0 {
				results = append(results, heap.Pop().ID)
			}
			hits := matchesInLists(neighbors[i][:k], results)
			mutex.Lock()
			relevant += hits
			mutex.Unlock()
		})

		recall := float32(relevant) / float32(k*len(queries))
		latency := float32(ellapsed.Microseconds()) / float32(len(queries))
		fmt.Println(distancer.Type(), recall, latency)
		assert.GreaterOrEqual(t, recall, float32(0.98), distancer.Type())
	}
}

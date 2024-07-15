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

package iterableconnections_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/iterableconnections"
)

func genVector(r *rand.Rand, size int) []uint64 {
	vector := make([]uint64, 0, size)
	for i := 0; i < size; i++ {
		vector = append(vector, r.Uint64())
	}
	return vector
}

func TestConstructor(t *testing.T) {
	vecs := genVector(rand.New(rand.NewSource(time.Now().UnixNano())), 1024)
	for i := range vecs {
		vecs[i] = vecs[i] % 100_000
	}
	iterator := iterableconnections.NewVarintIterableConnections(vecs)
	x, found := iterator.Next()
	i := 0
	for found {
		assert.Equal(t, x, vecs[i])
		x, found = iterator.Next()
		i++
	}
}

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

package segmentindex

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustRandUint64() uint64 {
	randInt, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Sprintf("mustRandUint64 error: %v", err))
	}
	return randInt.Uint64()
}

func TestBuildBalancedTree(t *testing.T) {
	size := 2000
	idealHeight := int(math.Ceil(math.Log2(float64(size))))
	fmt.Printf("ideal height would be %d\n", idealHeight)

	nodes := make([]Node, size)
	var tree Tree

	t.Run("generate random data", func(t *testing.T) {
		for i := range nodes {
			nodes[i].Key = make([]byte, 8)
			rand.Read(nodes[i].Key)

			nodes[i].Start = mustRandUint64()
			nodes[i].End = mustRandUint64()
		}
	})

	t.Run("insert", func(t *testing.T) {
		tree = NewBalanced(nodes)
	})

	t.Run("check height", func(t *testing.T) {
		assert.Equal(t, idealHeight, tree.Height())
	})

	t.Run("check values", func(t *testing.T) {
		for _, control := range nodes {
			k, s, e := tree.Get(control.Key)

			assert.Equal(t, control.Key, k)
			assert.Equal(t, control.Start, s)
			assert.Equal(t, control.End, e)
		}
	})
}

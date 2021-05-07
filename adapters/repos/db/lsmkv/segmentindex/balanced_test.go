package segmentindex

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

			nodes[i].Start = rand.Uint64()
			nodes[i].End = rand.Uint64()
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

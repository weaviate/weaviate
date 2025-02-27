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

package roaringsetrange

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("empty returns no nodes", func(t *testing.T) {
		m := NewMemtable(logger)
		nodes := m.Nodes()

		assert.Empty(t, nodes)
	})

	t.Run("returns only nodes for set bits - unique inserts", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{113, 213}) // ...1101
		m.Insert(5, []uint64{15, 25})    // ...0101
		m.Insert(0, []uint64{10, 20})    // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{113, 213}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("returns only nodes for set bits - overwriting inserts", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{11, 22, 33}) // ...1101
		m.Insert(5, []uint64{11, 22})      // ...0101
		m.Insert(0, []uint64{11})          // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{33}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("returns only nodes for set bits - overwriting inserts with deletes", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{11, 22, 33}) // ...1101
		m.Delete(5, []uint64{11, 22})      // ...0101
		m.Insert(5, []uint64{11, 22})      // ...0101
		m.Delete(0, []uint64{11})          // ...0000
		m.Insert(0, []uint64{11})          // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 3+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22, 33}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())

		node3 := nodes[3]
		assert.Equal(t, uint8(4), node3.Key)
		assert.ElementsMatch(t, []uint64{33}, node3.Additions.ToArray())
		assert.True(t, node3.Deletions.IsEmpty())
	})

	t.Run("delete does not mind key value", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Delete(13, []uint64{33}) // ...1101
		m.Delete(5, []uint64{22})  // ...0101
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())
	})

	t.Run("deletes all", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{33}) // ...1101
		m.Delete(13, []uint64{33}) // ...1101
		m.Insert(5, []uint64{22})  // ...0101
		m.Insert(0, []uint64{11})  // ...0000
		m.Delete(5, []uint64{22})  // ...0101
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())
	})

	t.Run("deletes all but one", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{33}) // ...1101
		m.Delete(13, []uint64{33}) // ...1101
		m.Insert(5, []uint64{22})  // ...0101
		m.Insert(0, []uint64{11})  // ...0000
		m.Delete(0, []uint64{11})  // ...0000

		nodes := m.Nodes()
		require.Len(t, nodes, 2+1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.ElementsMatch(t, []uint64{22}, nodeNN.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{11, 22, 33}, nodeNN.Deletions.ToArray())

		node0 := nodes[1]
		assert.Equal(t, uint8(1), node0.Key)
		assert.ElementsMatch(t, []uint64{22}, node0.Additions.ToArray())
		assert.True(t, node0.Deletions.IsEmpty())

		node2 := nodes[2]
		assert.Equal(t, uint8(3), node2.Key)
		assert.ElementsMatch(t, []uint64{22}, node2.Additions.ToArray())
		assert.True(t, node2.Deletions.IsEmpty())
	})

	t.Run("delete removes values regardless of key being deleted", func(t *testing.T) {
		m := NewMemtable(logger)
		m.Insert(13, []uint64{33})              // ...00001101
		m.Insert(5, []uint64{22})               // ...00000101
		m.Insert(0, []uint64{11})               // ...00000000
		m.Delete(123, []uint64{11, 22, 33, 44}) // ...01111011

		nodes := m.Nodes()
		require.Len(t, nodes, 1)

		nodeNN := nodes[0]
		assert.Equal(t, uint8(0), nodeNN.Key)
		assert.True(t, nodeNN.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{11, 22, 33, 44}, nodeNN.Deletions.ToArray())
	})

	t.Run("cloned memtable is not mutated", func(t *testing.T) {
		assertSameNodes := func(t *testing.T, expNodes, nodes []*MemtableNode) {
			require.NotNil(t, nodes)
			require.Len(t, nodes, len(expNodes))
			for i := range expNodes {
				assert.Equal(t, expNodes[i].Key, nodes[i].Key)
				assert.ElementsMatch(t, expNodes[i].Additions.ToArray(), nodes[i].Additions.ToArray())
				assert.ElementsMatch(t, expNodes[i].Deletions.ToArray(), nodes[i].Deletions.ToArray())
			}
		}

		m := NewMemtable(logger)
		m.Insert(1, []uint64{11, 21, 31})
		m.Insert(2, []uint64{12, 22, 32})
		m.Insert(3, []uint64{13, 23, 33})
		m.Delete(4, []uint64{14, 24, 34})
		m.Delete(5, []uint64{15, 25, 35})
		mNodes := m.Nodes()

		c := m.Clone()
		cNodes := c.Nodes()

		assertSameNodes(t, mNodes, cNodes)

		m.Insert(13, []uint64{113, 213, 313})
		m.Delete(14, []uint64{114, 214, 314})
		mNodes2 := m.Nodes()
		cNodes2 := c.Nodes()

		assert.NotEqual(t, len(mNodes), len(mNodes2))
		assertSameNodes(t, cNodes, cNodes2)
	})
}

func BenchmarkMemtableInsert(b *testing.B) {
	logger, _ := test.NewNullLogger()
	count := uint64(100_000)
	keys := make([]uint64, count)

	// generate
	for i := range keys {
		bytes, err := lexicographicallySortableFloat64(float64(i) / 3)
		require.NoError(b, err)
		value := binary.BigEndian.Uint64(bytes)
		keys[i] = value
	}

	// shuffle
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

	val := make([]uint64, 1)
	for i := 0; i < b.N; i++ {
		m := NewMemtable(logger)
		for value := uint64(0); value < count; value++ {
			val[0] = value
			m.Insert(keys[value], val)
		}
	}
}

func BenchmarkMemtableFlatten(b *testing.B) {
	logger, _ := test.NewNullLogger()
	count := uint64(100_000)
	keys := make([]uint64, count)

	// generate
	for i := range keys {
		bytes, err := lexicographicallySortableFloat64(float64(i) / 3)
		require.NoError(b, err)
		value := binary.BigEndian.Uint64(bytes)
		keys[i] = value
	}

	// shuffle
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}

	val := make([]uint64, 1)
	m := NewMemtable(logger)
	for value := uint64(0); value < count; value++ {
		val[0] = value
		m.Insert(keys[value], val)
	}

	for i := 0; i < b.N; i++ {
		m.Nodes()
	}
}

func lexicographicallySortableFloat64(in float64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.BigEndian, in)
	if err != nil {
		return nil, errors.Wrap(err, "serialize float64 value as big endian")
	}

	var out []byte
	if in >= 0 {
		// on positive numbers only flip the sign
		out = buf.Bytes()
		firstByte := out[0] ^ 0x80
		out = append([]byte{firstByte}, out[1:]...)
	} else {
		// on negative numbers flip every bit
		out = make([]byte, 8)
		for i, b := range buf.Bytes() {
			out[i] = b ^ 0xFF
		}
	}

	return out, nil
}

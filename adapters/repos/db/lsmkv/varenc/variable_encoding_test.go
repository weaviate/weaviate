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

package varenc

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
)

func TestSimpleEncodeDecode(t *testing.T) {
	enc := &SimpleEncoder[uint64]{}

	valueCount := terms.BLOCK_SIZE
	enc.Init(valueCount)
	values := make([]uint64, valueCount)
	for i := 0; i < valueCount; i++ {
		values[i] = uint64(i)
	}

	packed := enc.Encode(values)
	assert.NotNil(t, packed)

	decoded := enc.Decode(packed)
	assert.Equal(t, values, decoded)
}

func TestVarIntEncodeDecode(t *testing.T) {
	enc := &VarIntEncoder{}
	valueCount := terms.BLOCK_SIZE
	enc.Init(valueCount)

	values := make([]uint64, valueCount)
	for i := 0; i < valueCount; i++ {
		values[i] = uint64(i)
	}

	packed := enc.Encode(values)
	assert.NotNil(t, packed)

	decoded := enc.Decode(packed)
	assert.Equal(t, values, decoded)
}

func TestVarIntDeltaEncodeDecode(t *testing.T) {
	enc := &VarIntDeltaEncoder{}
	valueCount := terms.BLOCK_SIZE
	enc.Init(valueCount)

	values := make([]uint64, valueCount)
	for i := 0; i < valueCount; i++ {
		values[i] = uint64(i)
	}

	packed := enc.Encode(values)
	assert.NotNil(t, packed)

	decoded := enc.Decode(packed)
	assert.Equal(t, values, decoded)
}

func TestCompareNonDeltaEncoders(t *testing.T) {
	encs := []VarEncEncoder[uint64]{
		&SimpleEncoder[uint64]{},
		&VarIntEncoder{},
	}

	sizes := make([]int, len(encs))

	valueCount := terms.BLOCK_SIZE

	for _, enc := range encs {
		enc.Init(valueCount)
	}

	values := make([]uint64, valueCount)
	for i := 0; i < valueCount; i++ {
		values[i] = uint64(rand.Uint32() / 2)
	}

	for i, enc := range encs {
		packed := enc.Encode(values)
		assert.NotNil(t, packed)

		sizes[i] = len(packed)

		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded)
	}

	for i := range encs {
		fmt.Printf("Encoder %d: %d %f\n", i, sizes[i], float64(sizes[0])/float64(sizes[i]))
	}
}

func TestCompareDeltaEncoders(t *testing.T) {
	encs := []VarEncEncoder[uint64]{
		&SimpleEncoder[uint64]{},
		&VarIntEncoder{},
		&VarIntDeltaEncoder{},
	}

	sizes := make([]int, len(encs))

	valueCount := terms.BLOCK_SIZE

	for _, enc := range encs {
		enc.Init(valueCount)
	}

	values := make([]uint64, valueCount)
	values[0] = 100
	for i := 1; i < valueCount; i++ {
		values[i] = values[i-1] + rand.Uint64N(10)
	}

	for i, enc := range encs {
		packed := enc.Encode(values)
		assert.NotNil(t, packed)

		sizes[i] = len(packed)

		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded)
	}

	for i := range encs {
		fmt.Printf("Encoder %d: %d %f\n", i, sizes[i], float64(sizes[0])/float64(sizes[i]))
	}
}

func BenchmarkDeltaEncoders(b *testing.B) {
	encs := []VarEncEncoder[uint64]{
		&SimpleEncoder[uint64]{},
		&VarIntEncoder{},
		&VarIntDeltaEncoder{},
	}

	valueCount := terms.BLOCK_SIZE

	for _, enc := range encs {
		enc.Init(valueCount)
	}

	values := make([]uint64, valueCount)
	values[0] = 100
	for i := 1; i < valueCount; i++ {
		values[i] = values[i-1] + rand.Uint64N(10)
	}

	for _, enc := range encs {
		b.Run(fmt.Sprintf("%T", enc), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				packed := enc.Encode(values)
				decoded := enc.Decode(packed)
				assert.Equal(b, values, decoded)
			}
		})
	}
}

func BenchmarkDecoder(b *testing.B) {
	encs := []VarEncEncoder[uint64]{
		&SimpleEncoder[uint64]{},
		&VarIntEncoder{},
		&VarIntDeltaEncoder{},
	}
	valueCount := terms.BLOCK_SIZE

	// Example input values
	values := make([]uint64, valueCount)

	values[0] = 100
	for i := range values[1:] {
		// values[i+1] = values[i] + rand.Uint64N(10)
		values[i+1] = values[i] + 1
		// values[i] = uint64(math.Round(rand.Float64()*10)) + 1
	}

	for _, enc := range encs {
		enc.Init(valueCount)
		packed := enc.Encode(values)
		b.Run(fmt.Sprintf("%T", enc), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				enc.Decode(packed)
			}
			b.ReportMetric(float64(((b.N*valueCount)/1000000))/b.Elapsed().Seconds(), "Mvalues/s")
		})

	}
}

func BenchmarkDecoderMulti(b *testing.B) {
	encs := [][]VarEncEncoder[uint64]{
		{&SimpleEncoder[uint64]{}, &SimpleEncoder[uint64]{}},
		{&VarIntDeltaEncoder{}, &VarIntEncoder{}},
	}

	valueCount := terms.BLOCK_SIZE

	// Example input values
	docIds := make([]uint64, valueCount)

	docIds[0] = 100
	tfs := make([]uint64, valueCount)
	tfs[0] = 1
	for i := range docIds[1:] {
		docIds[i+1] = docIds[i] + rand.Uint64N(10)
		// docIds[i+1] = docIds[i] + 1
		tfs[i] = uint64(math.Round(rand.Float64()*10)) + 1
	}

	for _, enc := range encs {
		enc[0].Init(valueCount)
		enc[1].Init(valueCount)

		packedDocIds := enc[0].Encode(docIds)
		packedTfs := enc[1].Encode(tfs)

		b.Run(fmt.Sprintf("%T", enc), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				enc[0].Decode(packedDocIds)
				enc[1].Decode(packedTfs)
			}
			b.ReportMetric(float64(((b.N*valueCount)/1000000))/b.Elapsed().Seconds(), "Mvalues/s")
		})

	}
}

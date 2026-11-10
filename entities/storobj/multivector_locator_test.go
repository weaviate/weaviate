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

package storobj

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/entities/models"
)

// TestMultiVectorTokenRange: the range the locator names, read out of the
// object's bytes, is the token MultiVectorFromBinary decodes, for objects
// whose header is shorter and longer than the first head.
func TestMultiVectorTokenRange(t *testing.T) {
	marshalClass := func(t *testing.T, class string, props map[string]interface{}, vector []float32, vectors map[string][]float32, multi map[string][][]float32) []byte {
		obj := FromObject(&models.Object{
			Class:      class,
			ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: props,
		}, vector, vectors, multi)
		obj.DocID = 7
		bin, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		return bin
	}
	marshal := func(t *testing.T, props map[string]interface{}, vector []float32, vectors map[string][]float32, multi map[string][][]float32) []byte {
		return marshalClass(t, "Doc", props, vector, vectors, multi)
	}
	mvA := [][]float32{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
	mvB := [][]float32{{1, 2, 3, 4, 5}, {6, 7, 8, 9, 10}}
	long := strings.Repeat("x", 3000)
	// a multi-vector of 50 tokens, which may come before the one asked for
	mvLong := make([][]float32, 50)
	for i := range mvLong {
		mvLong[i] = []float32{float32(i), 1, 2}
	}
	// a class name whose end leaves the next length prefix just past the
	// head asked for after the legacy vector
	longClass := "D" + strings.Repeat("c", 250)

	tests := []struct {
		name  string
		bin   []byte
		multi map[string][][]float32
		// firstHeadEnough: the locator finds the token in the first
		// MultiVectorLocatorHead bytes, as it must for an object without
		// properties
		firstHeadEnough bool
		// minRounds: the most heads any token of the object needs is at
		// least this, whatever the order of the multi-vectors in the
		// segment
		minRounds int
	}{
		{
			name: "no properties, no vectors", bin: marshal(t, nil, nil, nil, map[string][][]float32{"a": mvA}),
			multi: map[string][][]float32{"a": mvA}, firstHeadEnough: true,
		},
		{
			name:  "properties and a legacy vector",
			bin:   marshal(t, map[string]interface{}{"name": "x", "n": float64(17)}, []float32{1, 2, 3}, nil, map[string][][]float32{"a": mvA, "b": mvB}),
			multi: map[string][][]float32{"a": mvA, "b": mvB}, firstHeadEnough: true,
		},
		{
			name:  "long properties and a target vector",
			bin:   marshal(t, map[string]interface{}{"text": long}, nil, map[string][]float32{"t": make([]float32, 1024)}, map[string][][]float32{"a": mvA, "b": mvB}),
			multi: map[string][][]float32{"a": mvA, "b": mvB},
		},
		{
			name: "a legacy vector, long properties, a named vector and two multi-vectors",
			bin: marshal(t, map[string]interface{}{"text": long}, make([]float32, 300),
				map[string][]float32{"t": make([]float32, 1024)}, map[string][][]float32{"a": mvLong, "b": mvB}),
			multi: map[string][][]float32{"a": mvLong, "b": mvB},
		},
		{
			name:  "the same with a long class name",
			bin:   marshalClass(t, longClass, map[string]interface{}{"text": long[:1000]}, make([]float32, 300), map[string][]float32{"t": make([]float32, 64)}, map[string][][]float32{"a": mvLong, "b": mvB}),
			multi: map[string][][]float32{"a": mvLong, "b": mvB}, minRounds: 5,
		},
	}

	// locate walks the head as a reader does: the first head, then what the
	// locator asks for, at most MultiVectorLocatorRounds heads
	locate := func(t *testing.T, bin []byte, name string, pos, dims int) (uint64, uint64, int) {
		head := bin[:min(len(bin), MultiVectorLocatorHead)]
		for rounds := 1; ; rounds++ {
			offset, length, need, err := MultiVectorTokenRange(head, name, pos, dims)
			require.NoError(t, err)
			if need <= uint64(len(head)) {
				return offset, length, rounds
			}
			// need is a lower bound: a store reads the whole value at most
			require.Less(t, len(head), len(bin), "the locator asks for more than the object has")
			require.Less(t, rounds, MultiVectorLocatorRounds, "the locator keeps asking")
			head = bin[:min(need, uint64(len(bin)))]
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxRounds := 0
			for name, tokens := range tt.multi {
				want, err := MultiVectorFromBinary(tt.bin, name)
				require.NoError(t, err)
				require.Equal(t, tokens, want)
				for pos := range tokens {
					dims := len(tokens[pos])
					offset, length, rounds := locate(t, tt.bin, name, pos, dims)
					maxRounds = max(maxRounds, rounds)
					if tt.firstHeadEnough {
						require.Equal(t, 1, rounds, "token %d of %q", pos, name)
					}
					got, err := TokenFromBytes(tt.bin[offset:offset+length], dims, nil)
					require.NoError(t, err)
					require.Equal(t, tokens[pos], got, "token %d of %q", pos, name)

					// a reused buffer takes the floats
					buffer := make([]float32, dims)
					got, err = TokenFromBytes(tt.bin[offset:offset+length], dims, buffer)
					require.NoError(t, err)
					require.Equal(t, &buffer[0], &got[0])
				}
				// a position past the count
				_, _, _, err = MultiVectorTokenRange(tt.bin, name, len(tokens), len(tokens[0]))
				require.ErrorContains(t, err, "but token")
				// the wrong dimension: the range is wrong or outside the
				// segment, and the prefix check catches what the locator
				// cannot
				offset, length, need, err := MultiVectorTokenRange(tt.bin, name, 0, len(tokens[0])+1)
				if err == nil {
					require.Zero(t, need)
					_, err = TokenFromBytes(tt.bin[offset:offset+length], len(tokens[0])+1, nil)
				}
				require.Error(t, err)
			}
			require.GreaterOrEqual(t, maxRounds, tt.minRounds)
			// a missing target vector
			_, _, _, err := MultiVectorTokenRange(tt.bin, "missing", 0, 3)
			require.ErrorAs(t, err, &ErrTargetVectorNotFound{})
			_, _, docErr := MultiVectorFromBinaryInto(tt.bin, nil, "missing")
			require.EqualError(t, err, docErr.Error(), "the token and document readers agree")
			// a truncated head asks for more, never errs
			for _, n := range []int{0, 1, 30, 42, 60, 100} {
				_, _, need, err := MultiVectorTokenRange(tt.bin[:min(n, len(tt.bin))], "a", 0, 3)
				require.NoError(t, err)
				require.Greater(t, need, uint64(n), "head of %d bytes", n)
			}
		})
	}

	t.Run("the header of an object without properties", func(t *testing.T) {
		bin := marshal(t, nil, nil, nil, map[string][][]float32{"colbert": mvA})
		offset, _, rounds := locate(t, bin, "colbert", 0, 3)
		require.Equal(t, 1, rounds)
		// the block starts 4 bytes (the count) before token 0, at the
		// multi-vectors segment's start: everything before is the header
		t.Logf("header of %d bytes before the multi-vectors segment", offset-4)
		require.Less(t, offset-4, uint64(MultiVectorLocatorHead))
	})

	t.Run("another marshaller version", func(t *testing.T) {
		bin := marshal(t, nil, nil, nil, map[string][][]float32{"a": mvA})
		bin[0] = 2
		_, _, _, err := MultiVectorTokenRange(bin, "a", 0, 3)
		require.ErrorContains(t, err, "unsupported marshaller version")
	})

	t.Run("bad arguments", func(t *testing.T) {
		bin := marshal(t, nil, nil, nil, map[string][][]float32{"a": mvA})
		_, _, _, err := MultiVectorTokenRange(bin, "a", -1, 3)
		require.Error(t, err)
		_, _, _, err = MultiVectorTokenRange(bin, "a", 0, 0)
		require.Error(t, err)
		_, err = TokenFromBytes(make([]byte, 14), 4, nil)
		require.ErrorContains(t, err, "bytes")
	})
}

// TestMultiVectorTokenRangeOddObjects: objects the locator meets rarely. It
// gives the token or an error, never a wrong range.
func TestMultiVectorTokenRangeOddObjects(t *testing.T) {
	marshal := func(t *testing.T, multi map[string][][]float32) []byte {
		obj := FromObject(&models.Object{
			Class: "Doc",
			ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		}, nil, nil, multi)
		bin, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)
		return bin
	}
	// offsets returns where the offsets blob of a lone multi-vector "a" is
	offsets := func(t *testing.T, bin []byte) (start, end int) {
		encoded, err := msgpack.Marshal(map[string]uint32{"a": 0})
		require.NoError(t, err)
		start = bytes.Index(bin, encoded)
		require.Positive(t, start)
		return start, start + len(encoded)
	}
	mv := [][]float32{{1, 2, 3}, {4, 5, 6}}

	t.Run("no vector sections at all", func(t *testing.T) {
		bin := marshal(t, nil)
		_, _, _, err := MultiVectorTokenRange(bin, "a", 0, 3)
		require.ErrorAs(t, err, &ErrTargetVectorNotFound{})
	})

	t.Run("an empty multi-vector", func(t *testing.T) {
		bin := marshal(t, map[string][][]float32{"a": {}})
		_, _, _, err := MultiVectorTokenRange(bin, "a", 0, 3)
		require.ErrorContains(t, err, "has 0 tokens but token 0 is asked")
	})

	t.Run("corrupt offsets", func(t *testing.T) {
		bin := marshal(t, map[string][][]float32{"a": mv})
		start, end := offsets(t, bin)
		for i := start; i < end; i++ {
			bin[i] = 0xc1 // a byte msgpack never uses
		}
		_, _, _, err := MultiVectorTokenRange(bin, "a", 0, 3)
		require.ErrorContains(t, err, "could not unmarshal multi vectors offset")
	})

	t.Run("a block offset outside the segment", func(t *testing.T) {
		bin := marshal(t, map[string][][]float32{"a": mv})
		start, end := offsets(t, bin)
		blob, err := msgpack.Marshal(map[string]uint32{"a": 1 << 20})
		require.NoError(t, err)
		patched := binary.LittleEndian.AppendUint32(slices.Clone(bin[:start-4]), uint32(len(blob)))
		patched = append(append(patched, blob...), bin[end:]...)
		_, _, _, err = MultiVectorTokenRange(patched, "a", 0, 3)
		require.ErrorContains(t, err, "outside its segment")
	})

	t.Run("tokens of different lengths", func(t *testing.T) {
		// the locator assumes one length: dims. It names a range, and the
		// reader's check of the dimension prefix refuses what lies there.
		bin := marshal(t, map[string][][]float32{"a": {{1, 2}, {3, 4, 5, 6}, {7, 8}}})
		offset, length, need, err := MultiVectorTokenRange(bin, "a", 1, 2)
		require.NoError(t, err)
		require.Zero(t, need)
		_, err = TokenFromBytes(bin[offset:offset+length], 2, nil)
		require.ErrorContains(t, err, "dimensions")
	})
}

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

package lsmkv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type extractorKey struct {
	key       string
	value     []byte
	tombstone bool
}

func TestBufferedKeyAndTombstoneExtractor(t *testing.T) {
	keys := []extractorKey{
		{key: "alpha", value: []byte("value-alpha")},
		{key: "bravo", value: bytes.Repeat([]byte("b"), 4096), tombstone: true},
		{key: "charlie"},
	}

	type test struct {
		name                string
		keys                []extractorKey
		secondaryIndexCount uint16
		// dataStartPos in a real segment sits behind the header
		padding             uint64
		endBeforeStart      bool
		maxOutputBufferSize uint64
		expectedBufferSize  func(dataSize uint64) uint64
		expectedCycles      int
		expectedErr         string
	}

	tests := []test{
		{
			name:                "segment smaller than the cap",
			keys:                keys,
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(dataSize uint64) uint64 { return dataSize },
			expectedCycles:      1,
		},
		{
			name:                "segment smaller than the cap, behind a header",
			keys:                keys,
			padding:             30,
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(dataSize uint64) uint64 { return dataSize },
			expectedCycles:      1,
		},
		{
			name:                "segment smaller than the cap, with secondary keys",
			keys:                keys,
			secondaryIndexCount: 2,
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(dataSize uint64) uint64 { return dataSize },
			expectedCycles:      1,
		},
		{
			name:                "segment larger than the cap, so the buffer is flushed repeatedly",
			keys:                keys,
			maxOutputBufferSize: 24,
			expectedBufferSize:  func(uint64) uint64 { return 24 },
			expectedCycles:      2,
		},
		{
			name:                "single key",
			keys:                keys[:1],
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(dataSize uint64) uint64 { return dataSize },
			expectedCycles:      1,
		},
		{
			name:                "empty data section",
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(uint64) uint64 { return 0 },
			expectedCycles:      1,
		},
		{
			name:                "data section ending before it starts",
			keys:                keys,
			padding:             30,
			endBeforeStart:      true,
			maxOutputBufferSize: 10e6,
			expectedBufferSize:  func(uint64) uint64 { return 0 },
			expectedCycles:      1,
		},
		{
			name:                "key too large for the buffer",
			keys:                keys,
			maxOutputBufferSize: 8,
			expectedBufferSize:  func(uint64) uint64 { return 8 },
			expectedErr:         "does not fit the 8 byte output buffer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := writeReplaceNodes(t, test.keys, test.secondaryIndexCount)
			rawSegment := append(bytes.Repeat([]byte{0xFF}, int(test.padding)), data...)

			start := test.padding
			end := start + uint64(len(data))
			if test.endBeforeStart {
				end = start - 1
			}

			var extracted []extractorKey
			cb := func(key []byte, tombstone bool) {
				extracted = append(extracted, extractorKey{
					key: string(key), tombstone: tombstone,
				})
			}

			extr := newBufferedKeyAndTombstoneExtractor(rawSegment, start, end,
				test.maxOutputBufferSize, test.secondaryIndexCount, cb)
			assert.Equal(t, test.expectedBufferSize(uint64(len(data))),
				uint64(len(extr.outputBuffer)))

			err := extr.do()
			if test.expectedErr != "" {
				require.ErrorContains(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expectedCycles, extr.callbackCycle)

			var expected []extractorKey
			if !test.endBeforeStart {
				for _, key := range test.keys {
					expected = append(expected, extractorKey{
						key: key.key, tombstone: key.tombstone,
					})
				}
			}
			assert.Equal(t, expected, extracted)
		})
	}
}

func writeReplaceNodes(t *testing.T, keys []extractorKey, secondaryIndexCount uint16) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	for _, key := range keys {
		secondaryKeys := make([][]byte, secondaryIndexCount)
		for i := range secondaryKeys {
			secondaryKeys[i] = fmt.Appendf(nil, "secondary-%d-%s", i, key.key)
		}

		node := segmentReplaceNode{
			tombstone:           key.tombstone,
			value:               key.value,
			primaryKey:          []byte(key.key),
			secondaryIndexCount: secondaryIndexCount,
			secondaryKeys:       secondaryKeys,
		}
		_, err := node.KeyIndexAndWriteTo(buf)
		require.NoError(t, err)
	}

	return buf.Bytes()
}

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

package hashtree

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactHashTreeSerialization(t *testing.T) {
	capacity := uint64(math.MaxUint64)

	for h := 0; h < 10; h++ {
		ht, err := NewCompactHashTree(capacity, h)
		require.NoError(t, err)

		require.Equal(t, h, ht.Height())

		leavesCount := LeavesCount(ht.Height())
		valuePrefix := "somevalue"

		for i := 0; i < leavesCount; i++ {
			err = ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("%s%d", valuePrefix, i)))
			require.NoError(t, err)
		}

		var buf bytes.Buffer

		_, err = ht.Serialize(&buf)
		require.NoError(t, err)

		readBuf := bytes.NewBuffer(buf.Bytes())

		ht1, err := DeserializeCompactHashTree(readBuf)
		require.NoError(t, err)
		require.Equal(t, ht.Capacity(), ht1.Capacity())
		require.Equal(t, ht.Height(), ht1.Height())
		require.Equal(t, ht.Root(), ht1.Root())
	}
}

// TestCompactHashTreeDeserializationHeaderValidation pins the compact header checksum: real murmur and the legacy pre-fix echo (header bytes zero-padded) are accepted, corruption is rejected.
func TestCompactHashTreeDeserializationHeaderValidation(t *testing.T) {
	const height = 4

	ht, err := NewCompactHashTree(uint64(math.MaxUint64), height)
	require.NoError(t, err)
	for i := 0; i < LeavesCount(height); i++ {
		require.NoError(t, ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("somevalue%d", i))))
	}

	var buf bytes.Buffer
	_, err = ht.Serialize(&buf)
	require.NoError(t, err)
	valid := buf.Bytes()

	legacyChecksum := make([]byte, DigestLength)
	copy(legacyChecksum, valid[0:13])

	tests := []struct {
		name    string
		mutate  func(b []byte)
		wantErr string
	}{
		{name: "valid roundtrip", mutate: func(b []byte) {}},
		{name: "legacy echo checksum", mutate: func(b []byte) { copy(b[13:29], legacyChecksum) }},
		{name: "corrupt magic", mutate: func(b []byte) { b[0] ^= 0xff }, wantErr: "magic number mismatch"},
		{name: "corrupt version", mutate: func(b []byte) { b[4] ^= 0xff }, wantErr: "unsupported version"},
		{name: "corrupt capacity", mutate: func(b []byte) { b[5] ^= 0xff }, wantErr: "header checksum mismatch"},
		{name: "corrupt checksum", mutate: func(b []byte) { b[13] ^= 0xff }, wantErr: "header checksum mismatch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := bytes.Clone(valid)
			tc.mutate(b)

			ht1, err := DeserializeCompactHashTree(bytes.NewBuffer(b))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, ht.Root(), ht1.Root())
		})
	}
}

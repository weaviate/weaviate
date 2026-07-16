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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashTreeSerialization(t *testing.T) {
	for h := 1; h < 10; h++ {
		ht, err := NewHashTree(h)
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

		ht1, err := DeserializeHashTree(readBuf)
		require.NoError(t, err)
		require.Equal(t, ht.Height(), ht1.Height())
		require.Equal(t, ht.Root(), ht1.Root())
	}
}

// TestHashTreeDeserializationHeaderValidation: real and legacy checksums accepted, header corruption rejected.
func TestHashTreeDeserializationHeaderValidation(t *testing.T) {
	const height = 4

	ht, err := NewHashTree(height)
	require.NoError(t, err)
	for i := 0; i < LeavesCount(height); i++ {
		require.NoError(t, ht.AggregateLeafWith(uint64(i), []byte(fmt.Sprintf("somevalue%d", i))))
	}

	var buf bytes.Buffer
	_, err = ht.Serialize(&buf)
	require.NoError(t, err)
	valid := buf.Bytes()

	tests := []struct {
		name    string
		mutate  func(b []byte)
		wantErr string
	}{
		{name: "valid roundtrip", mutate: func(b []byte) {}},
		{name: "legacy echo checksum", mutate: func(b []byte) { copy(b[25:41], b[0:16]) }},
		{name: "corrupt magic", mutate: func(b []byte) { b[0] ^= 0xff }, wantErr: "magic number mismatch"},
		{name: "corrupt version", mutate: func(b []byte) { b[4] ^= 0xff }, wantErr: "unsupported version"},
		{name: "corrupt height", mutate: func(b []byte) { b[5] ^= 0xff }, wantErr: "header checksum mismatch"},
		{name: "corrupt root", mutate: func(b []byte) { b[9] ^= 0xff }, wantErr: "header checksum mismatch"},
		{name: "corrupt checksum", mutate: func(b []byte) { b[25] ^= 0xff }, wantErr: "header checksum mismatch"},
		{name: "corrupt leaf", mutate: func(b []byte) { b[41] ^= 0xff }, wantErr: "root digest mismatch"},
		{name: "truncated header", mutate: nil, wantErr: "unexpected EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := bytes.Clone(valid)
			if tc.mutate == nil {
				b = b[:hashTreeHeaderLength-1]
			} else {
				tc.mutate(b)
			}

			ht1, err := DeserializeHashTree(bytes.NewBuffer(b))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, ht.Root(), ht1.Root())
		})
	}
}

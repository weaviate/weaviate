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

package changelog_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
)

func TestEncodeDecode_Roundtrip(t *testing.T) {
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	zeroID := [16]byte{}

	cases := []struct {
		name  string
		entry changelog.Entry
	}{
		{
			name: "put with payload",
			entry: changelog.Entry{
				LSN: 42, IsDelete: false, UpdateTimeMillis: 1700000000000,
				UUID: id, Payload: []byte("hello world"),
			},
		},
		{
			name: "put with empty payload",
			entry: changelog.Entry{
				LSN: 1, IsDelete: false, UpdateTimeMillis: 1,
				UUID: zeroID, Payload: nil,
			},
		},
		{
			name: "delete zero payload",
			entry: changelog.Entry{
				LSN: 99, IsDelete: true, UpdateTimeMillis: 1700000000001,
				UUID: id, Payload: nil,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := changelog.Encode(nil, &tc.entry)
			require.NoError(t, err)
			got, err := changelog.DecodeFrame(bytes.NewReader(buf))
			require.NoError(t, err)
			require.Equal(t, tc.entry.LSN, got.LSN)
			require.Equal(t, tc.entry.IsDelete, got.IsDelete)
			require.Equal(t, tc.entry.UpdateTimeMillis, got.UpdateTimeMillis)
			require.Equal(t, tc.entry.UUID, got.UUID)
			require.True(t, bytes.Equal(tc.entry.Payload, got.Payload),
				"payload mismatch: want %q got %q", tc.entry.Payload, got.Payload)
		})
	}
}

func TestDecodeFrame_TornTailAndCorruption(t *testing.T) {
	frame, err := changelog.Encode(nil, &changelog.Entry{
		LSN: 7, UpdateTimeMillis: 100, UUID: [16]byte{9}, Payload: []byte("payload"),
	})
	require.NoError(t, err)

	t.Run("clean EOF at start", func(t *testing.T) {
		_, err := changelog.DecodeFrame(bytes.NewReader(nil))
		require.ErrorIs(t, err, io.EOF)
	})

	// Truncate at every internal offset: mid-LSN, mid-header, mid-payload,
	// mid-CRC. All must return io.EOF.
	truncations := []int{1, 7, 8, 20, 36, 37, 40, len(frame) - 1}
	for _, at := range truncations {
		t.Run(fmt.Sprintf("truncated at %d of %d", at, len(frame)), func(t *testing.T) {
			_, err := changelog.DecodeFrame(bytes.NewReader(frame[:at]))
			require.Truef(t, errors.Is(err, io.EOF),
				"truncated at %d: want io.EOF got %v", at, err)
		})
	}

	t.Run("byte flip in header triggers CRC mismatch", func(t *testing.T) {
		corrupt := append([]byte(nil), frame...)
		corrupt[10] ^= 0xFF
		_, err := changelog.DecodeFrame(bytes.NewReader(corrupt))
		require.ErrorIs(t, err, changelog.ErrCRCMismatch)
	})

	t.Run("byte flip in payload triggers CRC mismatch", func(t *testing.T) {
		corrupt := append([]byte(nil), frame...)
		// headerSize = 37; payload starts at 37
		corrupt[40] ^= 0xFF
		_, err := changelog.DecodeFrame(bytes.NewReader(corrupt))
		require.ErrorIs(t, err, changelog.ErrCRCMismatch)
	})
}

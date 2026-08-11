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

package byteops

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// shortView returns a buffer of n readable bytes backed by a much larger array.
// This is the shape the checked readers exist for: the unchecked ones slice as
// Buffer[Position:Position+n], which Go bounds against capacity, so an overrun
// that stays inside the backing array returns the following bytes instead of
// panicking.
func shortView(n int) []byte {
	backing := make([]byte, n+1024)
	for i := range backing {
		backing[i] = 0xAA
	}
	return backing[:n]
}

func TestCheckedReadsRejectOverrun(t *testing.T) {
	reads := []struct {
		name string
		need uint64
		read func(*ReadWriter) error
	}{
		{"ReadUint8Checked", Uint8Len, func(rw *ReadWriter) error { _, err := rw.ReadUint8Checked(); return err }},
		{"ReadUint16Checked", Uint16Len, func(rw *ReadWriter) error { _, err := rw.ReadUint16Checked(); return err }},
		{"ReadUint32Checked", Uint32Len, func(rw *ReadWriter) error { _, err := rw.ReadUint32Checked(); return err }},
		{"ReadUint64Checked", Uint64Len, func(rw *ReadWriter) error { _, err := rw.ReadUint64Checked(); return err }},
		{"ReadBytesFromBufferChecked", 16, func(rw *ReadWriter) error {
			_, err := rw.ReadBytesFromBufferChecked(16)
			return err
		}},
		{"CopyBytesFromBufferChecked", 16, func(rw *ReadWriter) error {
			_, err := rw.CopyBytesFromBufferChecked(16, nil)
			return err
		}},
		{"SkipChecked", 16, func(rw *ReadWriter) error { return rw.SkipChecked(16) }},
	}

	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("exactly enough", func(t *testing.T) {
				rw := NewReadWriter(shortView(int(tc.need)))
				require.NoError(t, tc.read(&rw))
				require.Equal(t, tc.need, rw.Position)
			})

			t.Run("one byte short", func(t *testing.T) {
				rw := NewReadWriter(shortView(int(tc.need) - 1))
				err := tc.read(&rw)
				require.ErrorIs(t, err, ErrBufferOverrun)
				require.Zero(t, rw.Position, "a rejected read must not advance the cursor")
			})

			t.Run("empty buffer", func(t *testing.T) {
				rw := NewReadWriter(shortView(0))
				require.ErrorIs(t, tc.read(&rw), ErrBufferOverrun)
			})

			t.Run("cursor already past the end", func(t *testing.T) {
				rw := NewReadWriter(shortView(int(tc.need)))
				rw.Position = tc.need + 8
				require.ErrorIs(t, tc.read(&rw), ErrBufferOverrun)
			})
		})
	}
}

func TestLengthIndicatorReadsRejectCorruptLength(t *testing.T) {
	t.Run("uint32 length past the buffer", func(t *testing.T) {
		buf := shortView(64)
		binary.LittleEndian.PutUint32(buf, 0xFFFFFFF0)

		rw := NewReadWriter(buf)
		_, err := rw.ReadBytesFromBufferWithUint32LengthIndicatorChecked()
		require.ErrorIs(t, err, ErrBufferOverrun)
	})

	t.Run("uint64 length past the buffer", func(t *testing.T) {
		buf := shortView(64)
		binary.LittleEndian.PutUint64(buf, 1<<40)

		rw := NewReadWriter(buf)
		_, err := rw.ReadBytesFromBufferWithUint64LengthIndicatorChecked()
		require.ErrorIs(t, err, ErrBufferOverrun)
	})

	t.Run("length indicator itself truncated", func(t *testing.T) {
		rw := NewReadWriter(shortView(2))
		_, err := rw.ReadBytesFromBufferWithUint32LengthIndicatorChecked()
		require.ErrorIs(t, err, ErrBufferOverrun)
	})

	t.Run("well-formed payload round-trips", func(t *testing.T) {
		payload := []byte("weaviate")
		buf := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
		buf = append(buf, payload...)

		rw := NewReadWriter(buf)
		got, err := rw.ReadBytesFromBufferWithUint32LengthIndicatorChecked()
		require.NoError(t, err)
		require.Equal(t, payload, got)
		require.Equal(t, uint64(len(buf)), rw.Position)
	})
}

func TestSeekChecked(t *testing.T) {
	rw := NewReadWriter(shortView(32))

	require.NoError(t, rw.SeekChecked(16))
	require.Equal(t, uint64(16), rw.Position)

	// the end-of-value position every sequential decoder finishes at
	require.NoError(t, rw.SeekChecked(32))
	require.Equal(t, uint64(32), rw.Position)

	require.ErrorIs(t, rw.SeekChecked(33), ErrBufferOverrun)
	require.Equal(t, uint64(32), rw.Position, "a rejected seek must not move the cursor")
}

func TestRemaining(t *testing.T) {
	rw := NewReadWriter(shortView(8))
	require.Equal(t, uint64(8), rw.Remaining())

	rw.Position = 3
	require.Equal(t, uint64(5), rw.Remaining())

	rw.Position = 8
	require.Zero(t, rw.Remaining())

	rw.Position = 99
	require.Zero(t, rw.Remaining(), "a cursor past the end must not underflow")
}

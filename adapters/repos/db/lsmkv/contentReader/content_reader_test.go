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

package contentReader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContentReader_ReadValue(t *testing.T) {
	bytes := []byte{0, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			for i := 0; i < len(bytes); i++ {
				value, offset := contReader.ReadValue(uint64(i))
				require.Equal(t, byte(i), value)
				require.Equal(t, uint64(i+1), offset)
			}
			require.Equal(t, uint64(len(bytes)), contReader.Length())
		})
	}
}

func TestContentReader_ReadRange(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			buf, offset := contReader.ReadRange(0, 4)
			require.Equal(t, []byte{0, 0, 0, 0}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader.ReadRange(offset, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(8), offset)

			buf, offset = contReader.ReadRange(offset, 3)
			require.Equal(t, []byte{2, 3, 4}, buf)
			require.Equal(t, uint64(len(bytes)), offset)
			require.Equal(t, uint64(len(bytes)), contReader.Length())
		})
	}
}

func TestContentReader_OffsetsStartEnd(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			contReader2, err := contReader.NewWithOffsetStartEnd(4, 10)
			require.Nil(t, err)
			require.Equal(t, uint64(6), contReader2.Length())

			buf, offset := contReader2.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStartEnd(2, 6)
			require.Nil(t, err)
			require.Equal(t, uint64(4), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 2, 3}, buf)
			require.Equal(t, uint64(4), offset)
		})
	}
}

func TestContentReader_OffsetsStart(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			contReader2, err := contReader.NewWithOffsetStart(4)
			require.Nil(t, err)
			require.Equal(t, uint64(len(bytes)-4), contReader2.Length())

			buf, offset := contReader2.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStart(2)
			require.Nil(t, err)
			require.Equal(t, uint64(len(bytes)-6), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 2, 3}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader3.ReadRange(4, 1)
			require.Equal(t, []byte{4}, buf)
			require.Equal(t, uint64(5), offset)
		})
	}
}

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

package segmentindex

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkParseHeader(b *testing.B) {
	// All-zero data is now rejected as corrupt; use a minimal valid
	// empty-segment header instead.
	var buf bytes.Buffer
	_, err := (&Header{IndexStart: uint64(HeaderSize)}).WriteTo(&buf)
	require.NoError(b, err)
	data := buf.Bytes()
	require.Len(b, data, HeaderSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseHeader(data)
		require.NoError(b, err)
	}
}

// TestParseHeader_RejectsIndexStartBeforeHeaderEnd pins weaviate/weaviate#12199:
// IndexStart before HeaderSize must be rejected, not parsed as empty.
func TestParseHeader_RejectsIndexStartBeforeHeaderEnd(t *testing.T) {
	t.Run("all-zero header (size-preserved zeroed segment)", func(t *testing.T) {
		data := make([]byte, HeaderSize)
		_, err := ParseHeader(data)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupt segment header")
	})

	t.Run("IndexStart one byte before header end", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := (&Header{IndexStart: uint64(HeaderSize - 1)}).WriteTo(&buf)
		require.NoError(t, err)
		_, err = ParseHeader(buf.Bytes())
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupt segment header")
	})

	t.Run("IndexStart exactly at header end (legitimate empty segment)", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := (&Header{IndexStart: uint64(HeaderSize)}).WriteTo(&buf)
		require.NoError(t, err)
		header, err := ParseHeader(buf.Bytes())
		require.NoError(t, err)
		require.Equal(t, uint64(HeaderSize), header.IndexStart)
	})

	t.Run("IndexStart past header end (ordinary populated segment)", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := (&Header{IndexStart: uint64(HeaderSize) + 1000}).WriteTo(&buf)
		require.NoError(t, err)
		header, err := ParseHeader(buf.Bytes())
		require.NoError(t, err)
		require.Equal(t, uint64(HeaderSize)+1000, header.IndexStart)
	})
}

// TestHeaderValidateIndexBounds_RejectsIndexStartPastContentsLen pins
// weaviate/weaviate#12280: IndexStart past the segment's actual length must
// be rejected before any slice using it, not left to panic at the first
// read.
func TestHeaderValidateIndexBounds_RejectsIndexStartPastContentsLen(t *testing.T) {
	t.Run("IndexStart one byte past contentsLen", func(t *testing.T) {
		h := &Header{IndexStart: 101}
		err := h.ValidateIndexBounds(100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupt segment header")
		require.Contains(t, err.Error(), "past the segment end")
	})

	t.Run("large segment: 622914-byte length, IndexStart=722914", func(t *testing.T) {
		h := &Header{IndexStart: 722914}
		err := h.ValidateIndexBounds(622914)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupt segment header")
	})

	t.Run("IndexStart exactly at contentsLen (legitimate: index is the last byte range)", func(t *testing.T) {
		h := &Header{IndexStart: 100}
		require.NoError(t, h.ValidateIndexBounds(100))
	})

	t.Run("IndexStart well within contentsLen (ordinary populated segment)", func(t *testing.T) {
		h := &Header{IndexStart: 100}
		require.NoError(t, h.ValidateIndexBounds(10_000))
	})
}

// TestHeaderValidateIndexBounds_RejectsSecondaryIndexTablePastContentsLen
// pins weaviate/weaviate#12280: a corrupt-large SecondaryIndices count
// pushing the secondary offset table past the segment's actual length must
// be rejected, not left to panic reading the offset table.
func TestHeaderValidateIndexBounds_RejectsSecondaryIndexTablePastContentsLen(t *testing.T) {
	t.Run("SecondaryIndices count pushes the offset table past contentsLen", func(t *testing.T) {
		h := &Header{IndexStart: 100, SecondaryIndices: 65535}
		err := h.ValidateIndexBounds(1000)
		require.Error(t, err)
		require.Contains(t, err.Error(), "corrupt segment header")
		require.Contains(t, err.Error(), "secondary index table end")
	})

	t.Run("legitimate secondary index table fits within contentsLen", func(t *testing.T) {
		h := &Header{IndexStart: 100, SecondaryIndices: 2}
		require.NoError(t, h.ValidateIndexBounds(1000))
	})

	t.Run("SecondaryIndices zero is unaffected by the secondary-table check", func(t *testing.T) {
		h := &Header{IndexStart: 100, SecondaryIndices: 0}
		require.NoError(t, h.ValidateIndexBounds(100))
	})
}

func BenchmarkWriteHeader(b *testing.B) {
	header := Header{
		Version:          1,
		Level:            1,
		SecondaryIndices: 35,
		Strategy:         StrategyReplace,
		IndexStart:       234,
	}
	path := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.Create(path + "/test.tmp")
		require.NoError(b, err)
		header.WriteTo(f)
	}
}

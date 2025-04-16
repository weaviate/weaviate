package segmentindex

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkParseHeader(b *testing.B) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	require.Len(b, data, HeaderSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseHeader(bytes.NewReader(data))
		require.NoError(b, err)
	}
}

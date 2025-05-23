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

package segmentindex

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkParseHeader(b *testing.B) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	require.Len(b, data, HeaderSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseHeader(data)
		require.NoError(b, err)
	}
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

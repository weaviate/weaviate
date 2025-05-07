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

package lsmkv

import (
	"crypto/rand"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/mmap"
)

func BenchmarkMMap(b *testing.B) {
	tests := []struct {
		size int
	}{
		{size: 100},
		{size: 1000},
		{size: 10000},
		{size: 100000},
	}

	dir := b.TempDir()
	f, err := os.Create(dir + "/test.tmp")
	require.NoError(b, err)

	bytes := make([]byte, 100000)
	read, err := rand.Read(bytes)
	require.NoError(b, err)
	require.Equal(b, read, len(bytes))

	written, err := f.Write(bytes)
	require.NoError(b, err)
	require.Equal(b, written, len(bytes))

	b.ResetTimer()

	for _, test := range tests {
		sum := 0
		for i := range bytes[:test.size] {
			sum += int(bytes[i])
		}

		b.Run(strconv.Itoa(test.size)+"mmap", func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// not needed here, but we need to do it to have the same overhead in both tests
				_, err := f.Seek(0, io.SeekStart)
				require.NoError(b, err)

				contents, err := mmap.MapRegion(f, int(test.size), mmap.RDONLY, 0, 0)
				require.NoError(b, err)

				innerSum := 0
				for j := range contents {
					innerSum += int(contents[j])
				}
				require.Equal(b, sum, innerSum)
				require.NoError(b, contents.Unmap())
			}
		})

		b.Run(strconv.Itoa(test.size)+"full read", func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := f.Seek(0, io.SeekStart)
				require.NoError(b, err)

				data := make([]byte, test.size)
				n, err := f.Read(data)
				if err != nil {
					return
				}
				require.NoError(b, err)
				require.Equal(b, n, test.size)

				innerSum := 0
				for j := range data {
					innerSum += int(data[j])
				}
				require.Equal(b, sum, innerSum)
			}
		})
	}
}

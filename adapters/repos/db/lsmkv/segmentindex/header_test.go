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
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

// TestHeaderIndexFromRegion: region-based accessors match whole-file ones;
// corrupt offset tables error rather than panic.
func TestHeaderIndexFromRegion(t *testing.T) {
	buildSource := func(t *testing.T, secondaryCount uint16, indexStart uint64) ([]byte, *Header) {
		t.Helper()
		indexes := Indexes{SecondaryIndexCount: secondaryCount}
		for i := 0; i < 20; i++ {
			key := Key{
				Key:        []byte(fmt.Sprintf("primary-%02d", i)),
				ValueStart: i * 10,
				ValueEnd:   i*10 + 10,
			}
			for j := uint16(0); j < secondaryCount; j++ {
				key.SecondaryKeys = append(key.SecondaryKeys,
					[]byte(fmt.Sprintf("secondary-%d-%02d", j, i)))
			}
			indexes.Keys = append(indexes.Keys, key)
		}

		var buf bytes.Buffer
		buf.Write(make([]byte, indexStart)) // header + fake data section
		_, err := indexes.WriteTo(&buf)
		require.NoError(t, err)

		return buf.Bytes(), &Header{
			SecondaryIndices: secondaryCount,
			IndexStart:       indexStart,
		}
	}

	for _, secondaryCount := range []uint16{0, 1, 3} {
		t.Run(fmt.Sprintf("secondaryCount=%d", secondaryCount), func(t *testing.T) {
			source, header := buildSource(t, secondaryCount, 316)
			region := source[header.IndexStart:]

			fromFile, err := header.PrimaryIndex(source)
			require.NoError(t, err)
			fromRegion, err := header.PrimaryIndexFromRegion(region)
			require.NoError(t, err)
			assert.Equal(t, fromFile, fromRegion)

			for i := uint16(0); i < secondaryCount; i++ {
				fromFile, err := header.SecondaryIndex(source, i)
				require.NoError(t, err)
				fromRegion, err := header.SecondaryIndexFromRegion(region, i)
				require.NoError(t, err)
				assert.Equal(t, fromFile, fromRegion, "secondary index %d", i)
			}

			_, err = header.SecondaryIndexFromRegion(region, secondaryCount)
			require.Error(t, err, "out-of-range index id must error")
		})
	}

	t.Run("corrupt offset table errors instead of panicking", func(t *testing.T) {
		source, header := buildSource(t, 2, 316)
		region := append([]byte(nil), source[header.IndexStart:]...)

		// first offset points before the index region
		binary.LittleEndian.PutUint64(region[0:8], header.IndexStart-1)
		_, err := header.PrimaryIndexFromRegion(region)
		require.Error(t, err)
		_, err = header.SecondaryIndexFromRegion(region, 0)
		require.Error(t, err)

		// first offset overlaps the offset table itself
		binary.LittleEndian.PutUint64(region[0:8], header.IndexStart+8)
		_, err = header.PrimaryIndexFromRegion(region)
		require.Error(t, err)

		// second offset points past EOF while the first is valid
		binary.LittleEndian.PutUint64(region[0:8], header.IndexStart+16)
		binary.LittleEndian.PutUint64(region[8:16], header.IndexStart+uint64(len(region))+1)
		_, err = header.PrimaryIndexFromRegion(region)
		require.NoError(t, err) // primary only depends on offset 0
		_, err = header.SecondaryIndexFromRegion(region, 1)
		require.Error(t, err)

		// offsets out of order (second before first)
		binary.LittleEndian.PutUint64(region[0:8], header.IndexStart+32)
		binary.LittleEndian.PutUint64(region[8:16], header.IndexStart+16)
		_, err = header.SecondaryIndexFromRegion(region, 0)
		require.Error(t, err)
	})
}

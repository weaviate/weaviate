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

//go:build integrationTest

package lsmkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type countingReaderAt struct {
	inner readerAt
	bytes int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.inner.ReadAt(p, off)
	c.bytes += int64(n)
	return n, err
}

// TestSegmentCursorReusablePread_ReadAmplification pins that a sequential scan pulls each data byte through pread roughly once, instead of refilling a whole buffer per node.
func TestSegmentCursorReusablePread_ReadAmplification(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name           string
		count          int
		valueSize      int
		valuePrefixLen int
	}{
		{"full/tiny-values", 4000, 64, 0},
		{"full/medium-values", 1000, 2048, 0},
		{"digest/tiny-values", 4000, 64, 42},
		{"digest/vector-values", 200, 24576, 42},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, WithPread(true), WithMinMMapSize(0))
			t.Cleanup(func() { b.Shutdown(ctx) })

			value := bytes.Repeat([]byte{'x'}, tc.valueSize)
			for i := 0; i < tc.count; i++ {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%08d", i)), value))
			}
			require.NoError(t, b.FlushAndSwitch())

			segments, release := b.disk.getConsistentViewOfSegments()
			t.Cleanup(release)
			require.Len(t, segments, 1)
			seg, ok := segments[0].(*segment)
			require.True(t, ok)
			require.False(t, seg.readFromMemory)

			c := seg.newReplaceCursorReusableWithPrefix(tc.valuePrefixLen)
			t.Cleanup(c.releaseReader)
			counting := &countingReaderAt{inner: c.preadOffset.ra}
			c.preadOffset.ra = counting

			nodes := 0
			for n, err := c.first(); !errors.Is(err, lsmkv.NotFound); n, err = c.next() {
				require.NoError(t, err)
				require.NotNil(t, n)
				nodes++
			}

			require.Equal(t, tc.count, nodes)
			dataRegion := int64(seg.dataEndPos - seg.dataStartPos)
			require.LessOrEqual(t, counting.bytes, dataRegion+2*segmentCursorReaderBufSize,
				"scan of a %d-byte data region read %d bytes through pread", dataRegion, counting.bytes)
		})
	}
}

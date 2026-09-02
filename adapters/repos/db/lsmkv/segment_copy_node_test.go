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

package lsmkv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// Spelled out rather than derived from readMetricName, which would move this
// with the code it checks.
const copyNodeMetricName = "ReadFromSegmentcopyNode"

// The observer cache is global, so the entry is removed again afterwards.
func recordCopyNodeReads(tb testing.TB) *[]int64 {
	tb.Helper()

	var observed []int64
	readObserver.Store(copyNodeMetricName, BytesReadObserver(func(n, nanoseconds int64) {
		observed = append(observed, n)
		assert.Positive(tb, nanoseconds, "the elapsed time must be measured, not passed as a zero")
	}))
	tb.Cleanup(func() { readObserver.Delete(copyNodeMetricName) })
	return &observed
}

// The byte at i is the low byte of i, so a read at any offset has an expected
// value.
func preadSegmentFile(tb testing.TB, size int) (*os.File, []byte) {
	tb.Helper()

	contents := make([]byte, size)
	for i := range contents {
		contents[i] = byte(i)
	}
	path := filepath.Join(tb.TempDir(), "segment.db")
	require.NoError(tb, os.WriteFile(path, contents, 0o644))

	f, err := os.Open(path)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, f.Close()) })
	return f, contents
}

func TestSegmentCopyNodeFromFile(t *testing.T) {
	const fileSize = 8192

	tests := []struct {
		name   string
		start  uint64
		length int
		// contentFile is nil only where readFromMemory is set, so this state is
		// built here rather than reached from newSegment
		noContentFile bool
		wantErr       error
		wantErrText   []string
		wantObserved  []int64
	}{
		{
			name:         "header-sized read fetches only the bytes asked for",
			start:        0,
			length:       18,
			wantObserved: []int64{18},
		},
		{
			name:         "read spanning more than one page",
			start:        100,
			length:       4200,
			wantObserved: []int64{4200},
		},
		{
			name:         "read reaching the last byte of the file",
			start:        fileSize - 64,
			length:       64,
			wantObserved: []int64{64},
		},
		{
			name:        "read running past the end of the file",
			start:       fileSize - 192,
			length:      4096,
			wantErr:     io.EOF,
			wantErrText: []string{"copyNode"},
		},
		{
			name:    "read starting past the end of the file",
			start:   fileSize,
			length:  16,
			wantErr: io.EOF,
		},
		{
			name:          "segment with no content file",
			length:        18,
			noContentFile: true,
			// naming the segment is the whole point of the guard
			wantErrText: []string{"copyNode", "nil contentFile", "segment.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, contents := preadSegmentFile(t, fileSize)
			observed := recordCopyNodeReads(t)
			seg := &segment{contentFile: f, size: fileSize, path: f.Name()}
			if tt.noContentFile {
				seg.contentFile = nil
			}

			b := make([]byte, tt.length)
			err := seg.copyNode(b, nodeOffset{tt.start, tt.start + uint64(tt.length)})

			for _, want := range tt.wantErrText {
				require.ErrorContains(t, err, want)
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr,
					"a short read must be reported, not passed off as a filled buffer")
			} else if len(tt.wantErrText) == 0 {
				require.NoError(t, err)
				require.Equal(t, contents[tt.start:tt.start+uint64(tt.length)], b)
			}
			require.Equal(t, tt.wantObserved, *observed,
				"the read size summary must see the bytes that actually came off disk")
		})
	}
}

func TestSegmentCopyNodeServesBucketReads(t *testing.T) {
	ctx := context.Background()

	// one size and no subtest: copyNode branches on neither length nor page
	// boundary, and what this test alone kills is a segment served from memory
	const valueSize = 4200

	logger, _ := test.NewNullLogger()
	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1),
		WithPread(true), WithMinMMapSize(0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	key, secondary := []byte("primary"), []byte("secondary")
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i)
	}
	require.NoError(t, bucket.Put(key, value, WithSecondaryKey(0, secondary)))
	require.NoError(t, bucket.FlushMemtable())

	segments, release := bucket.disk.getConsistentViewOfSegments()
	t.Cleanup(release)
	require.Len(t, segments, 1)
	seg, ok := segments[0].(*segment)
	require.True(t, ok)
	// reading from memory would return before copyNode reaches the file
	require.False(t, seg.readFromMemory)

	got, err := bucket.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	got, err = bucket.GetBySecondary(ctx, 0, secondary)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

func BenchmarkSegmentCopyNode(b *testing.B) {
	const fileSize = 1024 * 1024

	metrics := benchIOReadMetrics(b)

	// seed the global cache, which a test in the same binary may have filled
	readObserver.Store(copyNodeMetricName, metrics.ReadObserver(copyNodeMetricName))
	b.Cleanup(func() { readObserver.Delete(copyNodeMetricName) })

	for _, length := range []int{18, 4200} {
		b.Run(fmt.Sprintf("%dB", length), func(b *testing.B) {
			f, _ := preadSegmentFile(b, fileSize)
			seg := &segment{contentFile: f, size: fileSize, path: f.Name(), metrics: metrics}
			buf := make([]byte, length)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := seg.copyNode(buf, nodeOffset{0, uint64(length)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

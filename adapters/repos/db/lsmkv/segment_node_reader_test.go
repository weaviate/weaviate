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
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func TestSegmentNodeReader(t *testing.T) {
	const fileSize = 16384

	f, contents := preadSegmentFile(t, fileSize)
	pread := &segment{contentFile: f, size: fileSize, path: f.Name()}
	memory := &segment{contents: contents, size: fileSize, readFromMemory: true}

	tests := []struct {
		name        string
		seg         *segment
		offset      nodeOffset
		length      int
		want        []byte
		wantErr     error
		wantErrText string
	}{
		{name: "pread from the start", seg: pread, offset: nodeOffset{start: 0, end: 64}, length: 18, want: contents[:18]},
		{name: "pread from an offset", seg: pread, offset: nodeOffset{start: 5000, end: 5064}, length: 64, want: contents[5000:5064]},
		{name: "pread spanning several buffer fills", seg: pread, offset: nodeOffset{start: 100}, length: 9000, want: contents[100:9100]},
		{name: "pread reaching the last byte of the file", seg: pread, offset: nodeOffset{start: fileSize - 64}, length: 64, want: contents[fileSize-64:]},
		{name: "pread past the end of the file", seg: pread, offset: nodeOffset{start: fileSize - 64}, length: 128, wantErr: io.ErrUnexpectedEOF},
		{name: "memory from the start", seg: memory, offset: nodeOffset{start: 0, end: 64}, length: 18, want: contents[:18]},
		{name: "memory from an offset", seg: memory, offset: nodeOffset{start: 5000, end: 5064}, length: 64, want: contents[5000:5064]},
		{name: "memory without an end runs to the end of the contents", seg: memory, offset: nodeOffset{start: fileSize - 64}, length: 64, want: contents[fileSize-64:]},
		{name: "memory stops at the end offset", seg: memory, offset: nodeOffset{start: 5000, end: 5064}, length: 65, wantErr: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// twice, so that the second pass runs on a reader the first one released
			for range 2 {
				r, err := tt.seg.newNodeReader(tt.offset, segmentCursorReplaceOp)
				require.NoError(t, err)

				got := make([]byte, tt.length)
				_, err = io.ReadFull(r, got)
				r.Release()

				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}

	errTests := []struct {
		name        string
		seg         *segment
		offset      nodeOffset
		wantErr     error
		wantErrText string
	}{
		{name: "memory with an empty range", seg: memory, offset: nodeOffset{start: 64, end: 64}, wantErr: lsmkv.NotFound},
		{name: "memory starting at the end of the contents", seg: memory, offset: nodeOffset{start: fileSize}, wantErr: lsmkv.NotFound},
		{name: "pread without a content file", seg: &segment{path: "/some/segment.db"}, wantErrText: "nil contentFile for segment at /some/segment.db"},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := tt.seg.newNodeReader(tt.offset, segmentCursorReplaceOp)
			require.Nil(t, r)
			require.ErrorContains(t, err, "new nodeReader")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.ErrorContains(t, err, tt.wantErrText)
		})
	}
}

func TestSegmentNodeReaderRelease(t *testing.T) {
	f, contents := preadSegmentFile(t, 8192)

	tests := []struct {
		name string
		seg  *segment
	}{
		{name: "pread", seg: &segment{contentFile: f, size: 8192, path: f.Name()}},
		{name: "memory", seg: &segment{contents: contents, size: 8192, readFromMemory: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := tt.seg.newNodeReader(nodeOffset{start: 0, end: 64}, segmentCursorReplaceOp)
			require.NoError(t, err)

			r.Release()

			// a pooled reader must not keep the segment's file or contents alive
			require.Nil(t, r.r)
			require.Nil(t, r.file.ra)
			require.Nil(t, r.file.observe)
			require.Zero(t, r.mem.Len())

			require.PanicsWithValue(t, "nodeReader.Read called after Release", func() {
				r.Read(make([]byte, 1))
			})
			// a second Release would otherwise hand the same reader to two reads
			require.NotPanics(t, r.Release)
		})
	}
}

// Each pread cursor step meters the bytes the buffered reader fetched, under the
// operation it was opened for.
func TestSegmentNodeReaderMetersReads(t *testing.T) {
	const metricName = "ReadFromSegmentsegmentCursorReplace"

	var observed []int64
	readObserver.Store(metricName, BytesReadObserver(func(n, nanoseconds int64) {
		observed = append(observed, n)
		assert.Positive(t, nanoseconds)
	}))
	t.Cleanup(func() { readObserver.Delete(metricName) })

	f, _ := preadSegmentFile(t, 16384)
	seg := &segment{contentFile: f, size: 16384, path: f.Name()}

	r, err := seg.newNodeReader(nodeOffset{start: 100}, segmentCursorReplaceOp)
	require.NoError(t, err)
	_, err = io.ReadFull(r, make([]byte, 18))
	require.NoError(t, err)
	r.Release()

	require.Equal(t, []int64{4096}, observed)
}

// keeps the compiler from dropping the join the allocation count is about
var readMetricNameSink string

// The names are what dashboards query, so they are spelled out here.
func TestReadMetricName(t *testing.T) {
	tests := []struct {
		operation string
		want      string
	}{
		{copyNodeOp, "ReadFromSegmentcopyNode"},
		{loadBMWOp, "ReadFromSegmentloadBMW"},
		{roaringSetReadOp, "roaringSetRead"},
		{segmentCursorReplaceOp, "ReadFromSegmentsegmentCursorReplace"},
		{segmentCursorMapOp, "ReadFromSegmentsegmentCursorMap"},
		{segmentCursorCollectionOp, "ReadFromSegmentsegmentCursorCollection"},
		{cursorCollectionReusableOp, "ReadFromSegmentCursorCollectionReusable"},
		{segmentCursorInvertedReusableOp, "ReadFromSegmentsegmentCursorInvertedReusable"},
		{targetedScanPeekOp, "ReadFromSegmentTargetedScanPeek"},
		{targetedScanRangeOp, "ReadFromSegmentTargetedScanRange"},
		{"anythingElse", "ReadFromSegmentanythingElse"},
	}
	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			require.Equal(t, tt.want, readMetricName(tt.operation))
			allocs := testing.AllocsPerRun(100, func() { readMetricNameSink = readMetricName(tt.operation) })
			if tt.operation == "anythingElse" {
				// the arm the listed operations are kept out of
				require.Positive(t, allocs)
				return
			}
			require.Zero(t, allocs)
		})
	}
}

// Pooled readers move between goroutines; every read must still see only its
// own segment and offset.
func TestSegmentNodeReaderConcurrentReuse(t *testing.T) {
	const fileSize = 16384

	f, contents := preadSegmentFile(t, fileSize)
	segs := []*segment{
		{contentFile: f, size: fileSize, path: f.Name()},
		{contents: contents, size: fileSize, readFromMemory: true},
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := make([]byte, 300)
			for i := range 500 {
				start := uint64((g*997 + i*131) % (fileSize - len(got)))
				r, err := segs[(g+i)%2].newNodeReader(nodeOffset{start: start, end: start + uint64(len(got))}, segmentCursorReplaceOp)
				if !assert.NoError(t, err) {
					return
				}
				_, err = io.ReadFull(r, got)
				r.Release()
				if !assert.NoError(t, err) || !assert.Equal(t, contents[start:start+uint64(len(got))], got) {
					return
				}
			}
		}()
	}
	wg.Wait()
}

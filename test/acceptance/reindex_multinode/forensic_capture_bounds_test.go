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

package reindex_multinode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllowNextFile(t *testing.T) {
	const budget = int64(forensicCaptureByteBudget)

	tests := []struct {
		name          string
		copiedTotal   int64
		filesThisNode int
		wantStop      bool
		wantLimit     int64
		wantReason    string
	}{
		{
			name:          "a fresh capture may use the whole budget",
			copiedTotal:   0,
			filesThisNode: 0,
			wantStop:      false,
			wantLimit:     budget,
		},
		{
			name:          "a partly used budget offers only the remainder",
			copiedTotal:   budget - 100,
			filesThisNode: 1,
			wantStop:      false,
			wantLimit:     100,
		},
		{
			name:          "the last byte of budget is still offered",
			copiedTotal:   budget - 1,
			filesThisNode: 1,
			wantStop:      false,
			wantLimit:     1,
		},
		{
			name:          "an exactly exhausted budget stops the capture",
			copiedTotal:   budget,
			filesThisNode: 1,
			wantStop:      true,
			wantReason:    "byte budget",
		},
		{
			name:          "an overshot budget stops the capture",
			copiedTotal:   budget + 4096,
			filesThisNode: 1,
			wantStop:      true,
			wantReason:    "byte budget",
		},
		{
			name:          "one file below the per-node cap is still allowed",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap - 1,
			wantStop:      false,
			wantLimit:     budget,
		},
		{
			name:          "reaching the per-node cap stops the capture",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
		{
			name:          "exceeding the per-node cap stops the capture",
			copiedTotal:   0,
			filesThisNode: forensicFilesPerNodeCap + 1,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
		{
			name:          "the file cap is reported when both bounds are hit",
			copiedTotal:   budget,
			filesThisNode: forensicFilesPerNodeCap,
			wantStop:      true,
			wantReason:    "per-node file cap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := allowNextFile(tt.copiedTotal, tt.filesThisNode)

			assert.Equal(t, tt.wantStop, got.stop)
			if tt.wantStop {
				assert.Contains(t, got.reason, tt.wantReason)
				return
			}
			assert.Empty(t, got.reason)
			assert.Equal(t, tt.wantLimit, got.limit)
			// A non-stopping allowance feeds io.CopyN directly, and a limit of
			// zero there would silently write an empty file forever.
			assert.Positive(t, got.limit)
		})
	}
}

// errReader fails after handing out its payload, standing in for a container
// stream that breaks mid-copy.
type errReader struct {
	payload []byte
	err     error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.payload) > 0 {
		n := copy(p, r.payload)
		r.payload = r.payload[n:]
		return n, nil
	}
	return 0, r.err
}

func TestCopyBounded(t *testing.T) {
	tests := []struct {
		name          string
		src           string
		limit         int64
		wantWritten   string
		wantTruncated bool
	}{
		{
			name:          "a file smaller than the limit is copied whole",
			src:           "segment",
			limit:         64,
			wantWritten:   "segment",
			wantTruncated: false,
		},
		{
			name:          "a file exactly at the limit is not reported truncated",
			src:           "seg",
			limit:         3,
			wantWritten:   "seg",
			wantTruncated: false,
		},
		{
			name:          "a file larger than the limit is cut and reported",
			src:           "segment-and-more",
			limit:         7,
			wantWritten:   "segment",
			wantTruncated: true,
		},
		{
			name:          "one byte over the limit still counts as truncated",
			src:           "abcd",
			limit:         3,
			wantWritten:   "abc",
			wantTruncated: true,
		},
		{
			name:          "an empty file copies nothing and is not truncated",
			src:           "",
			limit:         64,
			wantWritten:   "",
			wantTruncated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst bytes.Buffer

			written, truncated, err := copyBounded(&dst, strings.NewReader(tt.src), tt.limit)

			require.NoError(t, err)
			assert.Equal(t, tt.wantWritten, dst.String())
			assert.Equal(t, int64(len(tt.wantWritten)), written)
			assert.Equal(t, tt.wantTruncated, truncated)
		})
	}
}

func TestCopyBoundedSurfacesReadErrors(t *testing.T) {
	t.Run("a stream that breaks before the limit surfaces the error", func(t *testing.T) {
		var dst bytes.Buffer
		broken := &errReader{payload: []byte("par"), err: errors.New("stream reset")}

		written, truncated, err := copyBounded(&dst, broken, 64)

		require.Error(t, err)
		assert.EqualError(t, err, "stream reset")
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
	})

	t.Run("a stream that breaks exactly at the limit surfaces the error", func(t *testing.T) {
		var dst bytes.Buffer
		broken := &errReader{payload: []byte("par"), err: errors.New("stream reset")}

		written, truncated, err := copyBounded(&dst, broken, 3)

		require.Error(t, err)
		assert.EqualError(t, err, "stream reset")
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
		assert.Equal(t, "par", dst.String())
	})

	t.Run("io.EOF at the limit means the file simply ended", func(t *testing.T) {
		var dst bytes.Buffer
		exact := &errReader{payload: []byte("par"), err: io.EOF}

		written, truncated, err := copyBounded(&dst, exact, 3)

		require.NoError(t, err)
		assert.False(t, truncated)
		assert.Equal(t, int64(3), written)
	})
}

func TestCopyContainerFilesStopsWhenCaptureWindowClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Once the window is closed every copy would fail instantly, one log line
	// per remaining path. A nil container is safe here precisely because the
	// loop must stop before it reaches the first copy; if it does not, this
	// panics rather than quietly logging thousands of failures.
	copied := copyContainerFiles(ctx, t, nil, t.TempDir(), 1,
		"/data/foo/shard/lsm/a.db\n/data/foo/shard/lsm/b.db\n/data/foo/shard/lsm/c.db", 0, "probe")

	assert.Zero(t, copied)
}

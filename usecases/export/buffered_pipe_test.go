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

package export

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedPipe_ReadWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		writes   []string // data chunks to write
		readSize int      // 0 means use io.ReadAll
		expected string   // expected concatenated output
	}{
		{
			name:     "single write",
			writes:   []string{"hello, buffered pipe"},
			expected: "hello, buffered pipe",
		},
		{
			name:     "multiple chunks",
			writes:   []string{"chunk-0|", "chunk-1|", "chunk-2|", "chunk-3|", "chunk-4|"},
			expected: "chunk-0|chunk-1|chunk-2|chunk-3|chunk-4|",
		},
		{
			name:     "empty write produces no output",
			writes:   []string{"", ""},
			expected: "",
		},
		{
			name:     "single byte chunks",
			writes:   []string{"a", "b", "c"},
			expected: "abc",
		},
		{
			name:     "partial reads (3-byte buffer)",
			writes:   []string{"0123456789"},
			readSize: 3,
			expected: "0123456789",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pr, pw := newBufferedPipe(1024)

			for _, chunk := range tc.writes {
				_, err := pw.Write([]byte(chunk))
				require.NoError(t, err)
			}
			require.NoError(t, pw.Close())

			if tc.readSize == 0 {
				out, err := io.ReadAll(pr)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, string(out))
			} else {
				buf := make([]byte, tc.readSize)
				var all []byte
				for {
					n, err := pr.Read(buf)
					all = append(all, buf[:n]...)
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
				assert.Equal(t, tc.expected, string(all))
			}
		})
	}
}

func TestBufferedPipe_CloseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(pr *bufferedPipeReader, pw *bufferedPipeWriter)
		op        string // "read" or "write"
		wantErr   error  // nil means check with Contains
		errSubstr string // used when wantErr is nil
	}{
		{
			name: "write after reader close returns ErrClosedPipe",
			setup: func(pr *bufferedPipeReader, _ *bufferedPipeWriter) {
				pr.Close()
			},
			op:      "write",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "write after reader close with error returns that error",
			setup: func(pr *bufferedPipeReader, _ *bufferedPipeWriter) {
				pr.CloseWithError(fmt.Errorf("upload aborted"))
			},
			op:        "write",
			errSubstr: "upload aborted",
		},
		{
			name: "read after reader close returns ErrClosedPipe",
			setup: func(pr *bufferedPipeReader, pw *bufferedPipeWriter) {
				pw.Write([]byte("buffered data"))
				pr.Close()
			},
			op:      "read",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "write after writer close returns ErrClosedPipe",
			setup: func(_ *bufferedPipeReader, pw *bufferedPipeWriter) {
				pw.Close()
			},
			op:      "write",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "read after writer close with error returns that error",
			setup: func(_ *bufferedPipeReader, pw *bufferedPipeWriter) {
				pw.CloseWithError(fmt.Errorf("scan failed"))
			},
			op:        "read",
			errSubstr: "scan failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pr, pw := newBufferedPipe(1024)
			tc.setup(pr, pw)

			var err error
			if tc.op == "write" {
				_, err = pw.Write([]byte("data"))
			} else {
				buf := make([]byte, 100)
				_, err = pr.Read(buf)
			}

			require.Error(t, err)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.Contains(t, err.Error(), tc.errSubstr)
			}

			// Clean up whichever side wasn't closed by setup.
			pw.Close()
			pr.Close()
		})
	}
}

func TestBufferedPipe_WriterCloseWithErrorDrainsBufferFirst(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)

	_, err := pw.Write([]byte("some data"))
	require.NoError(t, err)

	scanErr := fmt.Errorf("scan failed")
	pw.CloseWithError(scanErr)

	// Reader should get the buffered data first, then the error.
	buf := make([]byte, 100)
	n, err := pr.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "some data", string(buf[:n]))

	// Next read should return the writer's error.
	_, err = pr.Read(buf)
	require.ErrorIs(t, err, scanErr)
}

func TestBufferedPipe_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	const totalBytes = 1024 * 1024 // 1 MB
	const chunkSize = 4096

	pr, pw := newBufferedPipe(32 * 1024) // 32 KB buffer — much smaller than total data

	data := make([]byte, totalBytes)
	_, err := rand.Read(data)
	require.NoError(t, err)

	var writeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := 0; off < len(data); off += chunkSize {
			end := min(off+chunkSize, len(data))
			if _, err := pw.Write(data[off:end]); err != nil {
				writeErr = err
				return
			}
		}
		writeErr = pw.Close()
	}()

	out, readErr := io.ReadAll(pr)

	wg.Wait()
	require.NoError(t, writeErr)
	require.NoError(t, readErr)
	assert.Equal(t, data, out)
}

func TestBufferedPipe_WriterUnblocksAfterDrain(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(100)

	// Fill the buffer.
	_, err := pw.Write(make([]byte, 100))
	require.NoError(t, err)

	// Start a write that will block until space is available.
	writeDone := make(chan struct{})
	go func() {
		pw.Write([]byte("more"))
		close(writeDone)
	}()

	// Drain the buffer — this must unblock the writer.
	buf := make([]byte, 200)
	_, err = pr.Read(buf)
	require.NoError(t, err)

	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after reader drained")
	}

	pw.Close()
	pr.Close()
}

func TestBufferedPipe_ReaderCloseUnblocksWriter(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(100)

	_, err := pw.Write(make([]byte, 100))
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("more"))
		writeDone <- err
	}()

	pr.CloseWithError(fmt.Errorf("reader aborted"))

	select {
	case err := <-writeDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reader aborted")
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after reader closed")
	}
}

func TestBufferedPipe_ReaderCloseUnblocksUpload(t *testing.T) {
	t.Parallel()

	// Use a zero-capacity buffer so the reader blocks immediately on the
	// first Read (no data to drain first). Writer is never closed, so the
	// only way for Read to return is via pr.Close().
	pr, pw := newBufferedPipe(1024)

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 100)
		_, err := pr.Read(buf)
		readDone <- err
	}()

	// Close the reader — must unblock the blocked Read.
	pr.Close()

	select {
	case err := <-readDone:
		require.Error(t, err)
		assert.ErrorIs(t, err, io.ErrClosedPipe)
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock after reader closed")
	}

	pw.Close()
}

func TestBufferedPipe_LargeDataIntegrity(t *testing.T) {
	t.Parallel()

	const totalBytes = 10 * 1024 * 1024   // 10 MB
	pr, pw := newBufferedPipe(256 * 1024) // 256 KB buffer

	data := make([]byte, totalBytes)
	_, err := rand.Read(data)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		off := 0
		sizes := []int{1000, 4096, 7777, 16384, 333}
		for i := 0; off < len(data); i++ {
			sz := sizes[i%len(sizes)]
			end := min(off+sz, len(data))
			_, err := pw.Write(data[off:end])
			if err != nil {
				return
			}
			off = end
		}
		pw.Close()
	}()

	var out bytes.Buffer
	_, err = io.Copy(&out, pr)
	require.NoError(t, err)
	wg.Wait()

	assert.Equal(t, data, out.Bytes())
}

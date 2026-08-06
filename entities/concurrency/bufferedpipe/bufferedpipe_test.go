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

package bufferedpipe

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

func TestReadWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxSize  int
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
		{
			name:     "write larger than maxSize overshoots the soft limit",
			maxSize:  4,
			writes:   []string{"much longer than the buffer capacity"},
			expected: "much longer than the buffer capacity",
		},
		{
			name:     "unbounded buffer accepts everything",
			maxSize:  Unbounded,
			writes:   []string{"a", "b", "c"},
			expected: "abc",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			maxSize := tc.maxSize
			if maxSize == 0 {
				maxSize = 1024
			}
			pr, pw := New(maxSize)

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

func TestCloseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(pr *Reader, pw *Writer)
		op        string // "read" or "write"
		wantErr   error  // nil means check with Contains
		errSubstr string // used when wantErr is nil
	}{
		{
			name: "write after reader close returns ErrClosedPipe",
			setup: func(pr *Reader, _ *Writer) {
				pr.Close()
			},
			op:      "write",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "write after reader close with error returns that error",
			setup: func(pr *Reader, _ *Writer) {
				pr.CloseWithError(fmt.Errorf("upload aborted"))
			},
			op:        "write",
			errSubstr: "upload aborted",
		},
		{
			name: "read after reader close returns ErrClosedPipe",
			setup: func(pr *Reader, pw *Writer) {
				pw.Write([]byte("buffered data"))
				pr.Close()
			},
			op:      "read",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "write after writer close returns ErrClosedPipe",
			setup: func(_ *Reader, pw *Writer) {
				pw.Close()
			},
			op:      "write",
			wantErr: io.ErrClosedPipe,
		},
		{
			name: "read after writer close with error returns that error",
			setup: func(_ *Reader, pw *Writer) {
				pw.CloseWithError(fmt.Errorf("scan failed"))
			},
			op:        "read",
			errSubstr: "scan failed",
		},
		{
			name: "repeated close keeps the first error",
			setup: func(_ *Reader, pw *Writer) {
				pw.CloseWithError(fmt.Errorf("scan failed"))
				pw.Close()
				pw.CloseWithError(fmt.Errorf("second error"))
			},
			op:        "read",
			errSubstr: "scan failed",
		},
		{
			name: "repeated reader close keeps the first error",
			setup: func(pr *Reader, _ *Writer) {
				pr.CloseWithError(fmt.Errorf("upload aborted"))
				pr.Close()
			},
			op:        "write",
			errSubstr: "upload aborted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pr, pw := New(1024)
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

func TestWriterCloseWithErrorDrainsBufferFirst(t *testing.T) {
	t.Parallel()

	pr, pw := New(1024)

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

func TestConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	const totalBytes = 1024 * 1024 // 1 MB
	const chunkSize = 4096

	pr, pw := New(32 * 1024) // 32 KB buffer — much smaller than total data

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

func TestWriterUnblocksAfterDrain(t *testing.T) {
	t.Parallel()

	pr, pw := New(100)

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

// Closing either half must release a Write that is already blocked on a full
// buffer, otherwise the writer waits for a drain that will never come.
func TestCloseUnblocksBlockedWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		close     func(pr *Reader, pw *Writer)
		errSubstr string
	}{
		{
			name:      "reader close with error",
			close:     func(pr *Reader, _ *Writer) { pr.CloseWithError(fmt.Errorf("reader aborted")) },
			errSubstr: "reader aborted",
		},
		{
			name:      "writer close",
			close:     func(_ *Reader, pw *Writer) { pw.Close() },
			errSubstr: io.ErrClosedPipe.Error(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pr, pw := New(100)

			_, err := pw.Write(make([]byte, 100))
			require.NoError(t, err)

			writeDone := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				_, err := pw.Write([]byte("more"))
				writeDone <- err
			}()

			// Let the Write reach its wait, so the close has to wake it
			// rather than being seen by the guard on the way in.
			<-started
			time.Sleep(50 * time.Millisecond)
			tc.close(pr, pw)

			select {
			case err := <-writeDone:
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubstr)
			case <-time.After(time.Second):
				t.Fatal("Write did not unblock after close")
			}
		})
	}
}

func TestReaderCloseUnblocksBlockedRead(t *testing.T) {
	t.Parallel()

	// The writer is never closed, so the only way for a Read on the empty
	// buffer to return is via pr.Close().
	pr, pw := New(1024)

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 100)
		_, err := pr.Read(buf)
		readDone <- err
	}()

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

func TestLargeDataIntegrity(t *testing.T) {
	t.Parallel()

	const totalBytes = 10 * 1024 * 1024 // 10 MB
	pr, pw := New(256 * 1024)           // 256 KB buffer

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

// Unbounded is the one way to opt out of the memory bound. A zero value must
// not reach it, or a caller who forgets to size its pipe gets the unlimited
// buffering its memory budget exists to prevent.
func TestNewRejectsInvalidSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		maxSize   int
		wantPanic bool
	}{
		{name: "zero value", maxSize: 0, wantPanic: true},
		{name: "negative other than Unbounded", maxSize: -2, wantPanic: true},
		{name: "Unbounded opts out explicitly", maxSize: Unbounded},
		{name: "smallest positive size", maxSize: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.wantPanic {
				require.Panics(t, func() { New(tt.maxSize) })
				return
			}
			pr, pw := New(tt.maxSize)
			require.NoError(t, pw.Close())
			require.NoError(t, pr.Close())
		})
	}
}

// A pipe retains more than maxSize, so callers budgeting memory need the real
// bound: maxSize plus one accepted-whole write plus one unread tail.
func TestWorstCaseRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		maxSize int
		largest int // largest single write the producer makes
	}{
		{name: "writes larger than the buffer", maxSize: 100, largest: 150},
		{name: "writes smaller than the buffer", maxSize: 1024, largest: 128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pr, pw := New(tt.maxSize)

			// Leave an unread tail in the reader: buffered drops by the whole
			// chunk while partial still holds most of it.
			_, err := pw.Write(make([]byte, tt.largest))
			require.NoError(t, err)
			n, err := pr.Read(make([]byte, 1))
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Len(t, pr.partial, tt.largest-1)
			require.Zero(t, pr.p.buffered)

			// Refill to one byte below the limit, which still admits a write.
			for remaining := tt.maxSize - 1; remaining > 0; {
				sz := min(remaining, tt.largest)
				_, err := pw.Write(make([]byte, sz))
				require.NoError(t, err)
				remaining -= sz
			}
			_, err = pw.Write(make([]byte, tt.largest))
			require.NoError(t, err)

			retained := pr.p.buffered + len(pr.partial)
			require.Equal(t, tt.maxSize+2*tt.largest-2, retained)
			require.Greater(t, retained, tt.maxSize)

			require.NoError(t, pw.Close())
			require.NoError(t, pr.Close())
		})
	}
}

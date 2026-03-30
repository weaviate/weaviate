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

func TestBufferedPipe_BasicReadWrite(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)

	msg := []byte("hello, buffered pipe")
	n, err := pw.Write(msg)
	require.NoError(t, err)
	assert.Equal(t, len(msg), n)

	require.NoError(t, pw.Close())

	out, err := io.ReadAll(pr)
	require.NoError(t, err)
	assert.Equal(t, msg, out)
}

func TestBufferedPipe_MultipleChunks(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)

	var expected []byte
	for i := range 10 {
		chunk := []byte(fmt.Sprintf("chunk-%d|", i))
		_, err := pw.Write(chunk)
		require.NoError(t, err)
		expected = append(expected, chunk...)
	}
	require.NoError(t, pw.Close())

	out, err := io.ReadAll(pr)
	require.NoError(t, err)
	assert.Equal(t, expected, out)
}

func TestBufferedPipe_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	const totalBytes = 1024 * 1024 // 1 MB
	const chunkSize = 4096

	pr, pw := newBufferedPipe(32 * 1024) // 32 KB buffer — much smaller than total data

	// Generate random data.
	data := make([]byte, totalBytes)
	_, err := rand.Read(data)
	require.NoError(t, err)

	// Writer goroutine.
	var writeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for off := 0; off < len(data); off += chunkSize {
			end := off + chunkSize
			if end > len(data) {
				end = len(data)
			}
			if _, err := pw.Write(data[off:end]); err != nil {
				writeErr = err
				return
			}
		}
		writeErr = pw.Close()
	}()

	// Reader goroutine (main).
	out, readErr := io.ReadAll(pr)

	wg.Wait()
	require.NoError(t, writeErr)
	require.NoError(t, readErr)
	assert.Equal(t, data, out)
}

func TestBufferedPipe_WriterBlocksWhenFull(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(100) // tiny buffer

	// Fill the buffer.
	data := make([]byte, 100)
	_, err := pw.Write(data)
	require.NoError(t, err)

	// Next write should block. Verify it doesn't complete within a short window.
	writeDone := make(chan struct{})
	go func() {
		pw.Write([]byte("more"))
		close(writeDone)
	}()

	select {
	case <-writeDone:
		t.Fatal("Write should have blocked, but it returned immediately")
	case <-time.After(50 * time.Millisecond):
		// Expected — writer is blocked.
	}

	// Drain the buffer to unblock the writer.
	buf := make([]byte, 200)
	_, err = pr.Read(buf)
	require.NoError(t, err)

	// Writer should now unblock.
	select {
	case <-writeDone:
		// Good.
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after reader drained")
	}

	pw.Close()
	pr.Close()
}

func TestBufferedPipe_ReaderCloseUnblocksWriter(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(100)

	// Fill the buffer.
	_, err := pw.Write(make([]byte, 100))
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("more"))
		writeDone <- err
	}()

	// Close reader with error — should unblock blocked writer.
	pr.CloseWithError(fmt.Errorf("reader aborted"))

	select {
	case err := <-writeDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reader aborted")
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after reader closed")
	}
}

func TestBufferedPipe_WriterCloseWithError(t *testing.T) {
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

func TestBufferedPipe_WriterCloseNil_ReaderGetsEOF(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)

	_, err := pw.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	out, err := io.ReadAll(pr)
	require.NoError(t, err) // io.ReadAll treats io.EOF as success
	assert.Equal(t, []byte("data"), out)
}

func TestBufferedPipe_PartialReads(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)

	// Write a 10-byte chunk.
	_, err := pw.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	// Read in small pieces.
	buf := make([]byte, 3)
	var all []byte
	for {
		n, err := pr.Read(buf)
		all = append(all, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, []byte("0123456789"), all)
}

func TestBufferedPipe_WriteAfterReaderClose(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)
	pr.Close()

	_, err := pw.Write([]byte("data"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestBufferedPipe_EmptyWrite(t *testing.T) {
	t.Parallel()

	pr, pw := newBufferedPipe(1024)
	n, err := pw.Write(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = pw.Write([]byte{})
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	pw.Close()
	out, err := io.ReadAll(pr)
	require.NoError(t, err)
	assert.Empty(t, out)
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
		// Write in variable-sized chunks.
		off := 0
		sizes := []int{1000, 4096, 7777, 16384, 333}
		for i := 0; off < len(data); i++ {
			sz := sizes[i%len(sizes)]
			end := off + sz
			if end > len(data) {
				end = len(data)
			}
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

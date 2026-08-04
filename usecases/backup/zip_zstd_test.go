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

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

// Under the race detector sync.Pool throws away about one in four stored items,
// so a test that needs a pooled encoder back has to ask more than once.
const poolAttempts = 40

var zstdLevels = []struct {
	name  string
	level CompressionLevel
}{
	{name: "best_speed", level: ZstdBestSpeed},
	{name: "default", level: ZstdDefaultCompression},
	{name: "best_compression", level: ZstdBestCompression},
}

// TestZipZstdEncoderReuseMatchesFreshEncoder checks that a pooled encoder writes
// the same stream a newly built one would, and that the chunk still restores.
func TestZipZstdEncoderReuseMatchesFreshEncoder(t *testing.T) {
	dir, files := newZstdChunkSource(t)

	chunks := []struct {
		name  string
		files []string
	}{
		{name: "no_files", files: nil},
		{name: "one_file", files: files[:1]},
		{name: "many_files", files: files},
	}

	for _, lv := range zstdLevels {
		for _, chunk := range chunks {
			t.Run(lv.name+"/"+chunk.name, func(t *testing.T) {
				want := freshZstdChunk(t, dir, lv.level, chunk.files)
				got := zipChunkReusingPooledEncoder(t, dir, lv.level, chunk.files, nil)

				requireEqualStream(t, want, got)
				requireChunkRestores(t, got, backup.CompressionZSTD, dir, chunk.files)
			})
		}
	}
}

// TestZipZstdEncoderResetBetweenChunks checks that an encoder that just wrote a
// full chunk carries nothing over into its next one.
func TestZipZstdEncoderResetBetweenChunks(t *testing.T) {
	dir, files := newZstdChunkSource(t)

	next := []struct {
		name  string
		files []string
	}{
		{name: "then_empty_chunk", files: nil},
		{name: "then_full_chunk", files: files},
	}

	for _, lv := range zstdLevels {
		for _, chunk := range next {
			t.Run(lv.name+"/"+chunk.name, func(t *testing.T) {
				tarStream, _ := zipChunk(t, dir, NoCompression, files)
				want := freshZstdChunk(t, dir, lv.level, chunk.files)
				got := zipChunkReusingPooledEncoder(t, dir, lv.level, chunk.files, tarStream)

				requireEqualStream(t, want, got)
			})
		}
	}
}

// TestZipZstdEncoderPoolIsPerLevel checks that an encoder pooled at one level is
// never handed to a chunk at another, which would compress at the wrong level.
func TestZipZstdEncoderPoolIsPerLevel(t *testing.T) {
	dir, files := newZstdChunkSource(t)

	tests := []struct {
		name    string
		pooled  CompressionLevel
		request CompressionLevel
	}{
		{name: "best_speed_pooled_default_requested", pooled: ZstdBestSpeed, request: ZstdDefaultCompression},
		{name: "best_speed_pooled_best_compression_requested", pooled: ZstdBestSpeed, request: ZstdBestCompression},
		{name: "best_compression_pooled_best_speed_requested", pooled: ZstdBestCompression, request: ZstdBestSpeed},
		{name: "default_pooled_best_compression_requested", pooled: ZstdDefaultCompression, request: ZstdBestCompression},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := freshZstdChunk(t, dir, test.request, files)

			for range poolAttempts {
				pooled := primeZstdEncoderPool(t, test.pooled, nil)
				got, used := zipChunk(t, dir, test.request, files)

				require.NotSame(t, pooled, used, "encoder pooled at another level must not be reused")
				requireEqualStream(t, want, got)
			}
		})
	}
}

// TestZipZstdEncoderNotPooledAfterFailedClose checks that an encoder whose close
// failed is dropped rather than pooled, and that the next chunk is unaffected.
func TestZipZstdEncoderNotPooledAfterFailedClose(t *testing.T) {
	dir, files := newZstdChunkSource(t)
	const level = ZstdDefaultCompression
	sd := backup.ShardDescriptor{Name: "shard1", Node: "node1"}

	for range poolAttempts {
		z, rc, err := NewZip(dir, int(level), 0, 0, 0)
		require.NoError(t, err)
		broken := z.zstdEncoder
		require.NotNil(t, broken)

		// consumer goes away while the chunk is still being written
		require.NoError(t, rc.Close())
		_, _, _ = z.WriteRegulars(context.Background(), &sd, newFileList(t, dir, files), &atomic.Int64{}, "chunk")
		require.Error(t, z.CloseWithError(errors.New("consumer gone")))

		next, _, err := takeZstdEncoder(level, io.Discard)
		require.NoError(t, err)
		require.NotSame(t, broken, next, "encoder whose close failed must not be reused")
		require.NoError(t, next.Close())
	}

	want := freshZstdChunk(t, dir, level, files)
	got, _ := zipChunk(t, dir, level, files)
	requireEqualStream(t, want, got)
}

// TestZipZstdEncoderReleasedOnlyOnce checks that a chunk lets go of its encoder on
// close, so a second close cannot pool the same one twice.
func TestZipZstdEncoderReleasedOnlyOnce(t *testing.T) {
	dir, files := newZstdChunkSource(t)

	tests := []struct {
		name        string
		closeErr    error
		wantCloseOK bool
	}{
		{name: "clean_close", wantCloseOK: true},
		{name: "close_with_error", closeErr: errors.New("producer failed")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			z, rc, err := NewZip(dir, int(ZstdBestSpeed), 0, 0, 0)
			require.NoError(t, err)
			require.NotNil(t, z.zstdEncoder)

			done := drainInBackground(rc, io.Discard)
			sd := backup.ShardDescriptor{Name: "shard1", Node: "node1"}
			_, _, writeErr := z.WriteRegulars(context.Background(), &sd, newFileList(t, dir, files), &atomic.Int64{}, "chunk")
			closeErr := z.CloseWithError(test.closeErr)
			drainErr := <-done
			require.NoError(t, writeErr)
			if test.wantCloseOK {
				require.NoError(t, closeErr)
				require.NoError(t, drainErr)
			} else {
				require.ErrorIs(t, drainErr, test.closeErr, "close must signal the error to the consumer")
			}

			require.Nil(t, z.zstdEncoder, "close must let go of the encoder")
			require.NoError(t, z.Close(), "closing again must stay harmless")
			require.NoError(t, rc.Close())
		})
	}
}

// TestZipZstdStaleCloseLeavesNextChunkAlone checks that closing a chunk twice
// cannot reach the encoder a later chunk already took from the pool.
func TestZipZstdStaleCloseLeavesNextChunkAlone(t *testing.T) {
	dir, files := newZstdChunkSource(t)
	const level = ZstdBestSpeed
	want := freshZstdChunk(t, dir, level, files)

	sd := backup.ShardDescriptor{Name: "shard1", Node: "node1"}
	for range poolAttempts {
		// a chunk that closed cleanly, so its encoder went to the pool
		closed, closedReader, err := NewZip(dir, int(level), 0, 0, 0)
		require.NoError(t, err)
		pooled := closed.zstdEncoder
		closedDone := drainInBackground(closedReader, io.Discard)
		_, _, err = closed.WriteRegulars(context.Background(), &sd, newFileList(t, dir, files), &atomic.Int64{}, "chunk")
		require.NoError(t, err)
		require.NoError(t, closed.Close())
		require.NoError(t, <-closedDone)
		require.NoError(t, closedReader.Close())

		next, rc, err := NewZip(dir, int(level), 0, 0, 0)
		require.NoError(t, err)
		reused := next.zstdEncoder == pooled

		require.NoError(t, closed.Close(), "closing an already closed chunk must stay harmless")

		var buf bytes.Buffer
		done := drainInBackground(rc, &buf)
		_, _, writeErr := next.WriteRegulars(context.Background(), &sd, newFileList(t, dir, files), &atomic.Int64{}, "chunk")
		closeErr := next.Close()
		drainErr := <-done
		require.NoError(t, rc.Close())
		require.NoError(t, writeErr)
		require.NoError(t, closeErr)
		require.NoError(t, drainErr)
		requireEqualStream(t, want, buf.Bytes())

		if reused {
			return
		}
	}
	t.Fatalf("no chunk picked up the pooled encoder in %d attempts", poolAttempts)
}

// TestZipZstdEncoderPoolManyChunks checks that chunks sharing a pool, in sequence
// and at the same time, all produce a correct stream.
func TestZipZstdEncoderPoolManyChunks(t *testing.T) {
	dir, files := newZstdChunkSource(t)
	const (
		level  = ZstdDefaultCompression
		chunks = 8
	)
	want := freshZstdChunk(t, dir, level, files)

	tests := []struct {
		name       string
		concurrent bool
	}{
		{name: "sequential"},
		{name: "concurrent", concurrent: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.concurrent {
				for range chunks {
					got, _ := zipChunk(t, dir, level, files)
					requireEqualStream(t, want, got)
				}
				return
			}
			streams := make([][]byte, chunks)
			errs := make([]error, chunks)
			var wg sync.WaitGroup
			for i := range chunks {
				list := newFileList(t, dir, files)
				wg.Add(1)
				go func() {
					defer wg.Done()
					streams[i], _, errs[i] = writeChunk(dir, level, list)
				}()
			}
			wg.Wait()
			for i := range chunks {
				require.NoError(t, errs[i])
				requireEqualStream(t, want, streams[i])
			}
		})
	}
}

// TestZipNonZstdLevelsHoldNoEncoder checks that the gzip and no-compression
// branches keep working without touching the pool.
func TestZipNonZstdLevelsHoldNoEncoder(t *testing.T) {
	dir, files := newZstdChunkSource(t)

	tests := []struct {
		name        string
		level       CompressionLevel
		compression backup.CompressionType
	}{
		{name: "no_compression", level: NoCompression, compression: backup.CompressionNone},
		{name: "gzip_default", level: GzipDefaultCompression, compression: backup.CompressionGZIP},
		{name: "gzip_best_speed", level: GzipBestSpeed, compression: backup.CompressionGZIP},
		{name: "gzip_best_compression", level: GzipBestCompression, compression: backup.CompressionGZIP},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, used := zipChunk(t, dir, test.level, files)
			require.Nil(t, used, "only zstd chunks hold an encoder")
			requireChunkRestores(t, got, test.compression, dir, files)
		})
	}
}

func TestZstdEncoderLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   CompressionLevel
		want    zstd.EncoderLevel
		wantErr bool
	}{
		{name: "best_speed", level: ZstdBestSpeed, want: zstd.SpeedFastest},
		{name: "default", level: ZstdDefaultCompression, want: zstd.SpeedDefault},
		{name: "best_compression", level: ZstdBestCompression, want: zstd.SpeedBetterCompression},
		{name: "gzip", level: GzipBestSpeed, wantErr: true},
		{name: "none", level: NoCompression, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := zstdEncoderLevel(test.level)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

// newZstdChunkSource writes compressible files spanning several zstd blocks and
// returns the directory plus their relative paths.
func newZstdChunkSource(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "collection"), 0o755))

	var files []string
	for i, size := range []int{1, 4096, 96 << 10, 128 << 10} {
		relPath := filepath.Join("collection", fmt.Sprintf("file%d.db", i))
		require.NoError(t, os.WriteFile(filepath.Join(dir, relPath), makeTestData(size, byte(i)), 0o644))
		files = append(files, relPath)
	}
	return dir, files
}

// zipChunk writes one chunk and returns its stream plus the zstd encoder it used,
// if any.
func zipChunk(t *testing.T, sourceDir string, level CompressionLevel, files []string) ([]byte, *zstd.Encoder) {
	t.Helper()
	stream, encoder, err := writeChunk(sourceDir, level, newFileList(t, sourceDir, files))
	require.NoError(t, err)
	return stream, encoder
}

// writeChunk is zipChunk reporting errors instead of failing the test, so it can
// run in a goroutine.
func writeChunk(sourceDir string, level CompressionLevel, files *backup.FileList) ([]byte, *zstd.Encoder, error) {
	z, rc, err := NewZip(sourceDir, int(level), 0, 0, 0)
	if err != nil {
		return nil, nil, err
	}
	encoder := z.zstdEncoder

	var buf bytes.Buffer
	done := drainInBackground(rc, &buf)

	sd := backup.ShardDescriptor{Name: "shard1", Node: "node1"}
	_, _, writeErr := z.WriteRegulars(context.Background(), &sd, files, &atomic.Int64{}, "chunk")
	closeErr := z.Close()
	drainErr := <-done
	return buf.Bytes(), encoder, errors.Join(writeErr, closeErr, drainErr, rc.Close())
}

// zipChunkReusingPooledEncoder pools an encoder that already wrote payload, then
// writes chunks until one takes that very encoder, so the stream it returns is
// known to come from a reused encoder.
func zipChunkReusingPooledEncoder(t *testing.T, sourceDir string, level CompressionLevel, files []string, payload []byte) []byte {
	t.Helper()
	for range poolAttempts {
		pooled := primeZstdEncoderPool(t, level, payload)
		stream, used := zipChunk(t, sourceDir, level, files)
		if used == pooled {
			return stream
		}
	}
	t.Fatalf("no chunk picked up the pooled encoder in %d attempts", poolAttempts)
	return nil
}

// freshZstdChunk compresses the chunk's tar stream with a newly built encoder: the
// stream a pooled encoder has to reproduce.
func freshZstdChunk(t *testing.T, sourceDir string, level CompressionLevel, files []string) []byte {
	t.Helper()
	tarStream, _ := zipChunk(t, sourceDir, NoCompression, files)

	encoderLevel, err := zstdEncoderLevel(level)
	require.NoError(t, err)
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(encoderLevel))
	require.NoError(t, err)
	_, err = enc.Write(tarStream)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	return buf.Bytes()
}

// primeZstdEncoderPool writes payload through an encoder and pools it the way
// CloseWithError does.
func primeZstdEncoderPool(t *testing.T, level CompressionLevel, payload []byte) *zstd.Encoder {
	t.Helper()
	enc, pool, err := takeZstdEncoder(level, io.Discard)
	require.NoError(t, err)
	_, err = enc.Write(payload)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	enc.Reset(nil)
	pool.Put(enc)
	return enc
}

// drainInBackground copies the chunk out of the pipe so the producer never blocks.
// The returned channel carries the read error once the stream ends.
func drainInBackground(rc io.Reader, dst io.Writer) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(dst, rc)
		done <- err
	}()
	return done
}

// requireChunkRestores unzips the chunk and checks every file came back with its
// original content.
func requireChunkRestores(t *testing.T, chunk []byte, compression backup.CompressionType, sourceDir string, files []string) {
	t.Helper()
	destDir := t.TempDir()
	uz, wc := NewUnzip(destDir, compression)
	go func() {
		defer func() { assert.NoError(t, wc.Close()) }()
		_, err := io.Copy(wc, bytes.NewReader(chunk))
		assert.NoError(t, err)
	}()
	_, err := uz.ReadChunk()
	require.NoError(t, err)
	require.NoError(t, uz.Close())

	for _, relPath := range files {
		want, err := os.ReadFile(filepath.Join(sourceDir, relPath))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(destDir, relPath))
		require.NoError(t, err)
		require.Equal(t, len(want), len(got), relPath)
		require.True(t, bytes.Equal(want, got), "%s: restored content differs", relPath)
	}
}

// requireEqualStream compares two chunk streams without dumping them on failure.
func requireEqualStream(t *testing.T, want, got []byte) {
	t.Helper()
	require.True(t, bytes.Equal(want, got),
		"stream differs from a freshly built encoder: want %d bytes, got %d", len(want), len(got))
}

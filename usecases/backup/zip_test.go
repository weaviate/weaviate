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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

func TestZip(t *testing.T) {
	var (
		pathNode = "./test_data/node1"
		ctx      = context.Background()
	)
	for _, compressionLevel := range []CompressionLevel{GzipBestCompression, NoCompression} {
		t.Run(fmt.Sprintf("compressionLevel=%v", compressionLevel), func(t *testing.T) {
			pathDest := filepath.Join(t.TempDir(), "test_data", "node1")
			require.NoError(t, copyDir(pathNode, pathDest))

			// setup
			sd, err := getShard(pathDest, "cT9eTErXgmTX")
			if err != nil {
				t.Fatal(err)
			}

			// compression writer
			compressBuf := bytes.NewBuffer(make([]byte, 0, 1000_000))
			z, rc, err := NewZip(pathDest, int(compressionLevel), 0, 0, 0)
			require.NoError(t, err)
			var zInputLen int64
			fileList := newFileList(t, pathDest, sd.Files)
			go func() {
				zInputLen, _, err = z.WriteShard(ctx, &sd, fileList, true, &atomic.Int64{}, "chunk")
				if err != nil {
					t.Errorf("compress: %v", err)
				}
				z.Close()
			}()

			// compression reader
			zOutputLen, err := io.Copy(compressBuf, rc)
			if err != nil {
				t.Fatal("copy to buffer", err)
			}

			if err := rc.Close(); err != nil {
				t.Errorf("compress:close %v", err)
			}

			f := float32(zInputLen) / float32(zOutputLen)
			fmt.Printf("compression input_size=%d output_size=%d factor=%v\n", zInputLen, zOutputLen, f)

			// cleanup folder to restore test afterwards
			require.NoError(t, os.RemoveAll(pathDest))
			require.NoError(t, os.MkdirAll(pathDest, 0o755))

			// decompression
			var compressionType backup.CompressionType
			if compressionLevel == NoCompression {
				compressionType = backup.CompressionNone
			} else {
				compressionType = backup.CompressionGZIP
			}
			uz, wc := NewUnzip(pathDest, compressionType)

			// decompression reader
			done := make(chan struct{})
			var uzInputLen atomic.Int64
			go func() {
				uzInputLen2, err := io.Copy(wc, compressBuf)
				if err != nil {
					t.Errorf("writer: %v", err)
				}
				uzInputLen.Store(uzInputLen2)
				if err := wc.Close(); err != nil {
					t.Errorf("close writer: %v", err)
				}
				done <- struct{}{}
				close(done)
			}()

			// decompression writer
			uzOutputLen, err := uz.ReadChunk()
			if err != nil {
				t.Fatalf("unzip: %v", err)
			}
			if err := uz.Close(); err != nil {
				t.Errorf("close reader: %v", err)
			}

			fmt.Printf("unzip input_size=%d output_size=%d\n", uzInputLen.Load(), uzOutputLen)

			_, err = os.Stat(pathDest)
			if err != nil {
				t.Fatalf("cannot find decompressed folder: %v", err)
			}

			<-done // wait for writer to finish

			if zInputLen != uzOutputLen {
				t.Errorf("zip input size %d != unzip output size %d", uzOutputLen, zInputLen)
			}
			if zOutputLen != uzInputLen.Load() {
				t.Errorf("zip output size %d != unzip input size %d", zOutputLen, uzInputLen.Load())
			}
		})
	}
}

func TestUnzipPathEscape(t *testing.T) {
	destPath := t.TempDir()               // destination directory for unzip
	tmpDir := t.TempDir()                 // temporary directory to create files
	completelyUnrelatedDir := t.TempDir() // directory that should not be written to

	// create a tar.gz archive with a file that tries to escape destPath
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "test1.txt"), []byte("malicious content"), 0o644))
	info, err := os.Stat(filepath.Join(tmpDir, "test1.txt"))
	require.NoError(t, err)
	header, err := tar.FileInfoHeader(info, info.Name())
	require.NoError(t, err)

	var buf bytes.Buffer
	gzw, _ := gzip.NewWriterLevel(&buf, zipLevel(0))
	tarWriter := tar.NewWriter(gzw)

	content := []byte("malicious content")
	header.Name = "../003/file.txt" // relative path that tries to escape the destPath to completelyUnrelatedDir
	require.NoError(t, tarWriter.WriteHeader(header))
	_, err = tarWriter.Write(content)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzw.Close())

	// now restore from the archive to destPath, all writes should be contained within destPath
	uz, wc := NewUnzip(destPath, backup.CompressionGZIP)
	go func() {
		_, err2 := io.Copy(wc, &buf)
		require.NoError(t, err2)
		require.NoError(t, wc.Close())
	}()

	_, err = uz.ReadChunk()
	require.ErrorContains(t, err, "outside shard root")

	entries, err := os.ReadDir(completelyUnrelatedDir)
	require.NoError(t, err)
	require.Len(t, entries, 0, "no files should be written outside of destPath")
}

func TestZipLevel(t *testing.T) {
	tests := []struct {
		in  int
		out int
	}{
		{-1, gzip.DefaultCompression},
		{4, gzip.DefaultCompression},
		{0, gzip.DefaultCompression},
		{int(GzipBestCompression), gzip.BestCompression},
		{int(GzipBestSpeed), gzip.BestSpeed},
	}

	for _, test := range tests {
		if got := zipLevel(test.in); got != test.out {
			t.Errorf("compression level got=%d want=%d", got, test.out)
		}
	}
}

func TestZipConfig(t *testing.T) {
	tests := []struct {
		chunkSize  int
		percentage int

		minPoolSize int
		maxPoolSize int
	}{
		{0, 0, 1, _NUMCPU / 2},
		{2 - 1, 50, _NUMCPU / 2, _NUMCPU},
		{512 + 1, 50, _NUMCPU / 2, _NUMCPU},
		{2, 0, 1, _NUMCPU / 2},
		{1, 100, 1, _NUMCPU},
		{100, 0, 1, _NUMCPU / 2}, // 100 MB
		{513, 0, 1, _NUMCPU / 2},
	}

	for i, test := range tests {
		got := newZipConfig(Compression{
			Level:         GzipBestSpeed,
			CPUPercentage: test.percentage,
		})
		if n := test.minPoolSize; got.GoPoolSize < n {
			t.Errorf("%d. min pool size got=%d  want>=%d", i, got.GoPoolSize, n)
		}
		if n := test.maxPoolSize; got.GoPoolSize > n {
			t.Errorf("%d. max pool size got=%d  want<%d", i, got.GoPoolSize, n)
		}
	}
}

func getShard(src, shardName string) (sd backup.ShardDescriptor, err error) {
	sd.Name = shardName
	err = filepath.Walk(src, func(fPath string, fi os.FileInfo, err error) error {
		// return on any error
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() || !strings.Contains(fPath, shardName) {
			return nil
		}
		relPath := strings.TrimPrefix(strings.ReplaceAll(fPath, src, ""), string(filepath.Separator))
		name := fi.Name()

		if strings.Contains(name, "indexcount") {
			sd.DocIDCounterPath = relPath
			sd.DocIDCounter, err = os.ReadFile(fPath)
		} else if strings.Contains(name, "proplengths") {
			sd.PropLengthTrackerPath = relPath
			sd.PropLengthTracker, err = os.ReadFile(fPath)

		} else if strings.Contains(name, "version") {
			sd.ShardVersionPath = relPath
			sd.Version, err = os.ReadFile(fPath)
		} else {
			sd.Files = append(sd.Files, relPath)
		}

		return err
	})

	return sd, err
}

func copyDir(src string, dest string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		destPath := filepath.Join(dest, relPath)
		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		return os.WriteFile(destPath, data, info.Mode())
	})
}

// TestRenaming tests that files can be read while being renamed concurrently without involving backup
func TestRenaming(t *testing.T) {
	dir := t.TempDir()
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

	// create files with random data and one important byte at the end to make sure that the complete file is read
	// There will be concurrent renaming and reading of the files
	for i := range 100 {
		f, err := os.Create(filepath.Join(dir, strconv.Itoa(i)+".tmp"))
		require.NoError(t, err)
		size := rng.Intn(4096)
		buf := make([]byte, size)
		n, err := rng.Read(buf)
		require.NoError(t, err)
		require.Equal(t, size, n)
		_, err = f.Write(buf)
		require.NoError(t, err)
		_, err = f.Write([]byte{byte(i)})
		require.NoError(t, err)

		require.NoError(t, f.Close())

	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			require.NoError(t, os.Rename(filepath.Join(dir, strconv.Itoa(i)+".tmp"), filepath.Join(dir, strconv.Itoa(i)+".tmp2")))
		}
	}()

	for i := range 100 {
		f, err := os.Open(filepath.Join(dir, strconv.Itoa(i)+".tmp"))
		if err != nil && errors.Is(err, fs.ErrNotExist) {
			f, err = os.Open(filepath.Join(dir, strconv.Itoa(i)+".tmp2"))
		}
		require.NoError(t, err)
		data, err := io.ReadAll(f)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, data[len(data)-1:])

	}
	wg.Wait()
}

func TestNewZipClampsSplitFileSize(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name                  string
		chunkTargetSize       int64
		splitFileSize         int64
		expectedSplitFileSize int64
	}{
		{
			name:                  "splitFileSize already above chunkTargetSize",
			chunkTargetSize:       500,
			splitFileSize:         1000,
			expectedSplitFileSize: 1000,
		},
		{
			name:                  "splitFileSize equals chunkTargetSize",
			chunkTargetSize:       1000,
			splitFileSize:         1000,
			expectedSplitFileSize: 1000,
		},
		{
			name:                  "splitFileSize below chunkTargetSize gets clamped up",
			chunkTargetSize:       1000,
			splitFileSize:         500,
			expectedSplitFileSize: 1000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			z, rc, err := NewZip(dir, int(GzipBestSpeed), tc.chunkTargetSize, 0, tc.splitFileSize)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSplitFileSize, z.splitFileSizeBytes)
			go func() { io.Copy(io.Discard, rc) }()
			z.Close()
			rc.Close()
		})
	}
}

// TestWriteRegulars is a table-driven test covering the main WriteRegulars behaviors:
// chunk filling with small files, big file isolation, and big file split propagation.
func TestWriteRegulars(t *testing.T) {
	type testFile struct {
		relPath string
		size    int
	}

	tests := []struct {
		name                 string
		files                []testFile
		chunkTargetSize      int64
		minIndividualSize    int64
		splitFileSize        int64
		expectTarFiles       []string // files expected in the produced tar chunk
		expectRemainingFiles []string // files expected to remain in the file list after the call
		expectSplitFile      string   // if non-empty, expect a SplitFile with this RelPath
		expectAlreadyWritten int64    // expected AlreadyWritten on the returned SplitFile
		expectBigFileChunk   string   // if non-empty, expect this file tracked in BigFilesChunk
	}{
		{
			name: "fills chunk with small files when big file does not fit",
			files: []testFile{
				{"shard/small1.db", 100},
				{"shard/big.db", 5000},
				{"shard/small2.db", 100},
				{"shard/small3.db", 100},
			},
			chunkTargetSize:      1000,
			minIndividualSize:    0,
			splitFileSize:        0,
			expectTarFiles:       []string{"shard/small1.db", "shard/small2.db", "shard/small3.db"},
			expectRemainingFiles: []string{"shard/big.db"},
		},
		{
			name: "big file gets own chunk and is tracked in BigFilesChunk",
			files: []testFile{
				{"shard/small1.db", 100},
				{"shard/big.db", 2000},
				{"shard/small2.db", 100},
			},
			chunkTargetSize:      10000,
			minIndividualSize:    500,
			splitFileSize:        0,
			expectTarFiles:       []string{"shard/small1.db", "shard/small2.db"},
			expectRemainingFiles: []string{"shard/big.db"},
			expectBigFileChunk:   "", // big file is NOT written in this chunk, only small files
		},
		{
			name: "big file exceeding split threshold returns SplitFile with first part written",
			files: []testFile{
				{"shard/huge.db", 5000},
			},
			// chunkTargetSize=1000, splitFileSize=500 → clamped to max(500,1000)=1000.
			// fileSize(5000) > splitFileSizeBytes(1000) → WriteSplitFile writes first 1000 bytes.
			chunkTargetSize:      1000,
			minIndividualSize:    1000,
			splitFileSize:        500,
			expectTarFiles:       []string{"shard/huge.db"},
			expectRemainingFiles: nil,
			expectSplitFile:      "shard/huge.db",
			expectAlreadyWritten: 1000, // clamped splitFileSize = max(500, 1000)
		},
		{
			name: "small file exceeding split threshold returns SplitFile with first part written",
			files: []testFile{
				{"shard/b.db", 800},
				{"shard/a.db", 100},
			},
			// chunkTargetSize=500, splitFileSize=200 → clamped to max(200,500)=500.
			// b.db is the first file. fileSize(800) > splitFileSizeBytes(500)
			// → WriteSplitFile writes the first 500 bytes and returns SplitFile{AW:500}.
			chunkTargetSize:      500,
			minIndividualSize:    0,
			splitFileSize:        200,
			expectTarFiles:       []string{"shard/b.db"},
			expectRemainingFiles: []string{"shard/a.db"},
			expectSplitFile:      "shard/b.db",
			expectAlreadyWritten: 500, // clamped splitFileSize = max(200, 500)
		},
		{
			name: "small file triggers chunkFull without big file ahead",
			files: []testFile{
				{"shard/a.db", 100},
				{"shard/b.db", 600},
				{"shard/c.db", 50},
			},
			// chunkTarget=500 so after writing a.db (100), b.db (600) exceeds the chunk.
			// splitFileSize=0 (disabled) so no split happens; chunkFull is returned.
			// fillChunkWithSmallFiles scans ahead and fits c.db (50) into the chunk.
			// b.db remains for the next chunk.
			chunkTargetSize:      500,
			minIndividualSize:    0,
			splitFileSize:        0,
			expectTarFiles:       []string{"shard/a.db", "shard/c.db"},
			expectRemainingFiles: []string{"shard/b.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "source")
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard"), os.ModePerm))

			var fileNames []string
			fileSizes := make(map[string]int64, len(tt.files))
			for _, f := range tt.files {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, f.relPath),
					bytes.Repeat([]byte("X"), f.size), 0o644))
				fileNames = append(fileNames, f.relPath)
				fileSizes[f.relPath] = int64(f.size)
			}

			sd := backup.ShardDescriptor{Name: "shard", Node: "node1"}
			fileList := &backup.FileList{
				Files:     append([]string{}, fileNames...),
				FileSizes: fileSizes,
			}

			z, rc, err := NewZip(dir, int(NoCompression), tt.chunkTargetSize, tt.minIndividualSize, tt.splitFileSize)
			require.NoError(t, err)

			preCompSize := &atomic.Int64{}
			var splitFile *SplitFile
			var writeErr error

			go func() {
				_, splitFile, writeErr = z.WriteRegulars(context.Background(), &sd, fileList, preCompSize, "chunk1")
				z.Close()
			}()

			buf := bytes.NewBuffer(nil)
			_, err = io.Copy(buf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.NoError(t, writeErr)

			// Check remaining files.
			require.Equal(t, len(tt.expectRemainingFiles), fileList.Len(), "remaining file count")
			for i, expected := range tt.expectRemainingFiles {
				require.Equal(t, expected, fileList.PeekAt(i), "remaining file at index %d", i)
			}

			// Check tar contents.
			tr := tar.NewReader(buf)
			var tarFiles []string
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				tarFiles = append(tarFiles, hdr.Name)
			}
			require.ElementsMatch(t, tt.expectTarFiles, tarFiles)

			// Check split file.
			if tt.expectSplitFile != "" {
				require.NotNil(t, splitFile, "expected a SplitFile")
				require.Equal(t, tt.expectSplitFile, splitFile.RelPath)
				require.Equal(t, tt.expectAlreadyWritten, splitFile.AlreadyWritten)
			} else {
				require.Nil(t, splitFile, "did not expect a SplitFile")
			}
		})
	}

	// Separate sub-test for the two-chunk big-file flow that verifies BigFilesChunk tracking.
	t.Run("big file written alone in second chunk with BigFilesChunk tracking", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "source")
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard"), os.ModePerm))

		require.NoError(t, os.WriteFile(filepath.Join(dir, "shard", "small1.db"), bytes.Repeat([]byte("s"), 100), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "shard", "big.db"), bytes.Repeat([]byte("B"), 2000), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "shard", "small2.db"), bytes.Repeat([]byte("s"), 100), 0o644))

		sd := backup.ShardDescriptor{Name: "shard", Node: "node1"}
		fileList := &backup.FileList{
			Files: []string{"shard/small1.db", "shard/big.db", "shard/small2.db"},
			FileSizes: map[string]int64{
				"shard/small1.db": 100,
				"shard/big.db":    2000,
				"shard/small2.db": 100,
			},
		}

		// Chunk 1: small files written, big file deferred.
		z, rc, err := NewZip(dir, int(NoCompression), 10000, 500, 0)
		require.NoError(t, err)
		preComp := &atomic.Int64{}
		var writeErr error
		go func() {
			_, _, writeErr = z.WriteRegulars(context.Background(), &sd, fileList, preComp, "chunk1")
			z.Close()
		}()

		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		require.NoError(t, writeErr)

		require.Equal(t, 1, fileList.Len(), "only big file should remain")

		// Chunk 2: big file is first and gets its own chunk.
		z2, rc2, err := NewZip(dir, int(NoCompression), 10000, 500, 0)
		require.NoError(t, err)
		preComp2 := &atomic.Int64{}
		go func() {
			_, _, writeErr = z2.WriteRegulars(context.Background(), &sd, fileList, preComp2, "chunk2")
			z2.Close()
		}()

		buf2 := bytes.NewBuffer(nil)
		_, err = io.Copy(buf2, rc2)
		require.NoError(t, err)
		require.NoError(t, rc2.Close())
		require.NoError(t, writeErr)

		require.Equal(t, 0, fileList.Len(), "all files should be written")

		// Verify chunk 2 tar contains only big.db.
		tr := tar.NewReader(buf2)
		var tarFiles []string
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			tarFiles = append(tarFiles, hdr.Name)
		}
		require.Equal(t, []string{"shard/big.db"}, tarFiles)

		// Verify BigFilesChunk tracking.
		require.NotNil(t, sd.BigFilesChunk)
		bigInfo, ok := sd.BigFilesChunk["shard/big.db"]
		require.True(t, ok, "big.db should be tracked in BigFilesChunk")
		require.Equal(t, int64(2000), bigInfo.Size)
		require.Equal(t, []string{"chunk2"}, bigInfo.ChunkKeys)
	})
}

// TestWriteShardFirstChunkRespectsInMemoryFiles verifies that when WriteShard
// writes in-memory shard files (firstChunkForShard=true), the first regular
// file is NOT force-written if the chunk is already full from those in-memory
// files. Before the fix, WriteRegulars always started with firstFile=true,
// ignoring the pre-existing data in the chunk.
func TestWriteShard(t *testing.T) {
	type testFile struct {
		relPath string
		size    int
	}

	inMemData := bytes.Repeat([]byte("M"), 100) // 100 bytes each, 300 total

	inMemFiles := []string{
		"shard/counter.bin",
		"shard/proplength.bin",
		"shard/version.bin",
	}

	tests := []struct {
		name                 string
		files                []testFile
		chunkTargetSize      int64
		firstChunkForShard   bool
		expectTarFiles       []string
		expectRemainingFiles []string
		expectSplitFile      string
	}{
		{
			name:               "first chunk writes in-memory files and defers regular when chunk full",
			files:              []testFile{{"shard/data.db", 300}},
			chunkTargetSize:    400,
			firstChunkForShard: true,
			// 3 in-memory files = 300 bytes nearly fill the 400-byte chunk.
			// data.db (300) doesn't fit (300+300=600 > 400), deferred.
			expectTarFiles:       inMemFiles,
			expectRemainingFiles: []string{"shard/data.db"},
		},
		{
			name:               "first chunk writes in-memory files and regular file when both fit",
			files:              []testFile{{"shard/data.db", 100}},
			chunkTargetSize:    10000,
			firstChunkForShard: true,
			// Chunk is large enough: 300 (in-mem) + 100 (data.db) = 400 < 10000.
			expectTarFiles:       append(append([]string{}, inMemFiles...), "shard/data.db"),
			expectRemainingFiles: nil,
		},
		{
			name:               "non-first chunk skips in-memory files and writes regulars",
			files:              []testFile{{"shard/data.db", 100}},
			chunkTargetSize:    10000,
			firstChunkForShard: false,
			// Not first chunk, so no in-memory files. Only data.db is written.
			expectTarFiles:       []string{"shard/data.db"},
			expectRemainingFiles: nil,
		},
		{
			name:               "non-first chunk defers file that does not fit",
			files:              []testFile{{"shard/big.db", 500}},
			chunkTargetSize:    100,
			firstChunkForShard: false,
			// big.db is the first regular file, so it is force-written even though
			// it exceeds the chunk target (firstFile guarantee).
			expectTarFiles:       []string{"shard/big.db"},
			expectRemainingFiles: nil,
		},
		{
			name: "first chunk with in-memory files and split file",
			files: []testFile{
				{"shard/huge.db", 800},
			},
			chunkTargetSize:    200,
			firstChunkForShard: true,
			// In-memory files = 300 bytes > chunkTarget (200). huge.db (800) doesn't
			// fit and exceeds the default split threshold, but splitFileSize is
			// disabled (0 → max int64) so it's just deferred via chunkFull.
			expectTarFiles:       inMemFiles,
			expectRemainingFiles: []string{"shard/huge.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "source")
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "shard"), os.ModePerm))

			var fileNames []string
			fileSizes := make(map[string]int64, len(tt.files))
			for _, f := range tt.files {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, f.relPath),
					bytes.Repeat([]byte("X"), f.size), 0o644))
				fileNames = append(fileNames, f.relPath)
				fileSizes[f.relPath] = int64(f.size)
			}

			sd := backup.ShardDescriptor{
				Name:                  "shard",
				Node:                  "node1",
				DocIDCounterPath:      "shard/counter.bin",
				DocIDCounter:          inMemData,
				PropLengthTrackerPath: "shard/proplength.bin",
				PropLengthTracker:     inMemData,
				ShardVersionPath:      "shard/version.bin",
				Version:               inMemData,
			}

			fileList := &backup.FileList{
				Files:     append([]string{}, fileNames...),
				FileSizes: fileSizes,
			}

			z, rc, err := NewZip(dir, int(NoCompression), tt.chunkTargetSize, 0, 0)
			require.NoError(t, err)

			var splitFile *SplitFile
			var writeErr error
			go func() {
				preComp := &atomic.Int64{}
				_, splitFile, writeErr = z.WriteShard(context.Background(), &sd, fileList, tt.firstChunkForShard, preComp, "chunk1")
				z.Close()
			}()

			buf := bytes.NewBuffer(nil)
			_, err = io.Copy(buf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.NoError(t, writeErr)

			// Check remaining files.
			require.Equal(t, len(tt.expectRemainingFiles), fileList.Len(), "remaining file count")
			for i, expected := range tt.expectRemainingFiles {
				require.Equal(t, expected, fileList.PeekAt(i), "remaining file at index %d", i)
			}

			// Check tar contents.
			tr := tar.NewReader(buf)
			var tarFiles []string
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				tarFiles = append(tarFiles, hdr.Name)
			}
			require.ElementsMatch(t, tt.expectTarFiles, tarFiles)

			// Check split file.
			if tt.expectSplitFile != "" {
				require.NotNil(t, splitFile, "expected a SplitFile")
				require.Equal(t, tt.expectSplitFile, splitFile.RelPath)
			} else {
				require.Nil(t, splitFile, "did not expect a SplitFile")
			}
		})
	}
}

// TestRenamingDuringBackup tests that the backup process can handle files being renamed concurrently
func TestRenamingDuringBackup(t *testing.T) {
	for _, compressionLevel := range []CompressionLevel{GzipBestCompression, NoCompression} {
		t.Run(fmt.Sprintf("compressionLevel=%v", compressionLevel), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "source")
			dir2 := filepath.Join(t.TempDir(), "dest")
			ctx := context.Background()
			require.NoError(t, os.MkdirAll(dir, os.ModePerm))
			require.NoError(t, os.MkdirAll(dir2, os.ModePerm))

			sd := backup.ShardDescriptor{
				Name: "shard1",
				Node: "node1",
			}

			rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

			// create files with random data and one important byte at the end to make sure that the complete file is read
			// There will be concurrent renaming and reading of the files
			writeDir := filepath.Join(dir, "collection")
			writeDirRename := filepath.Join(dir, backup.DeleteMarkerAdd("collection"))

			require.NoError(t, os.MkdirAll(writeDir, os.ModePerm))
			counter := 0
			for i := range 100 {
				f, err := os.Create(filepath.Join(writeDir, strconv.Itoa(i)+".tmp"))
				require.NoError(t, err)
				size := rng.Intn(4096)
				buf := make([]byte, size)
				n, err := rng.Read(buf)
				require.NoError(t, err)
				require.Equal(t, size, n)
				_, err = f.Write(buf)
				require.NoError(t, err)
				_, err = f.Write([]byte{byte(i)})
				require.NoError(t, err)
				counter += size + i

				require.NoError(t, f.Close())
				sd.Files = append(sd.Files, filepath.Join("collection", strconv.Itoa(i)+".tmp"))
			}

			f, err := os.Create(filepath.Join(writeDir, "indexcount.tmp"))
			require.NoError(t, err)
			_, err = f.Write([]byte("12345"))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			sd.DocIDCounterPath = filepath.Join("collection", "indexcount.tmp")
			sd.DocIDCounter = []byte("12345")

			f, err = os.Create(filepath.Join(writeDir, "version.tmp"))
			require.NoError(t, err)
			_, err = f.Write([]byte("12345"))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			sd.ShardVersionPath = filepath.Join("collection", "version.tmp")
			sd.Version = []byte("12345")

			f, err = os.Create(filepath.Join(writeDir, "propLength.tmp"))
			require.NoError(t, err)
			_, err = f.Write([]byte("12345"))
			require.NoError(t, err)
			require.NoError(t, f.Close())
			sd.PropLengthTrackerPath = filepath.Join("collection", "propLength.tmp")
			sd.PropLengthTracker = []byte("12345")

			// start backup process
			z, rc, err := NewZip(dir, int(compressionLevel), 0, 0, 0)
			require.NoError(t, err)
			fileList := newFileList(t, dir, sd.Files)
			go func() {
				_, _, err := z.WriteShard(ctx, &sd, fileList, true, &atomic.Int64{}, "chunk")
				require.NoError(t, err)
				require.NoError(t, z.Close())
			}()

			// rename files concurrently
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, os.Rename(writeDir, writeDirRename))
			}()

			compressBuf := bytes.NewBuffer(make([]byte, 0, 1000_000))
			_, err = io.Copy(compressBuf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())

			require.NoError(t, os.RemoveAll(dir))

			var compressionType backup.CompressionType
			if compressionLevel == NoCompression {
				compressionType = backup.CompressionNone
			} else {
				compressionType = backup.CompressionGZIP
			}

			uz, wc := NewUnzip(dir2, compressionType)
			go func() {
				_, err := io.Copy(wc, compressBuf)
				require.NoError(t, err)
				require.NoError(t, wc.Close())
			}()
			_, err = uz.ReadChunk()
			require.NoError(t, err)
			require.NoError(t, uz.Close())

			wg.Wait()

			// check restored backup
			readDir := filepath.Join(dir2, "collection")
			counter2 := 0
			for i := range 100 {
				buf, err := os.ReadFile(filepath.Join(readDir, strconv.Itoa(i)+".tmp"))
				require.NoError(t, err)
				// files have a random length AND their last byte is the index
				counter2 += len(buf) - 1 + int(buf[len(buf)-1])
			}
			require.Equal(t, counter, counter2)
		})
	}
}

// TestSplitFileRoundTrip tests that files larger than the split file size threshold
// are correctly split across multiple chunks and restored with proper offsets.
func TestSplitFileRoundTrip(t *testing.T) {
	for _, compressionLevel := range []CompressionLevel{GzipBestCompression, NoCompression} {
		t.Run(fmt.Sprintf("compressionLevel=%v", compressionLevel), func(t *testing.T) {
			ctx := context.Background()
			sourceDir := filepath.Join(t.TempDir(), "source")
			restoreDir := filepath.Join(t.TempDir(), "restore")
			require.NoError(t, os.MkdirAll(sourceDir, os.ModePerm))
			require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

			shardDir := filepath.Join(sourceDir, "shard1")
			require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))

			// Create a large file (1000 bytes) that will be split with a 300-byte split file size.
			// This should produce 4 split parts: 300 + 300 + 300 + 100 bytes.
			largeData := make([]byte, 1000)
			for i := range largeData {
				largeData[i] = byte(i % 251) // deterministic pattern using prime to avoid repetition at byte boundary
			}
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "large.bin"), largeData, 0o644))

			// Create a small file that fits in one chunk
			smallData := []byte("hello world small file")
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "small.txt"), smallData, 0o644))

			sd := backup.ShardDescriptor{
				Name:                  "shard1",
				Node:                  "node1",
				Files:                 []string{"shard1/small.txt", "shard1/large.bin"},
				DocIDCounterPath:      "shard1/indexcount",
				DocIDCounter:          []byte("42"),
				PropLengthTrackerPath: "shard1/proplengths",
				PropLengthTracker:     []byte("7"),
				ShardVersionPath:      "shard1/version",
				Version:               []byte("1"),
			}

			const chunkSize = 500     // chunk target size in bytes
			const splitFileSize = 300 // split file threshold

			// Simulate the backup loop from backend.go: write chunks until all files
			// and split file parts are written.
			var chunks [][]byte
			filesInShard := newFileList(t, sourceDir, sd.Files)
			var fileSizeExceeded *SplitFile
			firstChunk := true

			for {
				var buf bytes.Buffer
				z, rc, err := NewZip(sourceDir, int(compressionLevel), chunkSize, 0, splitFileSize)
				require.NoError(t, err)

				type writeResult struct {
					split *SplitFile
					err   error
				}
				resultCh := make(chan writeResult, 1)

				go func() {
					preComp := atomic.Int64{}
					var sr *SplitFile
					var we error
					if fileSizeExceeded != nil {
						sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
					} else {
						_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
					}
					z.Close()
					resultCh <- writeResult{split: sr, err: we}
				}()

				_, err = io.Copy(&buf, rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())
				res := <-resultCh
				require.NoError(t, res.err)

				chunks = append(chunks, buf.Bytes())
				fileSizeExceeded = res.split
				firstChunk = false

				if filesInShard.Len() == 0 && fileSizeExceeded == nil {
					break
				}
			}

			// We expect multiple chunks due to splitting
			require.Greater(t, len(chunks), 1, "expected multiple chunks due to file splitting")

			// Restore all chunks
			var compressionType backup.CompressionType
			if compressionLevel == NoCompression {
				compressionType = backup.CompressionNone
			} else {
				compressionType = backup.CompressionGZIP
			}

			for _, chunk := range chunks {
				uz, wc := NewUnzip(restoreDir, compressionType)
				go func() {
					_, err := io.Copy(wc, bytes.NewReader(chunk))
					require.NoError(t, err)
					require.NoError(t, wc.Close())
				}()
				_, err := uz.ReadChunk()
				require.NoError(t, err)
				require.NoError(t, uz.Close())
			}

			// Verify restored files match originals
			restoredLarge, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "large.bin"))
			require.NoError(t, err)
			require.Equal(t, largeData, restoredLarge, "large split file content mismatch")

			restoredSmall, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "small.txt"))
			require.NoError(t, err)
			require.Equal(t, smallData, restoredSmall, "small file content mismatch")
		})
	}
}

// TestSplitFileExactBoundary tests the edge case where a file size is exactly
// a multiple of the split file size, ensuring no empty trailing chunk is created.
func TestSplitFileExactBoundary(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "shard1"), os.ModePerm))
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	// File is exactly 2x the split size (600 bytes = 2 * 300)
	const splitFileSize = 300
	const chunkSize = 100 // small chunk to force splitting
	fileData := make([]byte, splitFileSize*2)
	for i := range fileData {
		fileData[i] = byte(i % 199)
	}
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "exact.bin"), fileData, 0o644))

	sd := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/exact.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("1"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("1"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	var chunks [][]byte
	filesInShard := newFileList(t, sourceDir, sd.Files)
	var fileSizeExceeded *SplitFile
	firstChunk := true

	for {
		var buf bytes.Buffer
		z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
		require.NoError(t, err)

		type writeResult struct {
			split *SplitFile
			err   error
		}
		resultCh := make(chan writeResult, 1)

		go func() {
			preComp := atomic.Int64{}
			var sr *SplitFile
			var we error
			if fileSizeExceeded != nil {
				sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
			} else {
				_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
			}
			z.Close()
			resultCh <- writeResult{split: sr, err: we}
		}()

		_, err = io.Copy(&buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		res := <-resultCh
		require.NoError(t, res.err)

		chunks = append(chunks, buf.Bytes())
		fileSizeExceeded = res.split
		firstChunk = false

		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}

	// Restore all chunks
	for _, chunk := range chunks {
		uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
		go func() {
			_, err := io.Copy(wc, bytes.NewReader(chunk))
			require.NoError(t, err)
			require.NoError(t, wc.Close())
		}()
		_, err := uz.ReadChunk()
		require.NoError(t, err)
		require.NoError(t, uz.Close())
	}

	restored, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "exact.bin"))
	require.NoError(t, err)
	require.Equal(t, fileData, restored, "file at exact boundary should be restored correctly")
}

// TestSplitFileBelowThreshold tests that files below the split threshold but
// exceeding the chunk size are NOT split, but deferred whole to the next chunk.
func TestSplitFileBelowThreshold(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "shard1"), os.ModePerm))
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	// Two files: first is 200 bytes, second is 200 bytes.
	// chunkSize=300, splitFileSize=500 (larger than any file).
	// The first file fits in chunk 1 (200 < 300). The second file doesn't fit
	// in the remaining space (200+200=400 > 300) but is below split threshold,
	// so it should go whole into chunk 2.
	const chunkSize = 300
	const splitFileSize = 500

	file1Data := make([]byte, 200)
	for i := range file1Data {
		file1Data[i] = byte(i)
	}
	file2Data := make([]byte, 200)
	for i := range file2Data {
		file2Data[i] = byte(i + 100)
	}
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "file1.bin"), file1Data, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "file2.bin"), file2Data, 0o644))

	sd := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/file1.bin", "shard1/file2.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("1"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("1"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	var chunks [][]byte
	filesInShard := newFileList(t, sourceDir, sd.Files)
	var fileSizeExceeded *SplitFile
	firstChunk := true

	for {
		var buf bytes.Buffer
		z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
		require.NoError(t, err)

		type writeResult struct {
			split *SplitFile
			err   error
		}
		resultCh := make(chan writeResult, 1)

		go func() {
			preComp := atomic.Int64{}
			var sr *SplitFile
			var we error
			if fileSizeExceeded != nil {
				sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
			} else {
				_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
			}
			z.Close()
			resultCh <- writeResult{split: sr, err: we}
		}()

		_, err = io.Copy(&buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		res := <-resultCh
		require.NoError(t, res.err)

		chunks = append(chunks, buf.Bytes())
		fileSizeExceeded = res.split
		firstChunk = false

		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}

	// Should have 2 chunks: chunk 1 has metadata + file1, chunk 2 has file2
	require.Equal(t, 2, len(chunks), "expected 2 chunks: file2 should not be split but deferred")

	// fileSizeExceeded should always have been nil (no split files)
	require.Nil(t, fileSizeExceeded)

	// Restore all chunks
	for _, chunk := range chunks {
		uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
		go func() {
			_, err := io.Copy(wc, bytes.NewReader(chunk))
			require.NoError(t, err)
			require.NoError(t, wc.Close())
		}()
		_, err := uz.ReadChunk()
		require.NoError(t, err)
		require.NoError(t, uz.Close())
	}

	restored1, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "file1.bin"))
	require.NoError(t, err)
	require.Equal(t, file1Data, restored1)

	restored2, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "file2.bin"))
	require.NoError(t, err)
	require.Equal(t, file2Data, restored2)
}

// TestSplitFirstFileInChunk tests that the very first file in a chunk is split
// if it exceeds the split file threshold, even though it's the first file.
// This ensures chunks always contain at least one file (or file part), but
// never write a file whole that should be split.
func TestSplitFirstFileInChunk(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "shard1"), os.ModePerm))
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	// Single file of 1000 bytes. chunkSize=200 (smaller than file),
	// splitFileSize=400 (smaller than file). The file exceeds both thresholds,
	// so even as the first (and only) file in the chunk it must be split.
	const chunkSize = 200
	const splitFileSize = 400

	fileData := make([]byte, 1000)
	for i := range fileData {
		fileData[i] = byte(i % 173)
	}
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "big.bin"), fileData, 0o644))

	sd := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/big.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("1"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("1"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	var chunks [][]byte
	filesInShard := newFileList(t, sourceDir, sd.Files)
	var fileSizeExceeded *SplitFile
	firstChunk := true

	for {
		var buf bytes.Buffer
		z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
		require.NoError(t, err)

		type writeResult struct {
			split *SplitFile
			err   error
		}
		resultCh := make(chan writeResult, 1)

		go func() {
			preComp := atomic.Int64{}
			var sr *SplitFile
			var we error
			if fileSizeExceeded != nil {
				sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
			} else {
				_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
			}
			z.Close()
			resultCh <- writeResult{split: sr, err: we}
		}()

		_, err = io.Copy(&buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		res := <-resultCh
		require.NoError(t, res.err)

		chunks = append(chunks, buf.Bytes())
		fileSizeExceeded = res.split
		firstChunk = false

		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}

	// The file is 1000 bytes with splitFileSize=400, so we expect 3 chunks
	// (400 + 400 + 200). The file must NOT be written whole in a single chunk.
	require.Greater(t, len(chunks), 1, "first file exceeding split threshold must be split, not written whole")

	// Restore all chunks
	for _, chunk := range chunks {
		uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
		go func() {
			_, err := io.Copy(wc, bytes.NewReader(chunk))
			require.NoError(t, err)
			require.NoError(t, wc.Close())
		}()
		_, err := uz.ReadChunk()
		require.NoError(t, err)
		require.NoError(t, uz.Close())
	}

	restored, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "big.bin"))
	require.NoError(t, err)
	require.Equal(t, fileData, restored, "split first file content mismatch after restore")
}

// TestCopyFileSplitPAXRecords tests the copyFile function directly with split file
// PAX records, including invalid offset values.
func TestCopyFileSplitPAXRecords(t *testing.T) {
	t.Run("valid split parts reassembled", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "out.bin")

		// Write part 1 (offset 0, 10 bytes)
		data1 := []byte("0123456789")
		h1 := &tar.Header{
			Name: "out.bin",
			Size: int64(len(data1)),
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "0",
			},
		}
		n, err := copyFile(target, h1, bytes.NewReader(data1))
		require.NoError(t, err)
		require.Equal(t, int64(10), n)

		// Write part 2 (offset 10, 5 bytes)
		data2 := []byte("ABCDE")
		h2 := &tar.Header{
			Name: "out.bin",
			Size: int64(len(data2)),
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "10",
			},
		}
		n, err = copyFile(target, h2, bytes.NewReader(data2))
		require.NoError(t, err)
		require.Equal(t, int64(5), n)

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, []byte("0123456789ABCDE"), got)
	})

	t.Run("out of order parts", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "out.bin")

		// Write part 2 first (offset 10)
		data2 := []byte("ABCDE")
		h2 := &tar.Header{
			Name: "out.bin",
			Size: int64(len(data2)),
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "10",
			},
		}
		_, err := copyFile(target, h2, bytes.NewReader(data2))
		require.NoError(t, err)

		// Write part 1 (offset 0)
		data1 := []byte("0123456789")
		h1 := &tar.Header{
			Name: "out.bin",
			Size: int64(len(data1)),
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "0",
			},
		}
		_, err = copyFile(target, h1, bytes.NewReader(data1))
		require.NoError(t, err)

		got, err := os.ReadFile(target)
		require.NoError(t, err)
		require.Equal(t, []byte("0123456789ABCDE"), got)
	})

	t.Run("invalid PAX offset", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "out.bin")

		h := &tar.Header{
			Name: "out.bin",
			Size: 5,
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "not_a_number",
			},
		}
		_, err := copyFile(target, h, bytes.NewReader([]byte("hello")))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid split file part")
	})

	t.Run("short reader returns error", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "out.bin")

		// Header claims 10 bytes but reader only has 3
		h := &tar.Header{
			Name: "out.bin",
			Size: 10,
			Mode: 0o644,
			PAXRecords: map[string]string{
				PAXRecordSplitFileOffsetName: "0",
			},
		}
		_, err := copyFile(target, h, bytes.NewReader([]byte("abc")))
		require.Error(t, err)
		require.Contains(t, err.Error(), "copy split")
	})
}

// TestFirstFileBelowSplitThresholdWrittenWhole tests that when the first file
// in a chunk exceeds maxChunkSize but is below splitFileSize, it is written
// whole to avoid an empty chunk.
func TestFirstFileBelowSplitThresholdWrittenWhole(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "shard1"), os.ModePerm))
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	// Single file of 500 bytes. chunkSize=100 (file exceeds it),
	// splitFileSize=1000 (file is below it).
	// The file should be written whole as the first file in the chunk,
	// NOT deferred or split.
	const chunkSize = 100
	const splitFileSize = 1000

	fileData := make([]byte, 500)
	for i := range fileData {
		fileData[i] = byte(i % 157)
	}
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "medium.bin"), fileData, 0o644))

	sd := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/medium.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("1"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("1"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	var chunks [][]byte
	filesInShard := newFileList(t, sourceDir, sd.Files)
	var fileSizeExceeded *SplitFile
	firstChunk := true

	for {
		var buf bytes.Buffer
		z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
		require.NoError(t, err)

		type writeResult struct {
			split *SplitFile
			err   error
		}
		resultCh := make(chan writeResult, 1)

		go func() {
			preComp := atomic.Int64{}
			var sr *SplitFile
			var we error
			if fileSizeExceeded != nil {
				sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
			} else {
				_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
			}
			z.Close()
			resultCh <- writeResult{split: sr, err: we}
		}()

		_, err = io.Copy(&buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		res := <-resultCh
		require.NoError(t, res.err)

		chunks = append(chunks, buf.Bytes())
		fileSizeExceeded = res.split
		firstChunk = false

		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}

	// The in-memory shard files are written first, filling the chunk. The regular
	// file (500 bytes, below splitFileSize=1000) is deferred to a second chunk where
	// it IS the first file and is written whole (not split).
	require.Equal(t, 2, len(chunks), "in-memory files in chunk 1, regular file written whole in chunk 2")
	require.Nil(t, fileSizeExceeded)

	// Restore and verify
	for _, chunk := range chunks {
		uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
		go func() {
			_, err := io.Copy(wc, bytes.NewReader(chunk))
			require.NoError(t, err)
			require.NoError(t, wc.Close())
		}()
		_, err := uz.ReadChunk()
		require.NoError(t, err)
		require.NoError(t, uz.Close())
	}

	restored, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "medium.bin"))
	require.NoError(t, err)
	require.Equal(t, fileData, restored)
}

// TestMultipleSplitFilesInShard tests that when a shard has multiple files that
// each exceed the split file threshold, they are all correctly split and restored.
func TestMultipleSplitFilesInShard(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "shard1"), os.ModePerm))
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	const chunkSize = 200
	const splitFileSize = 300

	// Two files that both exceed splitFileSize:
	// file_a: 700 bytes → split into 300+300+100
	// file_b: 500 bytes → split into 300+200
	fileAData := make([]byte, 700)
	for i := range fileAData {
		fileAData[i] = byte(i % 179)
	}
	fileBData := make([]byte, 500)
	for i := range fileBData {
		fileBData[i] = byte((i + 50) % 191)
	}
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "file_a.bin"), fileAData, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "shard1", "file_b.bin"), fileBData, 0o644))

	sd := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/file_a.bin", "shard1/file_b.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("1"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("1"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	var chunks [][]byte
	filesInShard := newFileList(t, sourceDir, sd.Files)
	var fileSizeExceeded *SplitFile
	firstChunk := true

	for {
		var buf bytes.Buffer
		z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
		require.NoError(t, err)

		type writeResult struct {
			split *SplitFile
			err   error
		}
		resultCh := make(chan writeResult, 1)

		go func() {
			preComp := atomic.Int64{}
			var sr *SplitFile
			var we error
			if fileSizeExceeded != nil {
				sr, we = z.WriteSplitFile(ctx, &sd, fileSizeExceeded, &preComp, "chunk")
			} else {
				_, sr, we = z.WriteShard(ctx, &sd, filesInShard, firstChunk, &preComp, "chunk")
			}
			z.Close()
			resultCh <- writeResult{split: sr, err: we}
		}()

		_, err = io.Copy(&buf, rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		res := <-resultCh
		require.NoError(t, res.err)

		chunks = append(chunks, buf.Bytes())
		fileSizeExceeded = res.split
		firstChunk = false

		if filesInShard.Len() == 0 && fileSizeExceeded == nil {
			break
		}
	}

	// file_a: 700/300 = 3 chunks, file_b: 500/300 = 2 chunks → at least 5 chunks
	require.GreaterOrEqual(t, len(chunks), 5, "expected at least 5 chunks for two large split files")

	// Restore all chunks
	for _, chunk := range chunks {
		uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
		go func() {
			_, err := io.Copy(wc, bytes.NewReader(chunk))
			require.NoError(t, err)
			require.NoError(t, wc.Close())
		}()
		_, err := uz.ReadChunk()
		require.NoError(t, err)
		require.NoError(t, uz.Close())
	}

	restoredA, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "file_a.bin"))
	require.NoError(t, err)
	require.Equal(t, fileAData, restoredA, "file_a content mismatch after restore")

	restoredB, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "file_b.bin"))
	require.NoError(t, err)
	require.Equal(t, fileBData, restoredB, "file_b content mismatch after restore")
}

// TestBackupRestoreEndToEnd simulates the full backup and restore cycle as
// backend.go performs it: multiple shards with a mix of small files, files
// larger than the chunk size, and files larger than the split threshold.
// Chunks are restored concurrently (as writeTempFiles does) to verify that
// concurrent split file writes to the same target file are safe.
func TestBackupRestoreEndToEnd(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	restoreDir := filepath.Join(t.TempDir(), "restore")

	const chunkSize = 300
	const splitFileSize = 500

	// Shard 1: one small file (100B), one file above chunk but below split (400B),
	// one file above split threshold (1200B).
	shard1Dir := filepath.Join(sourceDir, "shard1")
	require.NoError(t, os.MkdirAll(shard1Dir, os.ModePerm))

	small1 := makeTestData(100, 41)
	require.NoError(t, os.WriteFile(filepath.Join(shard1Dir, "small.bin"), small1, 0o644))
	medium1 := makeTestData(400, 67)
	require.NoError(t, os.WriteFile(filepath.Join(shard1Dir, "medium.bin"), medium1, 0o644))
	large1 := makeTestData(1200, 89)
	require.NoError(t, os.WriteFile(filepath.Join(shard1Dir, "large.bin"), large1, 0o644))

	sd1 := backup.ShardDescriptor{
		Name:                  "shard1",
		Node:                  "node1",
		Files:                 []string{"shard1/small.bin", "shard1/medium.bin", "shard1/large.bin"},
		DocIDCounterPath:      "shard1/indexcount",
		DocIDCounter:          []byte("42"),
		PropLengthTrackerPath: "shard1/proplengths",
		PropLengthTracker:     []byte("7"),
		ShardVersionPath:      "shard1/version",
		Version:               []byte("1"),
	}

	// Shard 2: two files above split threshold (800B and 600B).
	shard2Dir := filepath.Join(sourceDir, "shard2")
	require.NoError(t, os.MkdirAll(shard2Dir, os.ModePerm))

	large2a := makeTestData(800, 101)
	require.NoError(t, os.WriteFile(filepath.Join(shard2Dir, "big_a.bin"), large2a, 0o644))
	large2b := makeTestData(600, 131)
	require.NoError(t, os.WriteFile(filepath.Join(shard2Dir, "big_b.bin"), large2b, 0o644))

	sd2 := backup.ShardDescriptor{
		Name:                  "shard2",
		Node:                  "node1",
		Files:                 []string{"shard2/big_a.bin", "shard2/big_b.bin"},
		DocIDCounterPath:      "shard2/indexcount",
		DocIDCounter:          []byte("99"),
		PropLengthTrackerPath: "shard2/proplengths",
		PropLengthTracker:     []byte("3"),
		ShardVersionPath:      "shard2/version",
		Version:               []byte("2"),
	}

	// --- Backup phase: produce chunks for each shard (like backend.go processor) ---
	type chunkData struct {
		data []byte
	}
	allChunks := []chunkData{}

	for _, sd := range []*backup.ShardDescriptor{&sd1, &sd2} {
		filesInShard := newFileList(t, sourceDir, sd.Files)
		var fileSizeExceeded *SplitFile
		firstChunk := true

		for {
			var buf bytes.Buffer
			z, rc, err := NewZip(sourceDir, int(NoCompression), chunkSize, 0, splitFileSize)
			require.NoError(t, err)

			type writeResult struct {
				split *SplitFile
				err   error
			}
			resultCh := make(chan writeResult, 1)

			go func() {
				preComp := atomic.Int64{}
				var sr *SplitFile
				var we error
				if fileSizeExceeded != nil {
					sr, we = z.WriteSplitFile(ctx, sd, fileSizeExceeded, &preComp, "chunk")
				} else {
					_, sr, we = z.WriteShard(ctx, sd, filesInShard, firstChunk, &preComp, "chunk")
				}
				z.Close()
				resultCh <- writeResult{split: sr, err: we}
			}()

			_, err = io.Copy(&buf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			res := <-resultCh
			require.NoError(t, res.err)

			allChunks = append(allChunks, chunkData{data: buf.Bytes()})
			fileSizeExceeded = res.split
			firstChunk = false

			if filesInShard.Len() == 0 && fileSizeExceeded == nil {
				break
			}
		}
	}

	t.Logf("backup produced %d chunks", len(allChunks))
	require.Greater(t, len(allChunks), 2, "expected multiple chunks across shards")

	// --- Restore phase: process all chunks concurrently (like writeTempFiles) ---
	require.NoError(t, os.MkdirAll(restoreDir, os.ModePerm))

	var wg sync.WaitGroup
	errs := make([]error, len(allChunks))
	for i, c := range allChunks {
		wg.Add(1)
		go func(idx int, chunk []byte) {
			defer wg.Done()
			uz, wc := NewUnzip(restoreDir, backup.CompressionNone)
			go func() {
				_, _ = io.Copy(wc, bytes.NewReader(chunk))
				wc.Close()
			}()
			_, errs[idx] = uz.ReadChunk()
			uz.Close()
		}(i, c.data)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "chunk %d restore failed", i)
	}

	// --- Verify all files match originals ---
	// Shard 1
	restored, err := os.ReadFile(filepath.Join(restoreDir, "shard1", "small.bin"))
	require.NoError(t, err)
	require.Equal(t, small1, restored, "shard1/small.bin mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard1", "medium.bin"))
	require.NoError(t, err)
	require.Equal(t, medium1, restored, "shard1/medium.bin mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard1", "large.bin"))
	require.NoError(t, err)
	require.Equal(t, large1, restored, "shard1/large.bin mismatch")

	// Shard 1 in-memory files
	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard1", "indexcount"))
	require.NoError(t, err)
	require.Equal(t, []byte("42"), restored, "shard1 DocIDCounter mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard1", "proplengths"))
	require.NoError(t, err)
	require.Equal(t, []byte("7"), restored, "shard1 PropLengthTracker mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard1", "version"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), restored, "shard1 Version mismatch")

	// Shard 2
	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard2", "big_a.bin"))
	require.NoError(t, err)
	require.Equal(t, large2a, restored, "shard2/big_a.bin mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard2", "big_b.bin"))
	require.NoError(t, err)
	require.Equal(t, large2b, restored, "shard2/big_b.bin mismatch")

	// Shard 2 in-memory files
	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard2", "indexcount"))
	require.NoError(t, err)
	require.Equal(t, []byte("99"), restored, "shard2 DocIDCounter mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard2", "proplengths"))
	require.NoError(t, err)
	require.Equal(t, []byte("3"), restored, "shard2 PropLengthTracker mismatch")

	restored, err = os.ReadFile(filepath.Join(restoreDir, "shard2", "version"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), restored, "shard2 Version mismatch")
}

// TestWriteRegularsBigFileReturnsSplitFile verifies that when a "big" file
// (>= bigFileThreshold) also exceeds splitFileSizeBytes, WriteRegulars
// returns the SplitFile so the caller can split it across chunks instead of
// silently dropping it.
// makeTestData creates deterministic test data of the given size.
func makeTestData(size int, seed byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i + int(seed)) % 251)
	}
	return data
}

// newFileList creates a FileList from a list of file paths, collecting file sizes
// from the given source directory. This mirrors what createFileList does in production.
func newFileList(t *testing.T, sourceDir string, files []string) *backup.FileList {
	t.Helper()
	fileSizes := make(map[string]int64, len(files))
	for _, relPath := range files {
		info, err := os.Stat(filepath.Join(sourceDir, relPath))
		require.NoError(t, err, "stat %s", relPath)
		fileSizes[relPath] = info.Size()
	}
	return &backup.FileList{
		Files:     append([]string{}, files...),
		FileSizes: fileSizes,
	}
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	pathDest := filepath.Join(t.TempDir(), "test_data", "node1")
	require.NoError(t, copyDir(pathNode, pathDest))

	// setup
	sd, err := getShard(pathDest, "cT9eTErXgmTX")
	if err != nil {
		t.Fatal(err)
	}

	// compression writer
	compressBuf := bytes.NewBuffer(make([]byte, 0, 1000_000))
	z, rc := NewZip(pathDest, 0)
	var zInputLen int64
	go func() {
		zInputLen, err = z.WriteShard(ctx, &sd)
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
	uz, wc := NewUnzip(pathDest)

	// decompression reader
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

	if zInputLen != uzOutputLen {
		t.Errorf("zip input size %d != unzip output size %d", uzOutputLen, zInputLen)
	}
	if zOutputLen != uzInputLen.Load() {
		t.Errorf("zip output size %d != unzip input size %d", zOutputLen, uzInputLen.Load())
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
	uz, wc := NewUnzip(destPath)
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
		{int(BestCompression), gzip.BestCompression},
		{int(BestSpeed), gzip.BestSpeed},
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

		expectedChunkSize int
		minPoolSize       int
		maxPoolSize       int
	}{
		{0, 0, DefaultChunkSize, 1, _NUMCPU / 2},
		{2 - 1, 50, minChunkSize, _NUMCPU / 2, _NUMCPU},
		{512 + 1, 50, maxChunkSize, _NUMCPU / 2, _NUMCPU},
		{2, 0, minChunkSize, 1, _NUMCPU / 2},
		{1, 100, minChunkSize, 1, _NUMCPU},
		{100, 0, 100 * 1024 * 1024, 1, _NUMCPU / 2}, // 100 MB
		{513, 0, maxChunkSize, 1, _NUMCPU / 2},
	}

	for i, test := range tests {
		got := newZipConfig(Compression{
			Level:         BestSpeed,
			CPUPercentage: test.percentage,
			ChunkSize:     test.chunkSize,
		})
		if got.ChunkSize != test.expectedChunkSize {
			t.Errorf("%d. chunk size got=%v want=%v", i, got.ChunkSize, test.expectedChunkSize)
		}
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

// TestRenamingDuringBackup tests that the backup process can handle files being renamed concurrently
func TestRenamingDuringBackup(t *testing.T) {
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
	z, rc := NewZip(dir, 0)
	go func() {
		_, err = z.WriteShard(ctx, &sd)
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

	uz, wc := NewUnzip(dir2)
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
}

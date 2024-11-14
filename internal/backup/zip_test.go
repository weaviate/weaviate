//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/entities/backup"
)

func TestZip(t *testing.T) {
	var (
		pathNode = "test_data/node1"
		pathDest = "./test_data/node-unzipped"
		ctx      = context.Background()
	)

	defer os.RemoveAll(pathDest)
	// setup
	sd, err := getShard(pathNode, "cT9eTErXgmTX")
	if err != nil {
		t.Fatal(err)
	}

	// compression writer
	compressBuf := bytes.NewBuffer(make([]byte, 0, 1000_000))
	z, rc := NewZip(pathNode, 0)
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
	os.RemoveAll(pathDest)
	// decompression
	uz, wc := NewUnzip(pathDest)

	// decompression reader
	var uzInputLen int64
	go func() {
		uzInputLen, err = io.Copy(wc, compressBuf)
		if err != nil {
			t.Errorf("writer: %v", err)
		}
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

	fmt.Printf("unzip input_size=%d output_size=%d\n", uzInputLen, uzOutputLen)

	_, err = os.Stat(pathDest)
	if err != nil {
		t.Fatalf("cannot find decompressed folder: %v", err)
	}

	if zInputLen != uzOutputLen {
		t.Errorf("zip input size %d != unzip output size %d", uzOutputLen, zInputLen)
	}
	if zOutputLen != uzInputLen {
		t.Errorf("zip output size %d != unzip input size %d", zOutputLen, uzInputLen)
	}
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
		relPath := strings.TrimPrefix(strings.Replace(fPath, src, "", -1), string(filepath.Separator))
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

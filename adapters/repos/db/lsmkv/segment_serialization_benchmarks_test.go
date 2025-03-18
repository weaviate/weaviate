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

package lsmkv

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkReplaceNodeKeyIndexAndWriteTo(b *testing.B) {
	// targetBuf := bytes.NewBuffer(make([]byte, 32*1024*1024)) // large enough to avoid growths during running
	targetBuf := bytes.NewBuffer(nil) // large enough to avoid growths during running

	node := segmentReplaceNode{
		tombstone:           true,
		value:               []byte("foo bar"),
		primaryKey:          []byte("foo bar"),
		secondaryIndexCount: 1,
		secondaryKeys:       [][]byte{[]byte("foo bar")},
		offset:              27,
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := node.KeyIndexAndWriteTo(targetBuf)
		require.Nil(b, err)
	}
}

func BenchmarkCollectionNodeKeyIndexAndWriteTo(b *testing.B) {
	targetBuf := bytes.NewBuffer(make([]byte, 32*1024*1024)) // large enough to avoid growths during running

	node := segmentCollectionNode{
		primaryKey: []byte("foo bar"),
		offset:     27,
		values: []value{
			{
				value:     []byte("my-value"),
				tombstone: true,
			},
			{
				value:     []byte("my-value"),
				tombstone: true,
			},
		},
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		node.KeyIndexAndWriteTo(targetBuf)
	}
}

type testCase struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}

func BenchmarkFileParseReplaceNode(b *testing.B) {
	testCases := []testCase{
		{"PrimaryKey-64-Sec-0", 4096, 64, 0, 0},
		{"PrimaryKey-64-Sec-0", 4096, 128, 0, 0},
		{"PrimaryKey-512-Sec-0", 4096, 512, 0, 0},
		{"PrimaryKey-1024-Sec-0", 4096, 1024, 0, 0},
		{"PrimaryKey-4096-Sec-0", 4096, 4096, 0, 0},
		{"SecondaryKeys-1-128", 4096, 128, 1, 128},
		{"SecondaryKeys-2-128", 4096, 128, 2, 128},
		{"SecondaryKeys-3-128", 4096, 128, 3, 128},
		{"SecondaryKeys-4-128", 4096, 128, 4, 128},
		{"SecondaryKeys-8-128", 4096, 128, 8, 128},
		{"SecondaryKeys-16-128", 4096, 128, 16, 128},
		{"SecondaryKeys-128-128", 4096, 128, 128, 128},
	}

	out := &segmentReplaceNode{}
	tempDir := b.TempDir()

	for _, tc := range testCases {
		data, err := generateTestData(b, tc.valueSize, tc.keySize, tc.secondaryKeysCount, tc.secondaryKeySize)
		require.NoErrorf(b, err, "error generating test data")
		dataLen := len(data)

		tempFile := makeTempFile(b, tempDir, tc, data)

		benchmarkWithGCMetrics(b, fmt.Sprintf("DirectFileAccess-%s-%d", tc.name, dataLen), func(b *testing.B) {
			runDirectFileAccess(b, tc, data, tempFile, out)
		})

		benchmarkWithGCMetrics(b, fmt.Sprintf("BufferedFileAccess-%s-%d", tc.name, dataLen), func(b *testing.B) {
			runBufferedFileAccess(b, tc, data, tempFile, out)
		})

		benchmarkWithGCMetrics(b, fmt.Sprintf("PreloadBufferAccess-%s-%d", tc.name, dataLen), func(b *testing.B) {
			runPreloadBufferAccess(b, tc, data, tempFile, out)
		})

		benchmarkWithGCMetrics(b, fmt.Sprintf("FileBufferingOnly-%s-%d", tc.name, dataLen), func(b *testing.B) {
			runFileBufferingOnly(b, data, tempFile)
		})

		benchmarkWithGCMetrics(b, fmt.Sprintf("FileParsingOnly-%s-%d", tc.name, dataLen), func(b *testing.B) {
			runFileParsingOnly(b, tc, data, tempFile, out)
		})
	}
}

// benchmarkWithGCMetrics runs a benchmark and reports GC pressure metrics
func benchmarkWithGCMetrics(b *testing.B, name string, benchFn func(b *testing.B)) {
	b.Run(name, func(b *testing.B) {
		var memStatsBeforeGC, memStatsAfterGC runtime.MemStats

		// Force GC before measurement to get a clean slate
		runtime.GC()
		runtime.ReadMemStats(&memStatsBeforeGC)
		benchFn(b)
		runtime.ReadMemStats(&memStatsAfterGC)

		cycles := float64(memStatsAfterGC.NumGC - memStatsBeforeGC.NumGC)
		b.ReportMetric(cycles/float64(b.N), "num/op")

		pauseTimeNanos := float64(memStatsAfterGC.PauseTotalNs - memStatsBeforeGC.PauseTotalNs)
		b.ReportMetric(pauseTimeNanos/float64(b.N), "ns/op")
	})
}

// runFileParsingOnly measures the cost of parsing when the file is already in memory.
// This isolates parsing performance without any file reading overhead.
func runFileParsingOnly(b *testing.B, tc testCase, data []byte, tempFile string, out *segmentReplaceNode,
) {
	fileContents, err := os.ReadFile(tempFile) // Read file before timing.
	require.NoErrorf(b, err, "error reading file %s", tempFile)

	reader := bytes.NewReader(fileContents)
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(fileContents)
		err = ParseReplaceNodeIntoPread(reader, uint16(tc.secondaryKeysCount), out)
		require.NoErrorf(b, err, "error parsing file %s", tempFile)
	}
}

// runFileBufferingOnly measures the cost of reading the file into a memory buffer (without parsing).
// This isolates the I/O overhead from parsing.
func runFileBufferingOnly(b *testing.B, data []byte, tempFile string) {
	var err error
	file, cleanup := openFile(b, tempFile)
	b.Cleanup(cleanup)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = file.Seek(0, 0)
		require.NoError(b, err, "error seeking file: %v", err)

		_, err = io.ReadAll(file) // Includes measuring the cost of reading the file into memory.
		require.NoErrorf(b, err, "error reading all: %v", err)
	}
}

// runPreloadBufferAccess measures parsing performance when the file has already been loaded into memory and a shared buffer is used.
// This isolates the parsing cost and avoids measuring I/O overhead.
func runPreloadBufferAccess(b *testing.B, tc testCase, data []byte, tempFile string, out *segmentReplaceNode,
) {
	fileContents, err := os.ReadFile(tempFile) // File read before benchmark timing.
	require.NoErrorf(b, err, "error reading file: %v", err)

	reader := bytes.NewReader(fileContents)
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(fileContents) // Resets reader instead of allocating a new one.
		err = ParseReplaceNodeIntoPread(reader, uint16(tc.secondaryKeysCount), out)
		require.NoErrorf(b, err, "error parsing test data: %v", err)
	}
}

// runBufferedFileAccess measures performance of a buffered approach where the entire file is read into memory first,
// then parsed from a memory buffer. Includes the cost of reading the file into memory.
func runBufferedFileAccess(b *testing.B, tc testCase, data []byte, tempFile string, out *segmentReplaceNode,
) {
	file, cleanup := openFile(b, tempFile)
	b.Cleanup(cleanup)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := file.Seek(0, 0)
		require.NoError(b, err, "error seeking file: %v", err)

		all, err := io.ReadAll(file) // Reading full file into memory.
		require.NoErrorf(b, err, "error reading data: %v", err)

		require.NotEmpty(b, all, "file is empty")

		err = ParseReplaceNodeIntoPread(bytes.NewReader(all), uint16(tc.secondaryKeysCount), out)
		require.NoErrorf(b, err, "error parsing test data: %v", err)
	}
}

// runDirectFileAccess measures performance of direct file access using multiple `pread` calls.
// Includes the overhead of system calls for reading from disk.
func runDirectFileAccess(b *testing.B, tc testCase, data []byte, tempFile string, out *segmentReplaceNode,
) {
	var err error
	file, cleanup := openFile(b, tempFile)
	b.Cleanup(cleanup)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = file.Seek(0, 0)
		require.NoErrorf(b, err, "error seeking file: %v", err)

		err = ParseReplaceNodeIntoPread(file, uint16(tc.secondaryKeysCount), out)
		require.NoErrorf(b, err, "error parsing test data: %v", err)
	}
}

func randomTombstone() (int, error) {
	b := make([]byte, 1)
	if _, err := rand.Read(b); err != nil {
		return 0, err
	}
	return int(b[0] & 1), nil
}

func generateTestData(b *testing.B, valueSize, keySize, secondaryKeysCount, secondaryKeySize int) ([]byte, error) {
	b.Helper()

	var buffer bytes.Buffer
	var err error

	tombstone, err := randomTombstone()
	if err != nil {
		return nil, fmt.Errorf("error generating random tombstone: %w", err)
	}

	if err = binary.Write(&buffer, binary.LittleEndian, tombstone == 0); err != nil {
		return nil, fmt.Errorf("error writing tombstone binary: %w", err)
	}

	if err = binary.Write(&buffer, binary.LittleEndian, uint64(valueSize)); err != nil {
		return nil, fmt.Errorf("error writing valueSize binary: %w", err)
	}

	valueBuffer := make([]byte, valueSize)
	if _, err = rand.Read(valueBuffer); err != nil {
		return nil, fmt.Errorf("error reading valueSize binary: %w", err)
	}
	buffer.Write(valueBuffer)

	if err = binary.Write(&buffer, binary.LittleEndian, uint32(keySize)); err != nil {
		return nil, fmt.Errorf("error writing keySize binary: %w", err)
	}

	keyBuffer := make([]byte, keySize)
	if _, err = rand.Read(keyBuffer); err != nil {
		return nil, fmt.Errorf("error reading keySize binary: %w", err)
	}
	buffer.Write(keyBuffer)

	for i := 0; i < secondaryKeysCount; i++ {
		if err = binary.Write(&buffer, binary.LittleEndian, uint32(secondaryKeySize)); err != nil {
			return nil, fmt.Errorf("error writing secondaryKeySize binary: %w", err)
		}

		if secondaryKeySize > 0 {
			secondaryKeyBuffer := make([]byte, secondaryKeySize)
			if _, err := rand.Read(secondaryKeyBuffer); err != nil {
				return nil, fmt.Errorf("error reading secondaryKeySize binary: %w", err)
			}
			buffer.Write(secondaryKeyBuffer)
		}
	}

	return buffer.Bytes(), nil
}

func openFile(b *testing.B, tempFile string) (*os.File, func()) {
	file, err := os.Open(tempFile)
	require.NoErrorf(b, err, "error opening temp file: %v", err)
	cleanup := func() {
		err := file.Close()
		require.NoErrorf(b, err, "error closing temp file: %v", err)
	}
	return file, cleanup
}

func makeTempFile(b *testing.B, tempDir string, tc testCase, data []byte,
) string {
	tempFile := filepath.Join(tempDir, fmt.Sprintf("%s.dat", tc.name))
	err := os.WriteFile(tempFile, data, 0o644)
	require.NoErrorf(b, err, "error writing temp file: %v", err)
	return tempFile
}

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
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
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

func BenchmarkFileParseReplaceNode(b *testing.B) {
	testCases := []struct {
		name               string
		valueSize          int
		keySize            int
		secondaryKeysCount int
		secondaryKeySize   int
	}{
		{"SecondaryKeys-0-0", 4096, 128, 0, 0},
		{"SecondaryKeys-4-128", 4096, 128, 4, 128},
		{"SecondaryKeys-16-128", 4096, 128, 16, 128},
		{"SecondaryKeys-128-128", 4096, 128, 128, 128},
		{"SecondaryKeys-1024-128", 4096, 128, 1024, 128},
		{"SecondaryKeys-1024-512", 4096, 128, 1024, 512},
	}

	out := &segmentReplaceNode{}
	tempDir, cleanup := makeTempDir(b)
	defer cleanup()

	for _, tc := range testCases {
		data, err := generateTestData(tc.valueSize, tc.keySize, tc.secondaryKeysCount, tc.secondaryKeySize)
		if err != nil {
			b.Fatal("error generating test data:", err)
		}
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
			runFileBufferingOnly(b, tc, data, tempFile)
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
func runFileParsingOnly(b *testing.B, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte, tempFile string, out *segmentReplaceNode) {
	fileContents, err := os.ReadFile(tempFile) // Read file before timing.
	if err != nil {
		b.Fatal("Failed to read file:", err)
	}

	reader := bytes.NewReader(fileContents)
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(fileContents)
		err = ParseReplaceNodeIntoPread(reader, uint16(tc.secondaryKeysCount), out)
		if err != nil {
			b.Fatal("error parsing test data:", err)
		}
	}
}

// runFileBufferingOnly measures the cost of reading the file into a memory buffer (without parsing).
// This isolates the I/O overhead from parsing.
func runFileBufferingOnly(b *testing.B, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte, tempFile string) {
	file, cleanup := openFile(b, tempFile)
	defer cleanup()

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := file.Seek(0, 0); err != nil {
			b.Fatal("Failed to seek file:", err)
		}

		all, err := io.ReadAll(file) // Includes measuring the cost of reading the file into memory.
		if err != nil {
			b.Fatal("error reading data:", err)
		}
		runtime.KeepAlive(all) // Prevents compiler optimizations.
	}
}

// runPreloadBufferAccess measures parsing performance when the file has already been loaded into memory and a shared buffer is used.
// This isolates the parsing cost and avoids measuring I/O overhead.
func runPreloadBufferAccess(b *testing.B, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte, tempFile string, out *segmentReplaceNode) {
	fileContents, err := os.ReadFile(tempFile) // File read before benchmark timing.
	if err != nil {
		b.Fatal("Failed to read file:", err)
	}

	reader := bytes.NewReader(fileContents)
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset(fileContents) // Resets reader instead of allocating a new one.
		err = ParseReplaceNodeIntoPread(reader, uint16(tc.secondaryKeysCount), out)
		if err != nil {
			b.Fatal("error parsing test data:", err)
		}
	}
}

// runBufferedFileAccess measures performance of a buffered approach where the entire file is read into memory first,
// then parsed from a memory buffer. Includes the cost of reading the file into memory.
func runBufferedFileAccess(b *testing.B, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte, tempFile string, out *segmentReplaceNode) {
	file, cleanup := openFile(b, tempFile)
	defer cleanup()

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := file.Seek(0, 0); err != nil {
			b.Fatalf("Failed to seek file: %v", err)
		}

		all, err := io.ReadAll(file) // Reading full file into memory.
		if err != nil {
			b.Fatal("error reading file:", err)
		}

		if len(all) == 0 {
			b.Fatalf("file is empty")
		}

		err = ParseReplaceNodeIntoPread(bytes.NewReader(all), uint16(tc.secondaryKeysCount), out)
		if err != nil {
			b.Fatal("error parsing test data:", err)
		}
	}
}

// runDirectFileAccess measures performance of direct file access using multiple `pread` calls.
// Includes the overhead of system calls for reading from disk.
func runDirectFileAccess(b *testing.B, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte, tempFile string, out *segmentReplaceNode) {
	file, cleanup := openFile(b, tempFile)
	defer cleanup()

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := file.Seek(0, 0); err != nil {
			b.Fatal("Failed to seek file:", err)
		}

		err := ParseReplaceNodeIntoPread(file, uint16(tc.secondaryKeysCount), out)
		if err != nil {
			b.Fatal("error parsing test data:", err)
		}
	}
}

func generateTestData(valueSize, keySize, secondaryKeysCount, secondaryKeySize int) ([]byte, error) {
	buffer := new(bytes.Buffer)
	var err error

	if err = binary.Write(buffer, binary.LittleEndian, byte(rand.Intn(2))); err != nil {
		return nil, fmt.Errorf("error writing tombstone binary: %w", err)
	}

	if err = binary.Write(buffer, binary.LittleEndian, uint64(valueSize)); err != nil {
		return nil, fmt.Errorf("error writing valueSize binary: %w", err)
	}

	valueBuffer := make([]byte, valueSize)
	if _, err = crand.Read(valueBuffer); err != nil {
		return nil, fmt.Errorf("error reading valueSize binary: %w", err)
	}
	buffer.Write(valueBuffer)

	if err = binary.Write(buffer, binary.LittleEndian, uint32(keySize)); err != nil {
		return nil, fmt.Errorf("error writing keySize binary: %w", err)
	}

	keyBuffer := make([]byte, keySize)
	if _, err = crand.Read(keyBuffer); err != nil {
		return nil, fmt.Errorf("error reading keySize binary: %w", err)
	}
	buffer.Write(keyBuffer)

	for i := 0; i < secondaryKeysCount; i++ {
		if err = binary.Write(buffer, binary.LittleEndian, uint32(secondaryKeySize)); err != nil {
			return nil, fmt.Errorf("error writing secondaryKeySize binary: %w", err)
		}

		if secondaryKeySize > 0 {
			secondaryKeyBuffer := make([]byte, secondaryKeySize)
			if _, err := crand.Read(secondaryKeyBuffer); err != nil {
				return nil, fmt.Errorf("error reading secondaryKeySize binary: %w", err)
			}
			buffer.Write(secondaryKeyBuffer)
		}
	}

	return buffer.Bytes(), nil
}

func makeTempDir(b *testing.B) (string, func()) {
	tempDir, err := os.MkdirTemp("", "benchmark-*")
	if err != nil {
		b.Fatal("Failed to create temp directory:", err)
	}
	cleanup := func() {
		err := os.RemoveAll(tempDir)
		if err != nil {
			b.Fatalf("Failed to remove temp directory %q: %v", tempDir, err)
		}
	}
	return tempDir, cleanup
}

func openFile(b *testing.B, tempFile string) (*os.File, func()) {
	file, err := os.Open(tempFile)
	if err != nil {
		b.Fatal("Failed to open file:", err)
	}
	cleanup := func() {
		err := file.Close()
		if err != nil {
			b.Fatalf("Failed to close file %q: %v", tempFile, err)
		}
	}
	return file, cleanup
}

func makeTempFile(b *testing.B, tempDir string, tc struct {
	name               string
	valueSize          int
	keySize            int
	secondaryKeysCount int
	secondaryKeySize   int
}, data []byte) string {
	tempFile := filepath.Join(tempDir, fmt.Sprintf("%s.dat", tc.name))
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		b.Fatal("Failed to write test data to file:", err)
	}
	return tempFile
}

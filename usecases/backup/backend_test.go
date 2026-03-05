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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestCalculateShardPreCompressionSize(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Create test files with known sizes
	testFiles := []string{
		"test1.db",
		"test2.db",
		"test3.db",
	}

	// Create files with specific content
	fileSizes := make(map[string]int64)
	for i, filename := range testFiles {
		content := make([]byte, 100*(i+1)) // 100, 200, 300 bytes
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, content, 0o644)
		require.NoError(t, err)
		fileSizes[filename] = int64(len(content))
	}

	// Create a test shard descriptor
	shard := &backup.ShardDescriptor{
		Name:  "test-shard",
		Node:  "test-node",
		Files: testFiles,
	}

	// Create a mock backend with expectations
	mockBackend := modulecapabilities.NewMockBackupBackend(t)
	mockBackend.EXPECT().SourceDataPath().Return(tempDir)

	// Create a mock uploader with the temp directory as source path
	uploader := &uploader{
		backend: nodeStore{
			objectStore: objectStore{
				backend: mockBackend,
			},
		},
		log: logrus.New(),
	}

	// Calculate pre-compression size
	preCompressionSize := uploader.calculateShardPreCompressionSize(shard)

	assert.Equal(t, int64(100+200+300), preCompressionSize)
}

func TestCreateFileList(t *testing.T) {
	t.Run("success with existing files", func(t *testing.T) {
		tempDir := t.TempDir()

		testFiles := []string{"file1.db", "file2.db"}
		for i, filename := range testFiles {
			content := make([]byte, 100*(i+1))
			err := os.WriteFile(filepath.Join(tempDir, filename), content, 0o644)
			require.NoError(t, err)
		}

		shard := &backup.ShardDescriptor{
			Name:  "test-shard",
			Node:  "test-node",
			Files: testFiles,
		}

		mockBackend := modulecapabilities.NewMockBackupBackend(t)
		mockBackend.EXPECT().SourceDataPath().Return(tempDir)

		u := &uploader{
			backend: nodeStore{
				objectStore: objectStore{
					backend: mockBackend,
				},
			},
			log: logrus.New(),
		}

		fileList, err := u.createFileList(shard)
		require.NoError(t, err)
		require.NotNil(t, fileList)
		assert.Equal(t, testFiles, fileList.Files)
		assert.Equal(t, int64(100), fileList.FileSizes["file1.db"])
		assert.Equal(t, int64(200), fileList.FileSizes["file2.db"])
	})

	t.Run("success with delete marker file", func(t *testing.T) {
		tempDir := t.TempDir()

		// Create a file with delete marker prefix
		deletedFile := "deleted.db"
		deletedFilePath := filepath.Join(tempDir, backup.DeleteMarkerAdd(deletedFile))
		err := os.WriteFile(deletedFilePath, make([]byte, 150), 0o644)
		require.NoError(t, err)

		shard := &backup.ShardDescriptor{
			Name:  "test-shard",
			Node:  "test-node",
			Files: []string{deletedFile},
		}

		mockBackend := modulecapabilities.NewMockBackupBackend(t)
		mockBackend.EXPECT().SourceDataPath().Return(tempDir)

		u := &uploader{
			backend: nodeStore{
				objectStore: objectStore{
					backend: mockBackend,
				},
			},
			log: logrus.New(),
		}

		fileList, err := u.createFileList(shard)
		require.NoError(t, err)
		require.NotNil(t, fileList)
		assert.Equal(t, int64(150), fileList.FileSizes[deletedFile])
	})

	t.Run("error when file not found at either path", func(t *testing.T) {
		tempDir := t.TempDir()

		// Create one existing file
		existingFile := "existing.db"
		err := os.WriteFile(filepath.Join(tempDir, existingFile), make([]byte, 100), 0o644)
		require.NoError(t, err)

		// Reference a file that doesn't exist
		missingFile := "missing.db"
		shard := &backup.ShardDescriptor{
			Name:  "test-shard",
			Node:  "test-node",
			Files: []string{existingFile, missingFile},
		}

		mockBackend := modulecapabilities.NewMockBackupBackend(t)
		mockBackend.EXPECT().SourceDataPath().Return(tempDir)

		u := &uploader{
			backend: nodeStore{
				objectStore: objectStore{
					backend: mockBackend,
				},
			},
			log: logrus.New(),
		}

		fileList, err := u.createFileList(shard)
		require.Error(t, err)
		require.Nil(t, fileList)
		assert.Contains(t, err.Error(), "missing.db")
		assert.Contains(t, err.Error(), "not found")
	})
}

// TestProcessShard tests the full write path from shard files to chunks with only
// the storage backend mocked. Real files are created on disk, and the real compress
// + createFileList logic runs end-to-end.
//
// Sizing note: with fewer than 100 files, Top100Size equals the smallest file size
// (clamped to MinChunkSize). bigFileThreshold = max(MinChunkSize, Top100Size).
// Files >= bigFileThreshold are "big" and get their own chunk.
// chunkTargetSize = max(ChunkTargetSize, bigFileThreshold).
// To have small files pack together, set MinChunkSize above the file sizes.
func TestProcessShard(t *testing.T) {
	type testFile struct {
		relPath string
		size    int
	}

	// drainWriter is a mock Write implementation that drains the reader to unblock the pipe.
	drainWriter := func(_ context.Context, _ string, _ string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
		n, _ := io.Copy(io.Discard, r)
		r.Close()
		return n, nil
	}

	// collectingWriter returns a Write func that records the chunk keys and drains the reader.
	collectingWriter := func(mu *sync.Mutex, keys *[]string) func(context.Context, string, string, string, string, backup.ReadCloserWithError) (int64, error) {
		return func(_ context.Context, _ string, key string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
			mu.Lock()
			*keys = append(*keys, key)
			mu.Unlock()
			n, _ := io.Copy(io.Discard, r)
			r.Close()
			return n, nil
		}
	}

	inMemData := bytes.Repeat([]byte("M"), 50) // 3 in-memory files × 50 = 150 bytes total

	tests := []struct {
		name                 string
		files                []testFile
		chunkTargetSize      int64
		minChunkSize         int64
		splitFileSize        int64
		expectChunks         int
		expectBigFilesChunks map[string]int // relPath → expected number of chunk keys in BigFilesChunk
	}{
		{
			name: "all files fit in one chunk",
			files: []testFile{
				{"shard1/a.db", 100},
				{"shard1/b.db", 100},
				{"shard1/c.db", 100},
			},
			// MinChunkSize=500 > file sizes → files are "small" and pack together.
			// chunkTargetSize = max(5000, 500) = 5000.
			// in-mem(150) + 3×100 = 450 < 5000 → all in one chunk.
			minChunkSize:    500,
			chunkTargetSize: 5000,
			expectChunks:    1,
		},
		{
			name: "files split across multiple chunks",
			files: []testFile{
				{"shard1/a.db", 200},
				{"shard1/b.db", 200},
				{"shard1/c.db", 200},
				{"shard1/d.db", 200},
			},
			// MinChunkSize=500 → bigFileThreshold=500, chunkTargetSize=500.
			// Files (200 each) < 500 → pack together.
			// Chunk 1: in-mem(150) + a(200) = 350. b: 550 > 500 → full.
			// Chunk 2: b(200) + c(200) = 400. d: 600 > 500 → full.
			// Chunk 3: d(200).
			minChunkSize:    500,
			chunkTargetSize: 500,
			expectChunks:    3,
		},
		{
			name: "big file gets own chunk",
			files: []testFile{
				{"shard1/small.db", 50},
				{"shard1/big.db", 500},
				{"shard1/small2.db", 50},
			},
			// MinChunkSize=200 → Top100Size=max(50,200)=200, minIndiv=200, chunkTarget=5000.
			// small(50) < 200 → packs. big(500) >= 200 → big → deferred.
			// Chunk 1: in-mem(150) + small(50) = 200. big is next but "big" and !firstFile
			//   → fillChunkWithSmallFiles picks up small2(50) = 250.
			// Chunk 2: big(500) alone.
			minChunkSize:         200,
			chunkTargetSize:      5000,
			expectChunks:         2,
			expectBigFilesChunks: map[string]int{"shard1/big.db": 1},
		},
		{
			name: "file split across chunks via splitFile mechanism",
			files: []testFile{
				{"shard1/small.db", 50},
				{"shard1/huge.db", 1000},
			},
			// MinChunkSize=200 → Top100Size=max(50,200)=200, minIndiv=200, chunkTarget=300.
			// small(50) < 200 → packs. huge(1000) >= 200 → big.
			// Chunk 1: in-mem(150) + small(50) = 200. huge deferred (big, !firstFile).
			// Chunk 2: huge is first file, big → WriteRegular: 1000 > 300 (splitFileSize)
			//   → WriteSplitFile writes first 300 bytes, returns SplitFile{AW:300}.
			// Chunk 3: WriteSplitFile writes 300 bytes (300-600).
			// Chunk 4: WriteSplitFile writes 300 bytes (600-900).
			// Chunk 5: WriteSplitFile writes remaining 100 bytes.
			minChunkSize:         200,
			chunkTargetSize:      300,
			splitFileSize:        300,
			expectChunks:         5,
			expectBigFilesChunks: map[string]int{"shard1/huge.db": 4}, // 4 chunk keys (chunks 2-5)
		},
		{
			name: "first and only file is big non-split",
			files: []testFile{
				{"shard1/big.db", 500},
			},
			// MinChunkSize=200 → Top100Size=max(500,200)=500, minIndiv=500, chunkTarget=5000.
			// Chunk 1 (firstChunk=true): in-mem(150). big.db(500) >= 500 → big, !firstFile → deferred.
			// Chunk 2 (firstChunk=false): big.db first, big → written alone. Tracked in BigFilesChunk.
			minChunkSize:         200,
			chunkTargetSize:      5000,
			expectChunks:         2,
			expectBigFilesChunks: map[string]int{"shard1/big.db": 1},
		},
		{
			name: "first real file is big and triggers split",
			files: []testFile{
				{"shard1/huge.db", 1000},
				{"shard1/small.db", 50},
			},
			// small.db brings Top100Size down: max(50, 200) = 200.
			// minIndiv=200, chunkTarget=max(300, 200)=300, splitFileSize=300.
			// huge(1000) >= 200 → big. small(50) < 200 → packs.
			// Chunk 1: in-mem(150). huge is first but big & !firstFile → fillSmall picks small(50)=200.
			// Chunk 2: huge first, big → WriteRegular: 1000 > 300 → WriteSplitFile writes first 300.
			// Chunks 3-4: split parts of 300 bytes each.
			// Chunk 5: final split part of 100 bytes.
			minChunkSize:         200,
			chunkTargetSize:      300,
			splitFileSize:        300,
			expectChunks:         5,
			expectBigFilesChunks: map[string]int{"shard1/huge.db": 4},
		},
		{
			name: "single big file is never split because chunkTarget adapts",
			files: []testFile{
				{"shard1/huge.db", 1000},
			},
			// With a single file, Top100Size = max(fileSize, MinChunkSize) >= fileSize.
			// So chunkTargetSize >= bigFileThreshold >= fileSize, and
			// 0+fileSize > chunkTargetSize is always false → file is written whole.
			minChunkSize:    200,
			chunkTargetSize: 300,
			splitFileSize:   300,
			expectChunks:    2, // in-mem + big file written whole
		},
		{
			name: "multiple files each requiring splitting",
			files: []testFile{
				{"shard1/small.db", 50},
				{"shard1/huge1.db", 900},
				{"shard1/huge2.db", 900},
			},
			// MinChunkSize=200 → Top100Size=max(50,200)=200, minIndiv=200, chunkTarget=300.
			// small(50) < 200 → packs. huge1(900) >= 200 → big. huge2(900) >= 200 → big.
			// Chunk 1: in-mem(150) + small(50) = 200. huge1 deferred (big, !firstFile).
			// Chunk 2: huge1 first, big → WriteRegular: 900 > 300 → WriteSplitFile writes first 300.
			// Chunks 3-4: split parts of huge1 (300, 300 bytes). huge1 done, filesInShard has huge2.
			// Chunk 5: huge2 first, big → WriteRegular: 900 > 300 → WriteSplitFile writes first 300.
			// Chunks 6-7: split parts of huge2 (300, 300 bytes). huge2 done.
			minChunkSize:         200,
			chunkTargetSize:      300,
			splitFileSize:        300,
			expectChunks:         7,
			expectBigFilesChunks: map[string]int{"shard1/huge1.db": 3, "shard1/huge2.db": 3},
		},
		{
			name: "each file is big and gets own chunk",
			files: []testFile{
				{"shard1/a.db", 100},
				{"shard1/b.db", 200},
				{"shard1/c.db", 300},
			},
			// MinChunkSize=0 → Top100Size=100 (smallest), minIndiv=100, chunkTarget=10000.
			// All files >= 100 → all "big" → each gets own chunk.
			// Chunk 1: in-mem(150). a.db is big, !firstFile → fillSmall: none → return.
			// Chunk 2: a.db alone. Chunk 3: b.db alone. Chunk 4: c.db alone.
			minChunkSize:         0,
			chunkTargetSize:      10000,
			expectChunks:         4,
			expectBigFilesChunks: map[string]int{"shard1/a.db": 1, "shard1/b.db": 1, "shard1/c.db": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "shard1"), os.ModePerm))

			var fileNames []string
			for _, f := range tt.files {
				require.NoError(t, os.WriteFile(
					filepath.Join(tempDir, f.relPath),
					bytes.Repeat([]byte("X"), f.size), 0o644))
				fileNames = append(fileNames, f.relPath)
			}

			var mu sync.Mutex
			var chunkKeys []string

			mockBackend := modulecapabilities.NewMockBackupBackend(t)
			mockBackend.EXPECT().SourceDataPath().Return(tempDir)
			mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(collectingWriter(&mu, &chunkKeys))

			shard := &backup.ShardDescriptor{
				Name:                  "shard1",
				Node:                  "node1",
				Files:                 fileNames,
				DocIDCounterPath:      "shard1/counter.bin",
				DocIDCounter:          inMemData,
				PropLengthTrackerPath: "shard1/proplength.bin",
				PropLengthTracker:     inMemData,
				ShardVersionPath:      "shard1/version.bin",
				Version:               inMemData,
			}

			u := &uploader{
				cfg: config.Backup{
					ChunkTargetSize: tt.chunkTargetSize,
					MinChunkSize:    tt.minChunkSize,
					SplitFileSize:   tt.splitFileSize,
				},
				backend: nodeStore{
					objectStore: objectStore{
						backend: mockBackend,
					},
				},
				zipConfig: zipConfig{
					Level:      int(NoCompression),
					GoPoolSize: 1,
				},
				log: logrus.New(),
			}

			lastChunk := int32(0)
			results, err := u.processShard(context.Background(), shard, "TestClass", &lastChunk, "", "")
			require.NoError(t, err)
			require.Len(t, results, tt.expectChunks, "expected %d chunks", tt.expectChunks)

			// Verify sequential chunk IDs.
			for i, r := range results {
				assert.Equal(t, int32(i+1), r.chunk, "chunk ID at index %d", i)
				assert.Equal(t, []string{"shard1"}, r.shards)
			}
			assert.Equal(t, int32(tt.expectChunks), lastChunk)
			assert.Len(t, chunkKeys, tt.expectChunks, "backend.Write call count")

			// Verify BigFilesChunk tracking.
			if tt.expectBigFilesChunks != nil {
				require.NotNil(t, shard.BigFilesChunk, "expected BigFilesChunk to be populated")
				assert.Len(t, shard.BigFilesChunk, len(tt.expectBigFilesChunks), "BigFilesChunk entry count")
				for relPath, expectedKeyCount := range tt.expectBigFilesChunks {
					info, ok := shard.BigFilesChunk[relPath]
					require.True(t, ok, "expected %s in BigFilesChunk", relPath)
					assert.Len(t, info.ChunkKeys, expectedKeyCount, "chunk key count for %s", relPath)
				}
			}
		})
	}

	t.Run("shared lastChunk counter across shards", func(t *testing.T) {
		tempDir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "s1"), os.ModePerm))
		require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "s2"), os.ModePerm))
		require.NoError(t, os.WriteFile(filepath.Join(tempDir, "s1", "a.db"), bytes.Repeat([]byte("A"), 100), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(tempDir, "s2", "b.db"), bytes.Repeat([]byte("B"), 100), 0o644))

		mockBackend := modulecapabilities.NewMockBackupBackend(t)
		mockBackend.EXPECT().SourceDataPath().Return(tempDir)
		mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(drainWriter)

		u := &uploader{
			cfg: config.Backup{ChunkTargetSize: 10000, MinChunkSize: 500},
			backend: nodeStore{
				objectStore: objectStore{backend: mockBackend},
			},
			zipConfig: zipConfig{Level: int(NoCompression), GoPoolSize: 1},
			log:       logrus.New(),
		}

		lastChunk := int32(0)

		results1, err := u.processShard(context.Background(),
			&backup.ShardDescriptor{
				Name: "s1", Node: "node1", Files: []string{"s1/a.db"},
				DocIDCounterPath: "s1/c.bin", DocIDCounter: inMemData,
				PropLengthTrackerPath: "s1/p.bin", PropLengthTracker: inMemData,
				ShardVersionPath: "s1/v.bin", Version: inMemData,
			},
			"TestClass", &lastChunk, "", "")
		require.NoError(t, err)

		results2, err := u.processShard(context.Background(),
			&backup.ShardDescriptor{
				Name: "s2", Node: "node1", Files: []string{"s2/b.db"},
				DocIDCounterPath: "s2/c.bin", DocIDCounter: inMemData,
				PropLengthTrackerPath: "s2/p.bin", PropLengthTracker: inMemData,
				ShardVersionPath: "s2/v.bin", Version: inMemData,
			},
			"TestClass", &lastChunk, "", "")
		require.NoError(t, err)

		assert.Equal(t, int32(1), results1[0].chunk)
		assert.Equal(t, int32(2), results2[0].chunk)
		assert.Equal(t, int32(2), lastChunk)
	})
}

// TestProcessShardEvenSplitSizes verifies that split file chunks are roughly
// the same size rather than producing one large chunk and a tiny remainder.
// Uses NoCompression so archive sizes directly reflect data sizes.
func TestProcessShardEvenSplitSizes(t *testing.T) {
	inMemData := bytes.Repeat([]byte("M"), 50)

	tests := []struct {
		name          string
		fileSize      int
		splitFileSize int64
		chunkTarget   int64
		minChunkSize  int64
		wantParts     int
	}{
		{
			name:          "6000 split by 5000 → 2 even chunks",
			fileSize:      6000,
			splitFileSize: 5000,
			chunkTarget:   5000,
			minChunkSize:  200,
			wantParts:     2,
		},
		{
			name:          "1000 split by 300 → 4 even chunks",
			fileSize:      1000,
			splitFileSize: 300,
			chunkTarget:   300,
			minChunkSize:  200,
			wantParts:     4,
		},
		{
			name:          "700 split by 300 → 3 even chunks",
			fileSize:      700,
			splitFileSize: 300,
			chunkTarget:   300,
			minChunkSize:  200,
			wantParts:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "shard1"), os.ModePerm))

			// Small file pulls bigFileThreshold down so the large file takes the split path.
			require.NoError(t, os.WriteFile(
				filepath.Join(tempDir, "shard1", "small.db"),
				bytes.Repeat([]byte("s"), 50), 0o644))
			require.NoError(t, os.WriteFile(
				filepath.Join(tempDir, "shard1", "big.db"),
				bytes.Repeat([]byte("B"), tt.fileSize), 0o644))

			var mu sync.Mutex
			chunkSizes := make(map[string]int64)

			mockBackend := modulecapabilities.NewMockBackupBackend(t)
			mockBackend.EXPECT().SourceDataPath().Return(tempDir)
			mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, _ string, key string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
					n, _ := io.Copy(io.Discard, r)
					r.Close()
					mu.Lock()
					chunkSizes[key] = n
					mu.Unlock()
					return n, nil
				})

			shard := &backup.ShardDescriptor{
				Name:                  "shard1",
				Node:                  "node1",
				Files:                 []string{"shard1/small.db", "shard1/big.db"},
				DocIDCounterPath:      "shard1/counter.bin",
				DocIDCounter:          inMemData,
				PropLengthTrackerPath: "shard1/proplength.bin",
				PropLengthTracker:     inMemData,
				ShardVersionPath:      "shard1/version.bin",
				Version:               inMemData,
			}

			u := &uploader{
				cfg: config.Backup{
					ChunkTargetSize: tt.chunkTarget,
					MinChunkSize:    tt.minChunkSize,
					SplitFileSize:   tt.splitFileSize,
				},
				backend: nodeStore{
					objectStore: objectStore{backend: mockBackend},
				},
				zipConfig: zipConfig{Level: int(NoCompression), GoPoolSize: 1},
				log:       logrus.New(),
			}

			lastChunk := int32(0)
			results, err := u.processShard(context.Background(), shard, "TestClass", &lastChunk, "", "")
			require.NoError(t, err)

			// First chunk has small files + in-memory data; skip it.
			// The remaining chunks contain the split parts of big.db.
			require.True(t, len(results) > 1, "expected more than 1 chunk")
			splitChunks := results[1:]
			require.Len(t, splitChunks, tt.wantParts, "number of split chunks")

			// Collect sizes of the split-file chunks.
			var sizes []int64
			for _, r := range splitChunks {
				key := fmt.Sprintf("TestClass/chunk-%d", r.chunk)
				sizes = append(sizes, chunkSizes[key])
			}

			// Assert all split chunks are roughly the same size.
			// With no compression and tar overhead being small relative to data,
			// the largest chunk should be at most 20% bigger than the smallest.
			minSize, maxSize := sizes[0], sizes[0]
			for _, s := range sizes[1:] {
				if s < minSize {
					minSize = s
				}
				if s > maxSize {
					maxSize = s
				}
			}
			assert.True(t, float64(maxSize) <= float64(minSize)*1.2,
				"chunks should be roughly equal: min=%d max=%d (ratio %.1f%%)",
				minSize, maxSize, float64(maxSize)*100/float64(minSize))
		})
	}
}

func TestCalculateTop100Size(t *testing.T) {
	const mb = 1 << 20

	// Helper to create fileSizes map with n files of increasing size starting at 2MB
	makeFiles := func(n int) map[string]int64 {
		files := make(map[string]int64, n)
		for i := 0; i < n; i++ {
			files[filepath.Join("dir", filepath.Base(
				filepath.Join("f", string(rune('a'+i%26))+string(rune('0'+i/26))),
			))] = int64((i + 2)) * mb
		}
		return files
	}

	tests := []struct {
		name            string
		fileSizes       map[string]int64
		numSkippedFiles int
		expected        int64
	}{
		{
			name:            "empty files returns minSize",
			fileSizes:       map[string]int64{},
			numSkippedFiles: 0,
			expected:        mb,
		},
		{
			name:            "empty files with skipped returns minSize",
			fileSizes:       map[string]int64{},
			numSkippedFiles: 50,
			expected:        mb,
		},
		{
			name:            "single small file no skipped",
			fileSizes:       map[string]int64{"a.db": 500},
			numSkippedFiles: 0,
			expected:        mb, // below minSize
		},
		{
			name:            "single large file no skipped",
			fileSizes:       map[string]int64{"a.db": 5 * mb},
			numSkippedFiles: 0,
			expected:        5 * mb,
		},
		{
			name:            "fewer than k files - returns smallest",
			fileSizes:       map[string]int64{"a.db": 2 * mb, "b.db": 5 * mb, "c.db": 3 * mb},
			numSkippedFiles: 0,
			expected:        2 * mb, // k=100, only 3 files, smallest is 2MB
		},
		{
			name:            "skipped reduces k - changes result",
			fileSizes:       makeFiles(105), // files from 2MB to 106MB
			numSkippedFiles: 0,
			expected:        7 * mb, // k=100, 105 files, 100th largest = 6th smallest (index 5) = 7MB
		},
		{
			name:            "skipped 50 reduces k to 50",
			fileSizes:       makeFiles(105), // files from 2MB to 106MB
			numSkippedFiles: 50,
			expected:        57 * mb, // k=50, 50th largest = 56th smallest (index 55) = 57MB
		},
		{
			name:            "skipped 99 reduces k to 1 - returns largest",
			fileSizes:       makeFiles(105),
			numSkippedFiles: 99,
			expected:        106 * mb, // k=1, returns the largest file
		},
		{
			name:            "skipped >= 100 clamps k to 1",
			fileSizes:       makeFiles(105),
			numSkippedFiles: 100,
			expected:        106 * mb, // k=max(100-100,1)=1, returns largest
		},
		{
			name:            "skipped > 100 clamps k to 1",
			fileSizes:       makeFiles(105),
			numSkippedFiles: 200,
			expected:        106 * mb,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := calculateTop100Size(tc.fileSizes, tc.numSkippedFiles, mb)
			assert.Equal(t, tc.expected, result)
		})
	}
}

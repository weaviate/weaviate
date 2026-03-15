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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		t.Parallel()
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
		t.Parallel()
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
		t.Parallel()
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
//
// Cases with expectChunkFiles also verify chunk isolation: metadata files never
// leak into big/split-file chunks, and big files never appear in the metadata chunk.
func TestProcessShard(t *testing.T) {
	type testFile struct {
		relPath string
		size    int
	}

	inMemData := bytes.Repeat([]byte("M"), 50) // 3 in-memory files × 50 = 150 bytes total
	inMemFiles := map[string]struct{}{
		"shard1/counter.bin":    {},
		"shard1/proplength.bin": {},
		"shard1/version.bin":    {},
	}

	tests := []struct {
		name                 string
		files                []testFile
		chunkTargetSize      int64
		minChunkSize         int64
		splitFileSize        int64
		expectChunks         int
		expectBigFilesChunks map[string]int   // relPath → expected number of chunk keys in BigFilesChunk
		expectChunkFiles     map[int][]string // chunk index (0-based) → tar entry names (nil map = skip tar verification)
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
			expectChunkFiles: map[int][]string{
				0: {"shard1/counter.bin", "shard1/proplength.bin", "shard1/version.bin", "shard1/small.db", "shard1/small2.db"},
				1: {"shard1/big.db"},
			},
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
			expectChunkFiles: map[int][]string{
				0: {"shard1/counter.bin", "shard1/proplength.bin", "shard1/version.bin", "shard1/small.db"},
				1: {"shard1/huge.db"},
				2: {"shard1/huge.db"},
				3: {"shard1/huge.db"},
				4: {"shard1/huge.db"},
			},
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
			expectChunkFiles: map[int][]string{
				0: {"shard1/counter.bin", "shard1/proplength.bin", "shard1/version.bin"},
				1: {"shard1/big.db"},
			},
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
			t.Parallel()
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
			chunkData := make(map[string]*bytes.Buffer)

			mockBackend := modulecapabilities.NewMockBackupBackend(t)
			mockBackend.EXPECT().SourceDataPath().Return(tempDir)
			mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, _ string, key string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
					buf := &bytes.Buffer{}
					n, _ := io.Copy(buf, r)
					r.Close()
					mu.Lock()
					chunkData[key] = buf
					mu.Unlock()
					return n, nil
				})

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

			var lastChunk atomic.Int32
			results, err := u.processShard(context.Background(), shard, "TestClass", &lastChunk, "", "")
			require.NoError(t, err)
			require.Len(t, results, tt.expectChunks, "expected %d chunks", tt.expectChunks)

			// Verify sequential chunk IDs.
			for i, r := range results {
				assert.Equal(t, int32(i+1), r.chunk, "chunk ID at index %d", i)
				assert.Equal(t, []string{"shard1"}, r.shards)
			}
			assert.Equal(t, int32(tt.expectChunks), lastChunk.Load())
			assert.Len(t, chunkData, tt.expectChunks, "backend.Write call count")

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

			// Verify tar entry names per chunk (chunk isolation).
			for chunkIdx, expectedFiles := range tt.expectChunkFiles {
				chunkID := int32(chunkIdx + 1)
				key := fmt.Sprintf("TestClass/chunk-%d", chunkID)
				buf, ok := chunkData[key]
				require.True(t, ok, "chunk %d data not captured (key=%s)", chunkIdx, key)

				var tarFiles []string
				tr := tar.NewReader(buf) // NoCompression: raw tar, no gzip wrapper
				for {
					hdr, err := tr.Next()
					if err == io.EOF {
						break
					}
					require.NoError(t, err, "reading tar entry in chunk %d", chunkIdx)
					tarFiles = append(tarFiles, hdr.Name)
				}

				require.ElementsMatch(t, expectedFiles, tarFiles,
					"chunk %d: expected files %v, got %v", chunkIdx, expectedFiles, tarFiles)

				// For the metadata-only chunk (chunk 0), verify no big/split files leaked in.
				if chunkIdx == 0 {
					for _, f := range tarFiles {
						if _, isMeta := inMemFiles[f]; !isMeta {
							for _, tf := range tt.files {
								if tf.relPath == f {
									assert.Less(t, int64(tf.size), tt.minChunkSize,
										"chunk 0 contains big file %s (size=%d) alongside metadata", f, tf.size)
								}
							}
						}
					}
				}

				// For non-first chunks, verify metadata files never appear.
				if chunkIdx > 0 {
					for _, f := range tarFiles {
						_, isMeta := inMemFiles[f]
						assert.False(t, isMeta,
							"chunk %d contains metadata file %s — metadata must only be in the first chunk", chunkIdx, f)
					}
				}
			}
		})
	}

	t.Run("shared lastChunk counter across shards", func(t *testing.T) {
		t.Parallel()
		// drainWriter is a mock Write implementation that drains the reader to unblock the pipe.
		drainWriter := func(_ context.Context, _ string, _ string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
			n, _ := io.Copy(io.Discard, r)
			r.Close()
			return n, nil
		}

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

		var lastChunk atomic.Int32

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
		assert.Equal(t, int32(2), lastChunk.Load())
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
			t.Parallel()
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

			var lastChunk atomic.Int32
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
			t.Parallel()
			result := calculateTop100Size(tc.fileSizes, tc.numSkippedFiles, mb)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestReadAndUnzipChunk_TrailingBytes verifies that readAndUnzipChunk does not
// deadlock when the backend sends trailing bytes after a valid compressed tar
// stream. This reproduces a real-world hang where the gzip/zstd decompressor
// detects end-of-stream and ReadChunk returns, but io.Copy in the readFn
// goroutine is still trying to write remaining bytes to the pipe. Without the
// fix (closing the pipe reader after ReadChunk), the write blocks forever
// because nobody reads from the pipe anymore.
func TestReadAndUnzipChunk_TrailingBytes(t *testing.T) {
	// Build a valid gzip-compressed tar archive containing one small file.
	var archiveBuf bytes.Buffer
	gw := gzip.NewWriter(&archiveBuf)
	tw := tar.NewWriter(gw)
	fileContent := []byte("hello world")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "testfile.txt",
		Mode: 0o644,
		Size: int64(len(fileContent)),
	}))
	_, err := tw.Write(fileContent)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())

	// Append trailing bytes after the valid gzip stream. This simulates
	// extra data on the wire (padding, second gzip member, etc.) that the
	// decompressor does not need. The pipe write of these bytes will block
	// if the pipe reader is not closed after ReadChunk returns.
	trailing := make([]byte, 4096)
	archiveBuf.Write(trailing)
	archiveData := archiveBuf.Bytes()

	destDir := t.TempDir()
	fw := &fileWriter{
		logger: logrus.New(),
	}

	done := make(chan error, 1)
	go func() {
		done <- fw.readAndUnzipChunk(destDir, backup.CompressionGZIP, "test-chunk",
			func(w io.WriteCloser) error {
				defer w.Close()
				_, err := io.Copy(w, bytes.NewReader(archiveData))
				return err
			})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("readAndUnzipChunk deadlocked: pipe reader was not closed after ReadChunk returned")
	}

	// Verify the file was extracted correctly.
	got, err := os.ReadFile(filepath.Join(destDir, "testfile.txt"))
	require.NoError(t, err)
	assert.Equal(t, fileContent, got)
}

// incrementalTestEnv provides shared infrastructure for incremental backup round-trip tests.
// It holds an in-memory chunk store (mock backend) and helpers for running production
// processShard (backup) and writeTempFiles (restore) code paths.
type incrementalTestEnv struct {
	t          *testing.T
	sourceDir  string
	className  string
	chunkMu    sync.Mutex
	chunkStore map[string][]byte
	cfg        config.Backup
	inMemData  []byte
}

func newIncrementalTestEnv(t *testing.T, cfg config.Backup) *incrementalTestEnv {
	return &incrementalTestEnv{
		t:          t,
		sourceDir:  t.TempDir(),
		className:  "TestClass",
		chunkStore: make(map[string][]byte),
		cfg:        cfg,
		inMemData:  bytes.Repeat([]byte("M"), 50),
	}
}

func (e *incrementalTestEnv) writeFile(relPath string, data []byte) {
	e.t.Helper()
	fullPath := filepath.Join(e.sourceDir, relPath)
	require.NoError(e.t, os.MkdirAll(filepath.Dir(fullPath), os.ModePerm))
	require.NoError(e.t, os.WriteFile(fullPath, data, 0o644))
}

func (e *incrementalTestEnv) makeShardDesc(name string, files []string) *backup.ShardDescriptor {
	return &backup.ShardDescriptor{
		Name:                  name,
		Node:                  "node1",
		Files:                 files,
		DocIDCounterPath:      filepath.Join(name, "counter.bin"),
		DocIDCounter:          append([]byte{}, e.inMemData...),
		PropLengthTrackerPath: filepath.Join(name, "proplength.bin"),
		PropLengthTracker:     append([]byte{}, e.inMemData...),
		ShardVersionPath:      filepath.Join(name, "version.bin"),
		Version:               append([]byte{}, e.inMemData...),
	}
}

func (e *incrementalTestEnv) storeWriterFn(backupID string) func(context.Context, string, string, string, string, backup.ReadCloserWithError) (int64, error) {
	return func(_ context.Context, _ string, key string, _ string, _ string, r backup.ReadCloserWithError) (int64, error) {
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			return 0, err
		}
		e.chunkMu.Lock()
		e.chunkStore[backupID+"/"+key] = data
		e.chunkMu.Unlock()
		return int64(len(data)), nil
	}
}

func (e *incrementalTestEnv) serveReaderFn() func(context.Context, string, string, string, string, io.WriteCloser) (int64, error) {
	return func(_ context.Context, backupID, key, _, _ string, w io.WriteCloser) (int64, error) {
		storeKey := backupID + "/" + key
		e.chunkMu.Lock()
		data, ok := e.chunkStore[storeKey]
		e.chunkMu.Unlock()
		if !ok {
			w.Close()
			return 0, fmt.Errorf("chunk not found: %s", storeKey)
		}
		n, err := io.Copy(w, bytes.NewReader(data))
		w.Close()
		return n, err
	}
}

func (e *incrementalTestEnv) backup(backupID string, shardDescs []*backup.ShardDescriptor) map[int32][]string {
	e.t.Helper()
	mockBackend := modulecapabilities.NewMockBackupBackend(e.t)
	mockBackend.EXPECT().SourceDataPath().Return(e.sourceDir)
	mockBackend.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(e.storeWriterFn(backupID + "/node1"))

	u := &uploader{
		cfg: e.cfg,
		backend: nodeStore{
			objectStore: objectStore{
				backend:  mockBackend,
				backupId: backupID + "/node1",
			},
		},
		zipConfig: zipConfig{Level: int(NoCompression), GoPoolSize: 1},
		log:       logrus.New(),
	}

	var lastChunk atomic.Int32
	chunksMap := make(map[int32][]string)
	for _, sd := range shardDescs {
		results, err := u.processShard(context.Background(), sd, e.className, &lastChunk, "", "")
		require.NoError(e.t, err)
		for _, r := range results {
			chunksMap[r.chunk] = r.shards
		}
	}
	return chunksMap
}

func (e *incrementalTestEnv) restore(backupID string, desc *backup.ClassDescriptor) string {
	e.t.Helper()
	restoreDir := e.t.TempDir()
	restoreMock := modulecapabilities.NewMockBackupBackend(e.t)
	// writeTempFiles never calls SourceDataPath — it uses classTempDir directly.
	restoreMock.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(e.serveReaderFn())

	fw := &fileWriter{
		backend: nodeStore{
			objectStore: objectStore{
				backend:  restoreMock,
				backupId: backupID + "/node1",
				node:     "node1",
			},
		},
		destDir:    restoreDir,
		tempDir:    filepath.Join(restoreDir, TempDirectory),
		compressed: true,
		GoPoolSize: 4,
		logger:     logrus.New(),
	}

	classTempDir := filepath.Join(fw.tempDir, e.className)
	err := fw.writeTempFiles(context.Background(), classTempDir, "", "", desc, backup.CompressionNone)
	require.NoError(e.t, err)
	return classTempDir
}

func (e *incrementalTestEnv) verify(restoredDir string, files []string) {
	e.t.Helper()
	for _, relPath := range files {
		originalData, err := os.ReadFile(filepath.Join(e.sourceDir, relPath))
		require.NoError(e.t, err, "read original %s", relPath)
		restoredData, err := os.ReadFile(filepath.Join(restoredDir, relPath))
		require.NoError(e.t, err, "read restored %s (missing from restore)", relPath)
		require.Equal(e.t, originalData, restoredData,
			"file content mismatch for %s", relPath)
	}
}

// TestIncrementalBackupWithChanges tests the full backup→mutate→incremental backup→restore
// cycle using production processShard (backup) and writeTempFiles (restore) code paths.
// Only the storage backend is mocked — chunks are stored in memory.
//
// This test verifies that new segment files (e.g. produced by flushing after changes)
// are correctly included in incremental backups and that restoring base+incremental
// chunks produces the same file state as the live shard directory.
func TestIncrementalBackupWithChanges(t *testing.T) {
	t.Run("single shard", func(t *testing.T) {
		t.Parallel()
		testIncrementalBackupWithChanges(t, 1)
	})
	t.Run("multi shard", func(t *testing.T) {
		t.Parallel()
		testIncrementalBackupWithChanges(t, 3)
	})
}

func testIncrementalBackupWithChanges(t *testing.T, numShards int) {
	env := newIncrementalTestEnv(t, config.Backup{
		ChunkTargetSize: 512,
		MinChunkSize:    500,
		SplitFileSize:   256,
	})
	shard := func(i int) string { return fmt.Sprintf("shard%d", i) }

	// --- Step 1: Create initial shard files on disk ---
	type shardSetup struct {
		name  string
		files []string
	}
	shards := make([]shardSetup, numShards)
	for si := range shards {
		name := shard(si)
		files := []string{
			filepath.Join(name, "segment-001.db"),
			filepath.Join(name, "segment-002.db"),
			filepath.Join(name, "segment-003.db"),
			filepath.Join(name, "big-segment.db"),
		}
		sizes := []int{100, 150, 80, 600}
		for i, f := range files {
			env.writeFile(f, bytes.Repeat([]byte{byte('A' + i)}, sizes[i]))
		}
		shards[si] = shardSetup{name: name, files: files}
	}

	// --- Step 2: Base backup ---
	baseShardDescs := make([]*backup.ShardDescriptor, numShards)
	for si, ss := range shards {
		baseShardDescs[si] = env.makeShardDesc(ss.name, ss.files)
	}
	env.backup("base-backup", baseShardDescs)
	t.Logf("base backup done")

	// --- Step 3: Simulate deletes by adding new segment files ---
	// In a real LSM store, deleting objects writes tombstone entries to the memtable.
	// Flushing produces a new segment file on disk containing those entries.
	// Here we simulate that by creating new segment files per shard.
	newSegments := make(map[string][]string)
	for _, ss := range shards {
		newFile := filepath.Join(ss.name, "segment-004.db")
		env.writeFile(newFile, bytes.Repeat([]byte("S"), 200))
		newSegments[ss.name] = []string{newFile}
	}

	// --- Step 4: Incremental dedup via FillFileInfo ---
	incrShardDescs := make([]*backup.ShardDescriptor, numShards)
	allFilesByDesc := make(map[string][]string)

	for si, ss := range shards {
		allFiles := append(append([]string{}, ss.files...), newSegments[ss.name]...)
		allFilesByDesc[ss.name] = allFiles

		sd := env.makeShardDesc(ss.name, nil)
		require.NoError(t, sd.FillFileInfo(allFiles, []backup.ShardAndID{
			{ShardDesc: baseShardDescs[si], BackupID: "base-backup"},
		}, env.sourceDir))

		t.Logf("shard %s: %d new files, %d skipped",
			ss.name, len(sd.Files), sd.IncrementalBackupInfo.NumFilesSkipped)
		require.NotEmpty(t, sd.Files, "shard %s must have new files", ss.name)
		incrShardDescs[si] = sd
	}

	// --- Step 5: Incremental backup ---
	incrChunks := env.backup("incr-backup", incrShardDescs)
	t.Logf("incremental backup: %d chunks", len(incrChunks))

	// --- Step 6: Restore ---
	restoredDir := env.restore("incr-backup", &backup.ClassDescriptor{
		Name:   env.className,
		Chunks: incrChunks,
		Shards: incrShardDescs,
	})

	// --- Step 7: Verify ---
	for _, ss := range shards {
		env.verify(restoredDir, allFilesByDesc[ss.name])
	}
}

// TestIncrementalBackupSplitFileVariants exercises three split-file scenarios that
// the basic tombstone test does not cover:
//  1. A split file that changes between base and incremental (size/content differ).
//  2. A small file that grows past the split threshold in the incremental.
//  3. A brand-new split file added only in the incremental (not in base at all).
//
// Each scenario uses production processShard + writeTempFiles with a mock backend.
func TestIncrementalBackupSplitFileVariants(t *testing.T) {
	t.Run("modified split file forces re-backup", func(t *testing.T) {
		t.Parallel()
		env := newIncrementalTestEnv(t, config.Backup{
			ChunkTargetSize: 512,
			MinChunkSize:    500,
			SplitFileSize:   256,
		})
		s := "shard0"

		// Base: small file packs into chunk 1; big file (600B) gets split across chunks 2-3.
		env.writeFile(s+"/small.db", bytes.Repeat([]byte("A"), 100))
		env.writeFile(s+"/big.db", bytes.Repeat([]byte("B"), 600))

		baseSd := env.makeShardDesc(s, []string{s + "/small.db", s + "/big.db"})
		env.backup("base", []*backup.ShardDescriptor{baseSd})

		// Verify big.db was split (multiple chunk keys in BigFilesChunk).
		require.NotNil(t, baseSd.BigFilesChunk)
		require.Contains(t, baseSd.BigFilesChunk, s+"/big.db")
		require.Greater(t, len(baseSd.BigFilesChunk[s+"/big.db"].ChunkKeys), 1,
			"big.db should be split across multiple chunks")

		// Modify big.db — different size forces FillFileInfo to re-include it.
		env.writeFile(s+"/big.db", bytes.Repeat([]byte("X"), 800))

		allFiles := []string{s + "/small.db", s + "/big.db"}
		incrSd := env.makeShardDesc(s, nil)
		require.NoError(t, incrSd.FillFileInfo(allFiles, []backup.ShardAndID{
			{ShardDesc: baseSd, BackupID: "base"},
		}, env.sourceDir))

		// big.db size changed → must be re-backed-up, not skipped.
		require.Contains(t, incrSd.Files, s+"/big.db")

		incrChunks := env.backup("incr", []*backup.ShardDescriptor{incrSd})

		// Verify big.db is split again in the incremental.
		require.NotNil(t, incrSd.BigFilesChunk)
		require.Contains(t, incrSd.BigFilesChunk, s+"/big.db")
		require.Greater(t, len(incrSd.BigFilesChunk[s+"/big.db"].ChunkKeys), 1,
			"modified big.db should be re-split in incremental")

		restoredDir := env.restore("incr", &backup.ClassDescriptor{
			Name: env.className, Chunks: incrChunks, Shards: []*backup.ShardDescriptor{incrSd},
		})
		env.verify(restoredDir, allFiles)
	})

	t.Run("small file grows into split territory", func(t *testing.T) {
		t.Parallel()
		env := newIncrementalTestEnv(t, config.Backup{
			ChunkTargetSize: 512,
			MinChunkSize:    500,
			SplitFileSize:   256,
		})
		s := "shard0"

		// Base: two small files, both pack into one chunk. Neither is big.
		env.writeFile(s+"/small.db", bytes.Repeat([]byte("A"), 100))
		env.writeFile(s+"/medium.db", bytes.Repeat([]byte("B"), 200))

		baseSd := env.makeShardDesc(s, []string{s + "/small.db", s + "/medium.db"})
		env.backup("base", []*backup.ShardDescriptor{baseSd})

		// medium.db was small → not tracked in BigFilesChunk.
		if baseSd.BigFilesChunk != nil {
			require.NotContains(t, baseSd.BigFilesChunk, s+"/medium.db")
		}

		// Grow medium.db past big-file + split thresholds (700B > minChunkSize=500, > splitFileSize=256).
		env.writeFile(s+"/medium.db", bytes.Repeat([]byte("G"), 700))

		allFiles := []string{s + "/small.db", s + "/medium.db"}
		incrSd := env.makeShardDesc(s, nil)
		require.NoError(t, incrSd.FillFileInfo(allFiles, []backup.ShardAndID{
			{ShardDesc: baseSd, BackupID: "base"},
		}, env.sourceDir))

		// medium.db was never in BigFilesChunk → FillFileInfo always includes it.
		require.Contains(t, incrSd.Files, s+"/medium.db")

		incrChunks := env.backup("incr", []*backup.ShardDescriptor{incrSd})

		// Verify medium.db is now tracked as big/split in the incremental.
		require.NotNil(t, incrSd.BigFilesChunk)
		require.Contains(t, incrSd.BigFilesChunk, s+"/medium.db")

		restoredDir := env.restore("incr", &backup.ClassDescriptor{
			Name: env.className, Chunks: incrChunks, Shards: []*backup.ShardDescriptor{incrSd},
		})
		env.verify(restoredDir, allFiles)
	})

	t.Run("new split file in incremental only", func(t *testing.T) {
		t.Parallel()
		env := newIncrementalTestEnv(t, config.Backup{
			ChunkTargetSize: 512,
			MinChunkSize:    500,
			SplitFileSize:   256,
		})
		s := "shard0"

		// Base: only a small file.
		env.writeFile(s+"/small.db", bytes.Repeat([]byte("A"), 100))

		baseSd := env.makeShardDesc(s, []string{s + "/small.db"})
		env.backup("base", []*backup.ShardDescriptor{baseSd})

		// Add a brand-new big file that did not exist in the base backup at all.
		env.writeFile(s+"/new-big.db", bytes.Repeat([]byte("N"), 700))

		allFiles := []string{s + "/small.db", s + "/new-big.db"}
		incrSd := env.makeShardDesc(s, nil)
		require.NoError(t, incrSd.FillFileInfo(allFiles, []backup.ShardAndID{
			{ShardDesc: baseSd, BackupID: "base"},
		}, env.sourceDir))

		// new-big.db not in base → must be in Files.
		require.Contains(t, incrSd.Files, s+"/new-big.db")
		// Nothing should be skipped (small.db wasn't big in base either).
		require.Empty(t, incrSd.IncrementalBackupInfo.FilesPerBackup)

		incrChunks := env.backup("incr", []*backup.ShardDescriptor{incrSd})

		// new-big.db should be split across multiple chunks.
		require.NotNil(t, incrSd.BigFilesChunk)
		require.Contains(t, incrSd.BigFilesChunk, s+"/new-big.db")
		require.Greater(t, len(incrSd.BigFilesChunk[s+"/new-big.db"].ChunkKeys), 1,
			"new-big.db should be split across multiple chunks")

		// Restore reads all chunks from the incremental backup only (no ReadFromOtherBackup).
		restoredDir := env.restore("incr", &backup.ClassDescriptor{
			Name: env.className, Chunks: incrChunks, Shards: []*backup.ShardDescriptor{incrSd},
		})
		env.verify(restoredDir, allFiles)
	})
}

// TestIncrementalBackupChainWithManySplitFiles exercises concurrent restoration of many
// split-file parts fetched from a 3-deep incremental backup chain.
//
// Layout: 4 shards × 8 large split files each, 3 backup generations.
// The restore must concurrently fetch split-file chunks from all three backups via
// Read (current) and ReadFromOtherBackup (two different base backups), reassembling
// each file at the correct offsets with WriteAt.
//
// With splitFileSize=100 and files of 300-2000 bytes, each file produces 3-20 split
// chunks. The total chunk count across all backups reaches 200+, all extracted
// concurrently during restore.
func TestIncrementalBackupChainWithManySplitFiles(t *testing.T) {
	// Use small split/chunk sizes to maximize the number of split parts.
	env := newIncrementalTestEnv(t, config.Backup{
		ChunkTargetSize: 300,
		MinChunkSize:    100,
		SplitFileSize:   100,
	})

	type fileSpec struct {
		rel  string
		size int
	}

	writeSpecs := func(specs []fileSpec, fill byte) {
		for _, f := range specs {
			env.writeFile(f.rel, bytes.Repeat([]byte{fill}, f.size))
		}
	}
	relPaths := func(specs []fileSpec) []string {
		out := make([]string, len(specs))
		for i, f := range specs {
			out[i] = f.rel
		}
		return out
	}

	// --- Shard layout ---
	// Each shard has a small anchor file (keeps Top100Size low so bigFileThreshold=100)
	// and 8 large files that will each be split into many chunks (3-20 parts).
	// 4 shards × 8 split files = 32 split files total.
	mkShard := func(name string) []fileSpec {
		return []fileSpec{
			{name + "/small.db", 50},     // anchor: keeps bigFileThreshold=100
			{name + "/seg-001.db", 800},  // 8 parts
			{name + "/seg-002.db", 600},  // 6 parts
			{name + "/seg-003.db", 1200}, // 12 parts
			{name + "/seg-004.db", 500},  // 5 parts
			{name + "/seg-005.db", 1500}, // 15 parts
			{name + "/seg-006.db", 300},  // 3 parts
			{name + "/seg-007.db", 2000}, // 20 parts
			{name + "/seg-008.db", 1000}, // 10 parts
		}
	}

	shardSpecs := [][]fileSpec{
		mkShard("shard0"),
		mkShard("shard1"),
		mkShard("shard2"),
		mkShard("shard3"),
	}
	shardNames := []string{"shard0", "shard1", "shard2", "shard3"}

	for _, specs := range shardSpecs {
		writeSpecs(specs, 'A')
	}

	// ========== Backup 1 (base): all files ==========
	baseDescs := make([]*backup.ShardDescriptor, len(shardNames))
	for i, specs := range shardSpecs {
		baseDescs[i] = env.makeShardDesc(shardNames[i], relPaths(specs))
	}
	env.backup("backup-1", baseDescs)

	totalSplitParts := 0
	for _, sd := range baseDescs {
		require.NotNil(t, sd.BigFilesChunk, "shard %s: BigFilesChunk should be populated", sd.Name)
		for relPath, info := range sd.BigFilesChunk {
			require.Greater(t, len(info.ChunkKeys), 1,
				"shard %s: %s should be split", sd.Name, relPath)
			totalSplitParts += len(info.ChunkKeys)
		}
	}
	t.Logf("backup-1: %d total split-file parts across %d shards", totalSplitParts, len(shardNames))

	// ========== Between backup-1 and backup-2 ==========
	// Per shard: modify seg-001, seg-003 (different size), add seg-009.
	for _, name := range shardNames {
		env.writeFile(name+"/seg-001.db", bytes.Repeat([]byte("X"), 850))
		env.writeFile(name+"/seg-003.db", bytes.Repeat([]byte("Y"), 1400))
		env.writeFile(name+"/seg-009.db", bytes.Repeat([]byte("N"), 700))
	}

	// Build all-files lists (original + new file).
	allFiles := make([][]string, len(shardNames))
	for i, specs := range shardSpecs {
		allFiles[i] = append(relPaths(specs), shardNames[i]+"/seg-009.db")
	}

	// Incremental dedup for backup-2.
	incrDescs2 := make([]*backup.ShardDescriptor, len(shardNames))
	for i := range shardNames {
		sd := env.makeShardDesc(shardNames[i], nil)
		require.NoError(t, sd.FillFileInfo(allFiles[i], []backup.ShardAndID{
			{ShardDesc: baseDescs[i], BackupID: "backup-1"},
		}, env.sourceDir))
		incrDescs2[i] = sd
	}

	totalNew2, totalSkipped2 := 0, 0
	for _, sd := range incrDescs2 {
		totalNew2 += len(sd.Files)
		totalSkipped2 += sd.IncrementalBackupInfo.NumFilesSkipped
	}
	t.Logf("backup-2 dedup: %d new files, %d skipped", totalNew2, totalSkipped2)

	env.backup("backup-2", incrDescs2)

	// ========== Between backup-2 and backup-3 ==========
	// Per shard: modify seg-002, seg-005 (unchanged since backup-1), add seg-010.
	for _, name := range shardNames {
		env.writeFile(name+"/seg-002.db", bytes.Repeat([]byte("Z"), 650))
		env.writeFile(name+"/seg-005.db", bytes.Repeat([]byte("W"), 1600))
		env.writeFile(name+"/seg-010.db", bytes.Repeat([]byte("Q"), 900))
	}

	for i := range shardNames {
		allFiles[i] = append(allFiles[i], shardNames[i]+"/seg-010.db")
	}

	// For backup-3, base references include BOTH backup-2 AND backup-1 (the full chain).
	incrDescs3 := make([]*backup.ShardDescriptor, len(shardNames))
	for i := range shardNames {
		sd := env.makeShardDesc(shardNames[i], nil)
		require.NoError(t, sd.FillFileInfo(allFiles[i], []backup.ShardAndID{
			{ShardDesc: incrDescs2[i], BackupID: "backup-2"},
			{ShardDesc: baseDescs[i], BackupID: "backup-1"},
		}, env.sourceDir))
		incrDescs3[i] = sd
	}

	totalNew3, totalSkipped3 := 0, 0
	for _, sd := range incrDescs3 {
		totalNew3 += len(sd.Files)
		totalSkipped3 += sd.IncrementalBackupInfo.NumFilesSkipped
	}
	t.Logf("backup-3 dedup: %d new files, %d skipped", totalNew3, totalSkipped3)

	// Verify the chain references both backup-1 and backup-2.
	allFilesPerBackup := make(map[string]int)
	for _, sd := range incrDescs3 {
		for backupID, infos := range sd.IncrementalBackupInfo.FilesPerBackup {
			allFilesPerBackup[backupID] += len(infos)
		}
	}
	require.Contains(t, allFilesPerBackup, "backup-1", "restore must reference backup-1")
	require.Contains(t, allFilesPerBackup, "backup-2", "restore must reference backup-2")
	t.Logf("backup-3 references: backup-1=%d files, backup-2=%d files",
		allFilesPerBackup["backup-1"], allFilesPerBackup["backup-2"])

	incrChunks3 := env.backup("backup-3", incrDescs3)
	t.Logf("backup-3: %d incremental chunks", len(incrChunks3))
	t.Logf("total chunks in store: %d", len(env.chunkStore))

	// ========== Restore from backup-3 ==========
	// Concurrently fetches split-file chunks from all three backups:
	//   - backup-3: modified seg-002, seg-005, new seg-010, small files
	//   - backup-2 via ReadFromOtherBackup: modified seg-001, seg-003, new seg-009
	//   - backup-1 via ReadFromOtherBackup: unchanged seg-004, seg-006, seg-007, seg-008
	restoredDir := env.restore("backup-3", &backup.ClassDescriptor{
		Name:   env.className,
		Chunks: incrChunks3,
		Shards: incrDescs3,
	})

	// Verify every file across all shards matches the current live state.
	for i := range shardNames {
		env.verify(restoredDir, allFiles[i])
	}
}

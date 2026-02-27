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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
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
			result := calculateTop100Size(tc.fileSizes, tc.numSkippedFiles)
			assert.Equal(t, tc.expected, result)
		})
	}
}

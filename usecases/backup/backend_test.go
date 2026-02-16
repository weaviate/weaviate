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

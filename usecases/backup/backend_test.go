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
		Name:              "test-shard",
		Node:              "test-node",
		Files:             testFiles,
		DocIDCounter:      []byte("docid-data"),
		PropLengthTracker: []byte("prop-data"),
		Version:           []byte("version-data"),
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

	// Expected size: sum of file sizes + metadata sizes
	expectedSize := int64(0)
	for _, size := range fileSizes {
		expectedSize += size
	}
	expectedSize += int64(len(shard.DocIDCounter))
	expectedSize += int64(len(shard.PropLengthTracker))
	expectedSize += int64(len(shard.Version))

	assert.Equal(t, expectedSize, preCompressionSize)
}

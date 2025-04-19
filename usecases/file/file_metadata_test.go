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

package file

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetFileMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := GetFileMetadata(tmpDir)
	require.ErrorIs(t, err, syscall.EISDIR)

	_, err = GetFileMetadata("non-existing-file")
	require.ErrorIs(t, err, os.ErrNotExist)

	tmpFilePath := filepath.Join(tmpDir, "testfile1.txt")
	tmpFileContent := []byte("hello, world")

	err = os.WriteFile(tmpFilePath, tmpFileContent, 0o644)
	require.NoError(t, err)

	md, err := GetFileMetadata(tmpFilePath)
	require.NoError(t, err)
	require.EqualValues(t, len(tmpFileContent), md.Size)
	require.Equal(t, crc32.ChecksumIEEE(tmpFileContent), md.CRC32)
}

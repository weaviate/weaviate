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

//go:build darwin || linux

package mmap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMmapFile(t *testing.T) {
	var (
		tmpDir   = t.TempDir()
		filePath = filepath.Join(tmpDir, "mmap.txt")

		data = generateData(10 * 1024 * 1024) // 10 MB
	)

	require.NoError(t, os.WriteFile(filePath, data, 0o600))

	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer file.Close()

	fileInfo, err := file.Stat()
	require.NoError(t, err)

	mmapedData, err := MapRegion(file, int(fileInfo.Size()), RDONLY, 0, 0)
	require.NoError(t, err)

	require.Equal(t, data, []byte(mmapedData))

	require.NoError(t, mmapedData.Unmap())
}

func generateData(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

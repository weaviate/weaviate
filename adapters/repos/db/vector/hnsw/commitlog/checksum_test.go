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

package commitlog

import (
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChecksumLoggerAndReader(t *testing.T) {
	content := []string{
		"line1",
		"line2",
		"line3",
		"line4",
		"line5",
	}

	checksumPath := path.Join(t.TempDir(), "checksumfile.checksum")

	checksumLogger, err := OpenCRC32ChecksumLogger(checksumPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	require.NoError(t, err)

	for _, l := range content {
		n, err := checksumLogger.Write([]byte(l))
		require.NoError(t, err)
		require.Equal(t, len(l), n)

		_, err = checksumLogger.Checksum(nil)
		require.NoError(t, err)

		checksumLogger.Reset()
	}

	err = checksumLogger.Close()
	require.NoError(t, err)

	checksumFile, err := os.Open(checksumPath)
	require.NoError(t, err)

	defer checksumFile.Close()

	checksumReader := NewCRC32ChecksumReader(checksumFile)

	for _, l := range content {
		hash := crc32.NewIEEE()

		checksum, err := checksumReader.NextHash()
		require.NoError(t, err)

		_, err = hash.Write([]byte(l))
		require.NoError(t, err)

		require.EqualValues(t, hash.Sum(nil), checksum)
	}
}

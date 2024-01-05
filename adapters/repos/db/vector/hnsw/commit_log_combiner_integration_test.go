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

package hnsw

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CommitlogCombiner(t *testing.T) {
	// For the combiner the contents of a commit log file don't actually matter
	// so we can put arbitrary data in the files. It will only make decisions
	// about what should be appended, the actual condensing will be taken care of
	// by the condensor

	rootPath := t.TempDir()
	logger, _ := test.NewNullLogger()

	threshold := int64(1000)
	id := "combiner_test"
	// create commit logger directory
	require.Nil(t, os.MkdirAll(commitLogDirectory(rootPath, id), 0o777))

	name := func(fileName string) string {
		return commitLogFileName(rootPath, id, fileName)
	}

	t.Run("create several condensed files below the threshold", func(t *testing.T) {
		// 4 files of 300 bytes each, with 1000 byte threshold. This lets us verify
		// that one and two will be combined, so will three and four.
		require.Nil(t, createDummyFile(name("1000.condensed"), []byte("file1\n"), 300))
		require.Nil(t, createDummyFile(name("1001.condensed"), []byte("file2\n"), 300))
		require.Nil(t, createDummyFile(name("1002.condensed"), []byte("file3\n"), 300))
		require.Nil(t, createDummyFile(name("1003.condensed"), []byte("file4\n"), 300))
		require.Nil(t, createDummyFile(name("1004"), []byte("current\n"), 50))
	})

	t.Run("run combiner", func(t *testing.T) {
		_, err := NewCommitLogCombiner(rootPath, id, threshold, logger).Do()
		require.Nil(t, err)
	})

	t.Run("we are now left with combined files", func(t *testing.T) {
		dir, err := os.Open(commitLogDirectory(rootPath, id))
		require.Nil(t, err)

		fileNames, err := dir.Readdirnames(0)
		require.Nil(t, err)
		require.Len(t, fileNames, 3)
		require.ElementsMatch(t, []string{"1000", "1002", "1004"}, fileNames)

		t.Run("the first file is correctly combined", func(t *testing.T) {
			contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1000"))
			require.Nil(t, err)
			require.Len(t, contents, 600)
			assert.Equal(t, contents[0:6], []byte("file1\n"))
			assert.Equal(t, contents[300:306], []byte("file2\n"))
		})

		t.Run("the second file is correctly combined", func(t *testing.T) {
			contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1002"))
			require.Nil(t, err)
			require.Len(t, contents, 600)
			assert.Equal(t, contents[0:6], []byte("file3\n"))
			assert.Equal(t, contents[300:306], []byte("file4\n"))
		})

		t.Run("latest file is unchanged", func(t *testing.T) {
			contents, err := os.ReadFile(commitLogFileName(rootPath, id, "1004"))
			require.Nil(t, err)
			require.Len(t, contents, 50)
			assert.Equal(t, contents[0:8], []byte("current\n"))
			assert.Equal(t, contents[42:], []byte("rrent\ncu"))
		})
	})
}

func createDummyFile(fileName string, content []byte, size int) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}

	defer f.Close()

	written := 0
	for {
		if size == written {
			break
		}

		if size-written < len(content) {
			content = content[:(size - written)]
		}

		n, err := f.Write([]byte(content))
		written += n

		if err != nil {
			return err
		}
	}

	return nil
}

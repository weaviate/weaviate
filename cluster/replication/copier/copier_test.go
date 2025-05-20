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

package copier_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/integrity"
)

func TestCopierCopyReplicaFiles(t *testing.T) {
	remoteTmpDir, err := os.MkdirTemp("", "remote-*")
	require.NoError(t, err)
	defer os.RemoveAll(remoteTmpDir)

	localTmpDir, err := os.MkdirTemp("", "local-*")
	require.NoError(t, err)
	defer os.RemoveAll(localTmpDir)

	type filesToCreateBeforeCopy struct {
		relativeFilePath string
		fileContent      []byte
	}

	type fileWithMetadata struct {
		absoluteFilePath string
		relativeFilePath string
		fileContent      []byte
		crc32            uint32
	}

	createTestFiles := func(t *testing.T, basePath string, files []filesToCreateBeforeCopy) []fileWithMetadata {
		createdFiles := []fileWithMetadata{}
		for _, file := range files {
			absolutePath := filepath.Join(basePath, file.relativeFilePath)
			require.NoError(t, os.WriteFile(absolutePath, file.fileContent, 0o644))
			_, fileCrc32, err := integrity.CRC32(absolutePath)
			require.NoError(t, err)
			createdFiles = append(createdFiles, fileWithMetadata{
				absoluteFilePath: absolutePath,
				relativeFilePath: file.relativeFilePath,
				fileContent:      file.fileContent,
				crc32:            fileCrc32,
			})
		}
		return createdFiles
	}
	type testCase struct {
		name              string
		localFilesBefore  []fileWithMetadata
		remoteFilesToSync []fileWithMetadata
	}
	for _, tc := range []testCase{
		{
			name: "ensure unexpected local files are deleted",
			localFilesBefore: createTestFiles(t, localTmpDir, []filesToCreateBeforeCopy{
				{relativeFilePath: "file2", fileContent: []byte("bar")},
			}),
			remoteFilesToSync: createTestFiles(t, remoteTmpDir, []filesToCreateBeforeCopy{
				{relativeFilePath: "file1", fileContent: []byte("foo")},
			}),
		},
		{
			name: "an existing local file with the same path as a remote file is overwritten",
			localFilesBefore: createTestFiles(t, localTmpDir, []filesToCreateBeforeCopy{
				{relativeFilePath: "file1", fileContent: []byte("bar")},
			}),
			remoteFilesToSync: createTestFiles(t, remoteTmpDir, []filesToCreateBeforeCopy{
				{relativeFilePath: "file1", fileContent: []byte("foo")},
			}),
		},
	} {
		mockRemoteIndex := types.NewMockRemoteIndex(t)
		mockRemoteIndex.EXPECT().PauseFileActivity(mock.Anything, "node1", "collection", "shard", uint64(0)).Return(nil)
		mockRemoteIndex.EXPECT().ResumeFileActivity(mock.Anything, "node1", "collection", "shard").Return(nil)
		remoteFileRelativePaths := []string{}
		for _, remoteFilePath := range tc.remoteFilesToSync {
			fi, err := os.Stat(remoteFilePath.absoluteFilePath)
			require.NoError(t, err)
			_, fileCrc32, err := integrity.CRC32(remoteFilePath.absoluteFilePath)
			require.NoError(t, err)
			fileMetadata := file.FileMetadata{
				Name:  remoteFilePath.relativeFilePath,
				Size:  fi.Size(),
				CRC32: fileCrc32,
			}
			mockRemoteIndex.EXPECT().GetFileMetadata(
				mock.Anything,
				"node1",
				"collection",
				"shard",
				remoteFilePath.relativeFilePath,
			).Return(fileMetadata, nil)
			remoteFileRelativePaths = append(remoteFileRelativePaths, remoteFilePath.relativeFilePath)
			mockRemoteIndex.EXPECT().GetFile(
				mock.Anything,
				"node1",
				"collection",
				"shard",
				remoteFilePath.relativeFilePath,
			).Return(io.NopCloser(bytes.NewReader(remoteFilePath.fileContent)), nil)
		}
		mockRemoteIndex.EXPECT().ListFiles(mock.Anything, "node1", "collection", "shard").Return(remoteFileRelativePaths, nil)

		fakeNodeSelector := fakes.NewFakeClusterState("node1")
		logger, _ := logrusTest.NewNullLogger()
		copier := copier.New(mockRemoteIndex, fakeNodeSelector, localTmpDir, nil, logger)
		err = copier.CopyReplicaFiles(t.Context(), "node1", "collection", "shard", 0)
		require.NoError(t, err)

		remoteFilesRelativePathLookup := map[string]struct{}{}
		for _, remoteFile := range tc.remoteFilesToSync {
			newLocalFilePath := filepath.Join(localTmpDir, remoteFile.relativeFilePath)
			_, err := os.Stat(newLocalFilePath)
			require.NoError(t, err)
			// assert the content of the synced local/remote files match
			remoteFileContent, err := os.ReadFile(remoteFile.absoluteFilePath)
			require.NoError(t, err)
			finalLocalFileContent, err := os.ReadFile(newLocalFilePath)
			require.NoError(t, err)
			require.Equal(t, remoteFileContent, finalLocalFileContent)
			remoteFilesRelativePathLookup[remoteFile.relativeFilePath] = struct{}{}
		}

		// verify that the unexpected local files from before were deleted
		for _, localFile := range tc.localFilesBefore {
			// if the file exists on the remote, it should not be deleted
			if _, ok := remoteFilesRelativePathLookup[localFile.relativeFilePath]; ok {
				continue
			}
			_, err := os.Stat(localFile.absoluteFilePath)
			require.Error(t, err)
			require.ErrorIs(t, err, os.ErrNotExist)
		}

	}
}

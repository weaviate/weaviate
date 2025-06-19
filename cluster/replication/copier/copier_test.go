//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
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
		isDir            bool
	}

	type fileWithMetadata struct {
		absoluteFilePath string
		relativeFilePath string
		fileContent      []byte
		crc32            uint32
		isDir            bool
	}

	createTestFiles := func(t *testing.T, basePath string, files []filesToCreateBeforeCopy) []fileWithMetadata {
		createdFiles := []fileWithMetadata{}
		for _, file := range files {
			absolutePath := filepath.Join(basePath, file.relativeFilePath)

			dir := path.Dir(absolutePath)
			require.NoError(t, os.MkdirAll(dir, os.ModePerm))

			var fileCrc32 uint32
			if file.isDir {
				require.NoError(t, os.Mkdir(absolutePath, 0o755))
			} else {
				require.NoError(t, os.WriteFile(absolutePath, file.fileContent, 0o644))
				_, fileCrc32, err = integrity.CRC32(absolutePath)
				require.NoError(t, err)
			}
			createdFiles = append(createdFiles, fileWithMetadata{
				absoluteFilePath: absolutePath,
				relativeFilePath: file.relativeFilePath,
				fileContent:      file.fileContent,
				crc32:            fileCrc32,
				isDir:            file.isDir,
			})
		}
		return createdFiles
	}
	type testCase struct {
		name              string
		localFilesBefore  func() []fileWithMetadata
		remoteFilesToSync func() []fileWithMetadata
	}
	for _, tc := range []testCase{
		{
			name: "ensure unexpected local files are deleted",
			localFilesBefore: func() []fileWithMetadata {
				return createTestFiles(t, localTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/file2", fileContent: []byte("bar")},
				})
			},
			remoteFilesToSync: func() []fileWithMetadata {
				return createTestFiles(t, remoteTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/file1", fileContent: []byte("foo")},
				})
			},
		},
		{
			name: "an existing local file with the same path as a remote file is overwritten",
			localFilesBefore: func() []fileWithMetadata {
				return createTestFiles(t, localTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/file1", fileContent: []byte("bar")},
				})
			},
			remoteFilesToSync: func() []fileWithMetadata {
				return createTestFiles(t, remoteTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/file1", fileContent: []byte("foo")},
				})
			},
		},
		{
			name: "ensure nested empty local directories are deleted",
			localFilesBefore: func() []fileWithMetadata {
				return createTestFiles(t, localTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/dir1/file2", fileContent: []byte("bar")},
					{relativeFilePath: "collection/shard/dir2/", isDir: true},
					{relativeFilePath: "collection/shard/dir2/dir3/", isDir: true},
				})
			},
			remoteFilesToSync: func() []fileWithMetadata {
				return createTestFiles(t, remoteTmpDir, []filesToCreateBeforeCopy{
					{relativeFilePath: "collection/shard/dir1/file1", fileContent: []byte("foo")},
				})
			},
		},
	} {
		localFilesBefore := tc.localFilesBefore()
		remoteFilesToSync := tc.remoteFilesToSync()

		mockRemoteIndex := types.NewMockRemoteIndex(t)

		mockClient := NewMockFileReplicationServiceClient(t)

		mockClient.EXPECT().PauseFileActivity(
			mock.Anything,
			mock.MatchedBy(func(req *pbv1.PauseFileActivityRequest) bool {
				return req.IndexName == "collection" &&
					req.ShardName == "shard" &&
					req.SchemaVersion == uint64(0)
			}),
		).Return(&pbv1.PauseFileActivityResponse{}, nil)

		mockClient.EXPECT().ResumeFileActivity(
			mock.Anything,
			mock.MatchedBy(func(req *pbv1.ResumeFileActivityRequest) bool {
				return req.IndexName == "collection" && req.ShardName == "shard"
			}),
		).Return(&pbv1.ResumeFileActivityResponse{}, nil)

		mockClient.EXPECT().Close().Return(nil)

		remoteFileRelativePaths := []string{}

		mockBidirectionalFileMetadataStream := NewMockFileMetadataStream(t)

		mockBidirectionalFileMetadataStream.EXPECT().
			CloseSend().
			Return(nil)

		mockBidirectionalFileChunkStream := NewMockFileChunkStream(t)

		mockBidirectionalFileChunkStream.EXPECT().
			CloseSend().
			Return(nil)

		for _, remoteFilePath := range remoteFilesToSync {
			fi, err := os.Stat(remoteFilePath.absoluteFilePath)
			require.NoError(t, err)

			_, fileCrc32, err := integrity.CRC32(remoteFilePath.absoluteFilePath)
			require.NoError(t, err)

			fileMetadata := file.FileMetadata{
				Name:  remoteFilePath.relativeFilePath,
				Size:  fi.Size(),
				CRC32: fileCrc32,
			}

			mockBidirectionalFileMetadataStream.EXPECT().
				Send(&pbv1.GetFileMetadataRequest{
					IndexName: "collection",
					ShardName: "shard",
					FileName:  remoteFilePath.relativeFilePath,
				}).Return(nil)

			mockBidirectionalFileMetadataStream.EXPECT().
				Recv().
				Return(&pbv1.FileMetadata{
					IndexName: "collection",
					ShardName: "shard",
					FileName:  fileMetadata.Name,
					Size:      fileMetadata.Size,
					Crc32:     fileMetadata.CRC32,
				}, nil).Times(1)

			mockBidirectionalFileChunkStream.EXPECT().
				Recv().
				Return(&pbv1.FileChunk{
					Offset: 0,
					Data:   remoteFilePath.fileContent,
					Eof:    true,
				}, nil).Times(1)

			mockBidirectionalFileChunkStream.EXPECT().
				Send(&pbv1.GetFileRequest{
					IndexName: "collection",
					ShardName: "shard",
					FileName:  remoteFilePath.relativeFilePath,
				}).Return(nil)

			remoteFileRelativePaths = append(remoteFileRelativePaths, remoteFilePath.relativeFilePath)
		}

		mockBidirectionalFileMetadataStream.EXPECT().
			Recv().
			Return(nil, io.EOF)

		mockBidirectionalFileChunkStream.EXPECT().
			Recv().
			Return(nil, io.EOF)

		mockClient.EXPECT().GetFileMetadata(
			mock.Anything,
		).Return(mockBidirectionalFileMetadataStream, nil)

		mockClient.EXPECT().GetFile(
			mock.Anything,
		).Return(mockBidirectionalFileChunkStream, nil)

		mockClient.EXPECT().ListFiles(
			mock.Anything,
			mock.MatchedBy(func(req *pbv1.ListFilesRequest) bool {
				return req.IndexName == "collection" && req.ShardName == "shard"
			}),
		).Return(&pbv1.ListFilesResponse{
			FileNames: remoteFileRelativePaths,
		}, nil)

		mockClientFactory := func(ctx context.Context, address string) (FileReplicationServiceClient, error) {
			return mockClient, nil
		}

		fakeNodeSelector := fakes.NewFakeClusterState("node1")

		logger, _ := logrusTest.NewNullLogger()

		copier := New(mockClientFactory, mockRemoteIndex, fakeNodeSelector, 1, localTmpDir, nil, logger)
		err = copier.CopyReplicaFiles(t.Context(), "node1", "collection", "shard", 0)
		require.NoError(t, err)

		remoteFilesRelativePathLookup := map[string]struct{}{}
		for _, remoteFile := range remoteFilesToSync {
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
		for _, localFile := range localFilesBefore {
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

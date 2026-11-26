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

package copier_test

import (
	"context"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/usecases/fakes"
)

// The source node picks the file names. A name must be a relative path to a
// file inside the shard directory. A name that puts the file, or its ".tmp"
// file, outside the shard directory is refused before any file is fetched or
// written. So is an absolute name, even though filepath.Join would keep it
// inside. So is a name that leaves the shard directory and re-enters it.
func TestDownloadWorkerFileName(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		// existing is a file, relative to the temp dir, that already holds the
		// source's bytes. It matches the checksum the source sends. A worker
		// that reads it before checking the name skips the file instead of
		// refusing the name.
		existing string
		// shardFile is an honest file the source also lists. It is already in
		// the shard directory with the source's bytes. So the shard directory
		// exists and holds only listed files. A worker that skips the refused
		// name instead of refusing it then makes the copy succeed.
		shardFile string
	}{
		{name: "parent traversal escapes the data root", fileName: "../../../escaped.txt"},
		{
			name:      "parent traversal to a file with the same checksum",
			fileName:  "../../../escaped.txt",
			existing:  "escaped.txt",
			shardFile: "fileA",
		},
		{name: "dot names the shard directory itself", fileName: "."},
		{name: "empty name names the shard directory itself", fileName: ""},
		{name: "traversal back into the shard directory", fileName: "../shard"},
		{name: "traversal that leaves and re-enters the shard directory", fileName: "../shard/fileA"},
		{name: "nested name that leaves and re-enters the shard directory", fileName: "nested/../../shard/fileA"},
		{name: "traversal that cleans to the collection directory", fileName: "nested/../.."},
		{name: "traversal to a sibling whose path starts with the shard's path", fileName: "../shardX/escaped.txt"},
		{name: "absolute path, though joining keeps it inside the shard directory", fileName: "/abs.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The data root is one level below the temp dir. A file that escapes
			// the data root then still lands in the tree this test walks.
			tmpDir := t.TempDir()
			dataRoot := filepath.Join(tmpDir, "data")
			require.NoError(t, os.Mkdir(dataRoot, os.ModePerm))

			data := []byte("hostile")
			wantPaths := []string{tmpDir, dataRoot}
			if tt.existing != "" {
				existing := filepath.Join(tmpDir, tt.existing)
				require.NoError(t, os.WriteFile(existing, data, 0o644))
				wantPaths = append(wantPaths, existing)
			}
			fileNames := []string{tt.fileName}
			if tt.shardFile != "" {
				shardDir := filepath.Join(dataRoot, "collection", "shard")
				require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))
				shardFile := filepath.Join(shardDir, tt.shardFile)
				require.NoError(t, os.WriteFile(shardFile, data, 0o644))
				wantPaths = append(wantPaths, filepath.Dir(shardDir), shardDir, shardFile)
				fileNames = []string{tt.shardFile, tt.fileName}
			}

			mockClient := copier.NewMockFileReplicationServiceClient(t)
			mockClient.EXPECT().CreateReplicaSnapshot(mock.Anything, mock.Anything).
				Return(&protocol.CreateReplicaSnapshotResponse{FileNames: fileNames}, nil)
			mockClient.EXPECT().ReleaseReplicaSnapshot(mock.Anything, mock.Anything).
				Return(&protocol.ReleaseReplicaSnapshotResponse{}, nil)
			mockClient.EXPECT().GetReplicaSnapshotFileMetadata(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, req *protocol.GetReplicaSnapshotFileMetadataRequest,
					_ ...grpc.CallOption,
				) (*protocol.FileMetadata, error) {
					return &protocol.FileMetadata{
						IndexName: "collection",
						FileName:  req.FileName,
						Size:      int64(len(data)),
						Crc32:     crc32.ChecksumIEEE(data),
					}, nil
				})

			// These mocks answer if called. Without the name check, the test
			// then fails on the file written to disk, not on an unexpected mock
			// call inside a worker goroutine.
			stream := copier.NewMockFileChunkStream(t)
			stream.EXPECT().Recv().Return(&protocol.FileChunk{Data: data, Eof: true}, nil).Maybe()
			mockClient.EXPECT().GetReplicaSnapshotFile(mock.Anything, mock.Anything).Return(stream, nil).Maybe()

			logger, _ := logrusTest.NewNullLogger()
			c := copier.New(
				func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
					return mockClient, nil
				},
				types.NewMockRemoteIndex(t),
				fakes.NewFakeClusterState("node1"),
				1,
				dataRoot,
				nil,
				"node1",
				logger,
			)

			err := c.CopyReplicaFiles(context.Background(), strfmt.UUID("33333333-3333-3333-3333-333333333333"),
				"node1", "collection", "shard", 0)

			var gotPaths []string
			require.NoError(t, filepath.WalkDir(tmpDir, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				gotPaths = append(gotPaths, p)
				return nil
			}))
			require.ElementsMatch(t, wantPaths, gotPaths, "no file or directory may be created for a refused name")
			mockClient.AssertNotCalled(t, "GetReplicaSnapshotFile", mock.Anything, mock.Anything)
			require.ErrorContains(t, err, "not a path inside the shard directory")
		})
	}
}

// A name whose first element only begins with ".." is a name inside the shard
// directory, not a step out of it.
func TestDownloadWorkerAcceptedFileName(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		// wantPaths lists every path under the temp dir after the copy, relative
		// to the temp dir. The temp dir itself is not listed. The last entry is
		// the copied file.
		wantPaths []string
	}{
		{
			name:      "top-level name that begins with dot-dot",
			fileName:  "..meta",
			wantPaths: []string{"data", "data/collection", "data/collection/shard", "data/collection/shard/..meta"},
		},
		{
			name:      "top-level name of three dots",
			fileName:  "...",
			wantPaths: []string{"data", "data/collection", "data/collection/shard", "data/collection/shard/..."},
		},
		{
			name:     "nested under a directory that begins with dot-dot",
			fileName: filepath.Join("..a", "b"),
			wantPaths: []string{
				"data", "data/collection", "data/collection/shard",
				"data/collection/shard/..a", "data/collection/shard/..a/b",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The data root is one level below the temp dir, so a file written
			// above it still lands in the tree this test walks.
			tmpDir := t.TempDir()
			dataRoot := filepath.Join(tmpDir, "data")
			require.NoError(t, os.Mkdir(dataRoot, os.ModePerm))

			data := []byte("served")
			mockClient := copier.NewMockFileReplicationServiceClient(t)
			mockClient.EXPECT().CreateReplicaSnapshot(mock.Anything, mock.Anything).
				Return(&protocol.CreateReplicaSnapshotResponse{FileNames: []string{tt.fileName}}, nil)
			mockClient.EXPECT().ReleaseReplicaSnapshot(mock.Anything, mock.Anything).
				Return(&protocol.ReleaseReplicaSnapshotResponse{}, nil)
			mockClient.EXPECT().GetReplicaSnapshotFileMetadata(mock.Anything, mock.Anything).
				Return(&protocol.FileMetadata{
					IndexName: "collection",
					FileName:  tt.fileName,
					Size:      int64(len(data)),
					Crc32:     crc32.ChecksumIEEE(data),
				}, nil)
			stream := copier.NewMockFileChunkStream(t)
			stream.EXPECT().Recv().Return(&protocol.FileChunk{Data: data, Eof: true}, nil)
			mockClient.EXPECT().GetReplicaSnapshotFile(mock.Anything, &protocol.GetReplicaSnapshotFileRequest{
				IndexName: "collection",
				OpId:      "44444444-4444-4444-4444-444444444444",
				FileName:  tt.fileName,
			}).Return(stream, nil)

			logger, _ := logrusTest.NewNullLogger()
			c := copier.New(
				func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
					return mockClient, nil
				},
				types.NewMockRemoteIndex(t),
				fakes.NewFakeClusterState("node1"),
				1,
				dataRoot,
				nil,
				"node1",
				logger,
			)

			err := c.CopyReplicaFiles(context.Background(), strfmt.UUID("44444444-4444-4444-4444-444444444444"),
				"node1", "collection", "shard", 0)
			require.NoError(t, err)

			wantPaths := []string{tmpDir}
			for _, p := range tt.wantPaths {
				wantPaths = append(wantPaths, filepath.Join(tmpDir, filepath.FromSlash(p)))
			}
			var gotPaths []string
			require.NoError(t, filepath.WalkDir(tmpDir, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				gotPaths = append(gotPaths, p)
				return nil
			}))
			require.ElementsMatch(t, wantPaths, gotPaths, "the tree under the temp dir must be exactly the listed paths")

			got, err := os.ReadFile(wantPaths[len(wantPaths)-1])
			require.NoError(t, err)
			require.Equal(t, data, got)
		})
	}
}

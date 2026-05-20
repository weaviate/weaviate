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

package copier

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/integrity"
)

func TestCopyReplicaFiles(t *testing.T) {
	remoteTmpDir := t.TempDir()
	localTmpDir := t.TempDir()

	write := func(dir, rel string, content []byte) string {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, content, 0o644))
		return path
	}

	// remote files
	remoteFiles := []struct {
		rel string
		buf []byte
	}{
		{"collection/shard/fileA", []byte("AAA")},
		{"collection/shard/nested/fileB", []byte("BBB")},
	}

	for _, f := range remoteFiles {
		write(remoteTmpDir, f.rel, f.buf)
	}

	// local unexpected file that must be deleted
	_ = write(localTmpDir, "collection/shard/old", []byte("OLD"))

	mockClient := NewMockFileReplicationServiceClient(t)
	mockRemoteIndex := types.NewMockRemoteIndex(t)

	// Pause / resume
	mockClient.EXPECT().PauseFileActivity(mock.Anything, mock.Anything).
		Return(&protocol.PauseFileActivityResponse{}, nil)
	mockClient.EXPECT().ResumeFileActivity(mock.Anything, mock.Anything).
		Return(&protocol.ResumeFileActivityResponse{}, nil)

	// ListFiles
	fileNames := []string{
		"collection/shard/fileA",
		"collection/shard/nested/fileB",
	}
	mockClient.EXPECT().ListFiles(mock.Anything, mock.Anything).
		Return(&protocol.ListFilesResponse{FileNames: fileNames}, nil)

	// Metadata calls
	for _, f := range remoteFiles {
		st, err := os.Stat(filepath.Join(remoteTmpDir, f.rel))
		require.NoError(t, err)

		mockClient.EXPECT().GetFileMetadata(
			mock.Anything,
			&protocol.GetFileMetadataRequest{
				IndexName: "collection",
				ShardName: "shard",
				FileName:  f.rel,
			},
		).Return(&protocol.FileMetadata{
			IndexName: "collection",
			ShardName: "shard",
			FileName:  f.rel,
			Size:      st.Size(),
			Crc32:     checksum(filepath.Join(remoteTmpDir, f.rel), t),
		}, nil)
	}

	// File download streams
	for _, f := range remoteFiles {
		stream := NewMockFileChunkStream(t)

		stream.EXPECT().Recv().Return(&protocol.FileChunk{
			Data: []byte(f.buf),
			Eof:  true,
		}, nil).Once()

		stream.EXPECT().Recv().Return(nil, io.EOF)

		mockClient.EXPECT().GetFile(
			mock.Anything,
			&protocol.GetFileRequest{
				IndexName: "collection",
				ShardName: "shard",
				FileName:  f.rel,
			},
		).Return(stream, nil)
	}

	fakeSelector := fakes.NewFakeClusterState("node1")

	logger, _ := logrusTest.NewNullLogger()

	c := New(
		func(ctx context.Context, addr string) (FileReplicationServiceClient, error) { return mockClient, nil },
		mockRemoteIndex,
		fakeSelector,
		2,
		localTmpDir,
		nil,
		"node1",
		logger,
	)

	err := c.CopyReplicaFiles(context.Background(), "node1", "collection", "shard", 0)
	require.NoError(t, err)

	// Validation: remote files exist locally
	for _, f := range remoteFiles {
		path := filepath.Join(localTmpDir, f.rel)
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, f.buf, b)
	}

	// Validation: unexpected local file is removed
	_, err = os.Stat(filepath.Join(localTmpDir, "collection/shard/old"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// TestCopyReplicaFilesToLocalShard_RecoveryOverride mirrors
// TestCopyReplicaFiles but supplies a localShardOverride (as SELF_RECOVERY
// does, "<shard>.recovering"): the source still reports files under
// "<collection>/<shard>/...", but they must land locally under
// "<collection>/<shard>.recovering/..." — and the live "<collection>/<shard>/"
// directory must be left untouched.
func TestCopyReplicaFilesToLocalShard_RecoveryOverride(t *testing.T) {
	remoteTmpDir := t.TempDir()
	localTmpDir := t.TempDir()

	write := func(dir, rel string, content []byte) string {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, content, 0o644))
		return path
	}

	remoteFiles := []struct {
		rel string
		buf []byte
	}{
		{"collection/shard/fileA", []byte("AAA")},
		{"collection/shard/nested/fileB", []byte("BBB")},
	}
	for _, f := range remoteFiles {
		write(remoteTmpDir, f.rel, f.buf)
	}

	// A stale file in the recovery dir that must be cleaned up; plus a
	// file in the *live* dir that must NOT be touched.
	_ = write(localTmpDir, "collection/shard.recovering/old", []byte("OLD"))
	_ = write(localTmpDir, "collection/shard/live-marker", []byte("LIVE"))

	mockClient := NewMockFileReplicationServiceClient(t)
	mockRemoteIndex := types.NewMockRemoteIndex(t)

	mockClient.EXPECT().PauseFileActivity(mock.Anything, mock.Anything).
		Return(&protocol.PauseFileActivityResponse{}, nil)
	mockClient.EXPECT().ResumeFileActivity(mock.Anything, mock.Anything).
		Return(&protocol.ResumeFileActivityResponse{}, nil)

	fileNames := []string{
		"collection/shard/fileA",
		"collection/shard/nested/fileB",
	}
	mockClient.EXPECT().ListFiles(mock.Anything, mock.Anything).
		Return(&protocol.ListFilesResponse{FileNames: fileNames}, nil)

	for _, f := range remoteFiles {
		st, err := os.Stat(filepath.Join(remoteTmpDir, f.rel))
		require.NoError(t, err)
		mockClient.EXPECT().GetFileMetadata(
			mock.Anything,
			&protocol.GetFileMetadataRequest{IndexName: "collection", ShardName: "shard", FileName: f.rel},
		).Return(&protocol.FileMetadata{
			IndexName: "collection", ShardName: "shard", FileName: f.rel,
			Size:  st.Size(),
			Crc32: checksum(filepath.Join(remoteTmpDir, f.rel), t),
		}, nil)
	}
	for _, f := range remoteFiles {
		stream := NewMockFileChunkStream(t)
		stream.EXPECT().Recv().Return(&protocol.FileChunk{Data: []byte(f.buf), Eof: true}, nil).Once()
		stream.EXPECT().Recv().Return(nil, io.EOF)
		mockClient.EXPECT().GetFile(
			mock.Anything,
			&protocol.GetFileRequest{IndexName: "collection", ShardName: "shard", FileName: f.rel},
		).Return(stream, nil)
	}

	logger, _ := logrusTest.NewNullLogger()
	c := New(
		func(ctx context.Context, addr string) (FileReplicationServiceClient, error) { return mockClient, nil },
		mockRemoteIndex,
		fakes.NewFakeClusterState("node1"),
		2,
		localTmpDir,
		nil,
		"node1",
		logger,
	)

	require.NoError(t, c.CopyReplicaFilesToLocalShard(context.Background(), "node1", "collection", "shard", "shard.recovering", 0))

	// Files landed in the .recovering sibling, not the live dir.
	for _, f := range remoteFiles {
		relWithinShard := f.rel[len("collection/shard/"):] // e.g. "fileA", "nested/fileB"
		b, err := os.ReadFile(filepath.Join(localTmpDir, "collection", "shard.recovering", relWithinShard))
		require.NoError(t, err, "expected collection/shard.recovering/%s", relWithinShard)
		require.Equal(t, f.buf, b)
		_, err = os.Stat(filepath.Join(localTmpDir, "collection", "shard", relWithinShard))
		require.ErrorIs(t, err, os.ErrNotExist, "copied file must not appear under the live shard dir")
	}
	// Stale file in the recovery dir was cleaned up.
	_, err := os.Stat(filepath.Join(localTmpDir, "collection/shard.recovering/old"))
	require.ErrorIs(t, err, os.ErrNotExist)
	// The live shard dir was left completely untouched.
	b, err := os.ReadFile(filepath.Join(localTmpDir, "collection/shard/live-marker"))
	require.NoError(t, err)
	require.Equal(t, []byte("LIVE"), b)
}

// helper
func checksum(p string, t *testing.T) uint32 {
	_, c, err := integrity.CRC32(p)
	require.NoError(t, err)
	return c
}

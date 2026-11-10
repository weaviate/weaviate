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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/copier"
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

	// remote files (shard-relative paths)
	remoteFiles := []struct {
		rel string
		buf []byte
	}{
		{"fileA", []byte("AAA")},
		{"nested/fileB", []byte("BBB")},
	}

	remoteShardDir := filepath.Join(remoteTmpDir, "collection", "shard")
	for _, f := range remoteFiles {
		write(remoteShardDir, f.rel, f.buf)
	}

	_ = write(localTmpDir, "collection/shard/old", []byte("OLD"))

	mockClient := copier.NewMockFileReplicationServiceClient(t)
	mockRemoteIndex := types.NewMockRemoteIndex(t)

	opID := strfmt.UUID("11111111-1111-1111-1111-111111111111")
	fileNames := []string{"fileA", "nested/fileB"}

	// Create / Release replica snapshot
	mockClient.EXPECT().CreateReplicaSnapshot(mock.Anything, mock.Anything).
		Return(&protocol.CreateReplicaSnapshotResponse{FileNames: fileNames}, nil)
	mockClient.EXPECT().ReleaseReplicaSnapshot(mock.Anything, mock.Anything).
		Return(&protocol.ReleaseReplicaSnapshotResponse{}, nil)

	for _, f := range remoteFiles {
		st, err := os.Stat(filepath.Join(remoteShardDir, f.rel))
		require.NoError(t, err)

		mockClient.EXPECT().GetReplicaSnapshotFileMetadata(
			mock.Anything,
			&protocol.GetReplicaSnapshotFileMetadataRequest{
				IndexName: "collection",
				OpId:      string(opID),
				FileName:  f.rel,
			},
		).Return(&protocol.FileMetadata{
			IndexName: "collection",
			FileName:  f.rel,
			Size:      st.Size(),
			Crc32:     checksum(filepath.Join(remoteShardDir, f.rel), t),
		}, nil)
	}

	for _, f := range remoteFiles {
		stream := copier.NewMockFileChunkStream(t)

		stream.EXPECT().Recv().Return(&protocol.FileChunk{
			Data: []byte(f.buf),
			Eof:  true,
		}, nil).Once()

		stream.EXPECT().Recv().Return(nil, io.EOF)

		mockClient.EXPECT().GetReplicaSnapshotFile(
			mock.Anything,
			&protocol.GetReplicaSnapshotFileRequest{
				IndexName: "collection",
				OpId:      string(opID),
				FileName:  f.rel,
			},
		).Return(stream, nil)
	}

	fakeSelector := fakes.NewFakeClusterState("node1")

	logger, _ := logrusTest.NewNullLogger()

	c := copier.New(
		func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
			return mockClient, nil
		},
		mockRemoteIndex,
		fakeSelector,
		2,
		localTmpDir,
		nil,
		"node1",
		logger,
	)

	err := c.CopyReplicaFiles(context.Background(), opID, "node1", "collection", "shard", 0)
	require.NoError(t, err)

	// Validation: remote files exist locally under <localTmpDir>/<lowerClass>/<shard>/<rel>
	localShardDir := filepath.Join(localTmpDir, "collection", "shard")
	for _, f := range remoteFiles {
		path := filepath.Join(localShardDir, f.rel)
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, f.buf, b)
	}

	// Validation: unexpected local file is removed
	_, err = os.Stat(filepath.Join(localTmpDir, "collection/shard/old"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// helper
func checksum(p string, t *testing.T) uint32 {
	_, c, err := integrity.CRC32(p)
	require.NoError(t, err)
	return c
}

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/usecases/fakes"
)

// Pins the defer-ordering bug: a Recv error must not let f.Close() fire
// while in-flight WriteAt goroutines are still running. With the fix the
// deferred eg.Wait() drains them first; the sleep hook makes the timing
// observable as an elapsed-time bound.
func TestDownloadWorkerStreamErrorDoesNotUseClosedFD(t *testing.T) {
	const writeSleep = 200 * time.Millisecond
	t.Setenv("WEAVIATE_TEST_DOWNLOAD_WRITE_SLEEP", writeSleep.String())

	localTmpDir := t.TempDir()

	mockClient := copier.NewMockFileReplicationServiceClient(t)
	mockRemoteIndex := types.NewMockRemoteIndex(t)

	opID := strfmt.UUID("22222222-2222-2222-2222-222222222222")
	const fileName = "big"
	const fileSize int64 = 64 * 1024
	chunk := make([]byte, fileSize)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}

	mockClient.EXPECT().CreateReplicaSnapshot(mock.Anything, mock.Anything).
		Return(&protocol.CreateReplicaSnapshotResponse{FileNames: []string{fileName}}, nil)
	mockClient.EXPECT().ReleaseReplicaSnapshot(mock.Anything, mock.Anything).
		Return(&protocol.ReleaseReplicaSnapshotResponse{}, nil)

	// Mismatched CRC forces the downloader to fetch rather than skip.
	mockClient.EXPECT().GetReplicaSnapshotFileMetadata(mock.Anything, mock.Anything).
		Return(&protocol.FileMetadata{
			IndexName: "collection",
			FileName:  fileName,
			Size:      fileSize,
			Crc32:     0xdeadbeef,
		}, nil)

	stream := copier.NewMockFileChunkStream(t)
	stream.EXPECT().Recv().Return(&protocol.FileChunk{
		Offset: 0,
		Data:   chunk,
		Eof:    false,
	}, nil).Once()
	stream.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "injected mid-stream error")).Once()

	mockClient.EXPECT().GetReplicaSnapshotFile(mock.Anything, mock.Anything).Return(stream, nil)

	fakeSelector := fakes.NewFakeClusterState("node1")
	logger, _ := logrusTest.NewNullLogger()

	c := copier.New(
		func(ctx context.Context, addr string) (copier.FileReplicationServiceClient, error) {
			return mockClient, nil
		},
		mockRemoteIndex,
		fakeSelector,
		1, // single worker keeps the timing deterministic
		localTmpDir,
		nil,
		"node1",
		logger,
	)

	start := time.Now()
	err := c.CopyReplicaFiles(context.Background(), opID, "node1", "collection", "shard", 0)
	elapsed := time.Since(start)

	require.Error(t, err, "expected the injected stream error to propagate")

	require.GreaterOrEqualf(t, elapsed, writeSleep-20*time.Millisecond,
		"call returned in %v (< %v); deferred eg.Wait() did not drain the writer",
		elapsed, writeSleep)

	// Tmp cleanup is only observable once writers have completed.
	tmpPath := filepath.Join(localTmpDir, "collection", "shard", fileName+".tmp")
	_, statErr := os.Stat(tmpPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

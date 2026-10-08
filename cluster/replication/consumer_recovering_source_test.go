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

package replication_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestConsumerRecoveringSourceFailsCleanly(t *testing.T) {
	const (
		opID       = uint64(41)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	startRefused := status.Errorf(codes.Internal, "start change capture for index %q, shard %q, op %q: %v",
		collection, shardName, "41", enterrors.ErrShardRecovering)
	snapshotRefused := status.Errorf(codes.Unavailable, "shard %q on index %q is recovering: %v",
		shardName, collection, enterrors.ErrShardRecovering)
	tests := []struct {
		name     string
		transfer api.ShardReplicationTransferType
		startErr error
		copyErr  error
		wantCopy bool
	}{
		{name: "copy refused at capture start", transfer: api.COPY, startErr: startRefused},
		{name: "move refused at capture start", transfer: api.MOVE, startErr: startRefused},
		{name: "copy refused at snapshot", transfer: api.COPY, copyErr: snapshotRefused, wantCopy: true},
		{name: "move refused at snapshot", transfer: api.MOVE, copyErr: snapshotRefused, wantCopy: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)
			copier.EXPECT().StopChangeCapture(mock.Anything, "node1", collection, shardName, "41").Return(nil)
			copier.EXPECT().StartChangeCapture(mock.Anything, "node1", collection, shardName, "41", mock.Anything).Return(tc.startErr)
			var copied atomic.Bool
			copier.EXPECT().CopyReplicaFiles(mock.Anything, mock.Anything, "node1", collection, shardName, mock.Anything).
				RunAndReturn(func(context.Context, strfmt.UUID, string, string, string, uint64) error {
					copied.Store(true)
					return tc.copyErr
				}).Maybe()
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.HYDRATING, nil)
			registered := make(chan string, 1)
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).
				Run(func(_ context.Context, _ uint64, msg string) { registered <- msg }).Return(nil).Once()

			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1, metrics.NewReplicationEngineOpsCallbacksBuilder().Build(),
				newShardSchemaReader(collection, shardName, "node1", "node3"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, tc.transfer),
				replication.NewShardReplicationStatus(api.HYDRATING))

			var msg string
			select {
			case msg = <-registered:
			case <-time.After(5 * time.Second):
				t.Fatal("recovering source never failed the attempt")
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			require.Contains(t, msg, enterrors.ErrShardRecovering.Error())
			require.False(t, replication.IsReversibleRefusal(tc.startErr))
			require.Equal(t, tc.wantCopy, copied.Load())
			fsm.AssertNotCalled(t, "ReplicationUpdateReplicaOpStatus", mock.Anything, mock.Anything, mock.Anything)
			fsm.AssertNotCalled(t, "DeleteReplicaFromShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			fsm.AssertNotCalled(t, "ReplicationAddReplicaToShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

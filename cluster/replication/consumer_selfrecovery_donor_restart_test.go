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
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestConsumerSelfRecoveryFinalizingRewindsOrHoldsWhenDonorLostItsChangeLog(t *testing.T) {
	const (
		opID       = uint64(43)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	tests := []struct {
		name           string
		donorMsg       string
		wantStates     []api.ShardReplicationState
		wantRegistered bool
	}{
		{
			name:       "current donor reports the log lost and the op rewinds to hydrating",
			donorMsg:   changelog.ErrMsgChangeLogLost,
			wantStates: []api.ShardReplicationState{api.HYDRATING},
		},
		{
			name:           "older donor reports no active log and the op holds in finalizing",
			donorMsg:       changelog.ErrMsgNoActiveChangeCaptureLog,
			wantRegistered: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sweptLog := status.Errorf(codes.Internal, "incoming snapshot change-log LSN: op %q: shard: %s for that op-id", "43", tc.donorMsg)

			logger, _ := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.FINALIZING, nil)
			fsm.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil)
			fsm.EXPECT().ReplicationLocalOpCancelState(opID).Return(types.OpCancelState{State: api.FINALIZING, UnCancellable: true}, nil)
			copier.EXPECT().PromoteRecoveryFolder(collection, shardName).Return(nil)
			copier.EXPECT().PromoteRecoveredShard(mock.Anything, collection, shardName).Return(nil)
			copier.EXPECT().SnapshotChangeLogLSN(mock.Anything, "node1", collection, shardName, "43").Return(uint64(0), sweptLog)

			var (
				mu         sync.Mutex
				states     []api.ShardReplicationState
				registered []string
			)
			settled := make(chan struct{}, 2)
			fsm.EXPECT().ReplicationUpdateReplicaOpStatus(mock.Anything, opID, mock.Anything).
				RunAndReturn(func(_ context.Context, _ uint64, state api.ShardReplicationState) error {
					mu.Lock()
					states = append(states, state)
					mu.Unlock()
					settled <- struct{}{}
					return nil
				}).Maybe()
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).
				RunAndReturn(func(_ context.Context, _ uint64, msg string) error {
					mu.Lock()
					registered = append(registered, msg)
					mu.Unlock()
					settled <- struct{}{}
					return types.ErrCancellationImpossible
				}).Maybe()

			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1, metrics.NewReplicationEngineOpsCallbacksBuilder().Build(),
				newShardSchemaReader(collection, shardName, "node1", "node2", "node3"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, api.SELF_RECOVERY),
				replication.NewShardReplicationStatus(api.FINALIZING))

			select {
			case <-settled:
			case <-time.After(5 * time.Second):
				t.Fatal("swept donor log never settled the FINALIZING attempt")
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.wantStates, states)
			if tc.wantRegistered {
				require.Len(t, registered, 1)
				require.Contains(t, registered[0], changelog.ErrMsgNoActiveChangeCaptureLog)
			} else {
				require.Empty(t, registered)
			}
			fsm.AssertNotCalled(t, "ReplicationCancellationComplete", mock.Anything, mock.Anything)
			copier.AssertNotCalled(t, "TailAndApply", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			copier.AssertNotCalled(t, "FinalizeChangeLog", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			copier.AssertNotCalled(t, "DropLocalShard", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

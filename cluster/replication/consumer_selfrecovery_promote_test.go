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
	stderrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type localReads struct {
	mu    sync.Mutex
	steps []types.OpCancelState
	err   error
	calls int
}

func (r *localReads) next(uint64) (types.OpCancelState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return types.OpCancelState{}, r.err
	}
	i := min(r.calls-1, len(r.steps)-1)
	return r.steps[i], nil
}

func TestConsumerSelfRecoveryPromoteGate(t *testing.T) {
	const (
		opID       = uint64(21)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	finalizing := types.OpCancelState{State: api.FINALIZING}
	uncancellable := types.OpCancelState{State: api.FINALIZING, UnCancellable: true}
	tests := []struct {
		name        string
		transfer    api.ShardReplicationTransferType
		steps       []types.OpCancelState
		readErr     error
		wantPromote bool
		wantOutcome string
	}{
		{
			name:        "cancel applied before FINALIZING never promotes",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{{State: api.FINALIZING, ShouldCancel: true}},
			wantOutcome: "cancelled",
		},
		{
			name:        "cancel completed before the dispatch never promotes",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{{State: api.CANCELLED}},
			wantOutcome: "cancelled",
		},
		{
			name:        "op deleted before the dispatch never promotes",
			transfer:    api.SELF_RECOVERY,
			readErr:     fmt.Errorf("op %d: %w", opID, types.ErrReplicationOperationNotFound),
			wantOutcome: "cancelled",
		},
		{
			name:        "collection deletion past the point of no return never promotes",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{{State: api.FINALIZING, ShouldCancel: true, UnCancellable: true}},
			wantOutcome: "cancelled",
		},
		{
			name:        "local FSM without the point of no return never promotes",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{finalizing},
			wantOutcome: "failed",
		},
		{
			name:        "point of no return promotes",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{uncancellable},
			wantPromote: true,
			wantOutcome: "failed",
		},
		{
			name:        "promotes once the local FSM applies FINALIZING",
			transfer:    api.SELF_RECOVERY,
			steps:       []types.OpCancelState{{State: api.HYDRATING}, {State: api.HYDRATING}, uncancellable},
			wantPromote: true,
			wantOutcome: "failed",
		},
		{
			name:        "copy is not gated",
			transfer:    api.COPY,
			steps:       []types.OpCancelState{{State: api.FINALIZING, ShouldCancel: true}},
			wantPromote: true,
			wantOutcome: "failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)
			expectChangeCaptureMocks(copier, fsm)

			reads := &localReads{steps: tc.steps, err: tc.readErr}
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.FINALIZING, nil)
			fsm.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
			fsm.EXPECT().ReplicationLocalOpCancelState(opID).RunAndReturn(reads.next).Maybe()
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).Return(nil).Maybe()

			var promoted atomic.Bool
			stop := stderrors.New("stop after the promote")
			copier.EXPECT().PromoteRecoveryFolder(collection, shardName).RunAndReturn(func(string, string) error {
				promoted.Store(true)
				return stop
			}).Maybe()
			copier.EXPECT().LoadLocalShard(mock.Anything, collection, shardName).RunAndReturn(func(context.Context, string, string) error {
				promoted.Store(true)
				return stop
			}).Maybe()

			outcome := make(chan string, 2)
			callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
				WithOpCancelledCallback(func(string) { outcome <- "cancelled" }).
				WithOpFailedCallback(func(string) { outcome <- "failed" }).
				Build()
			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 3*time.Second, 1, callbacks,
				newShardSchemaReader(collection, shardName, "node1", "node2"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, tc.transfer),
				replication.NewShardReplicationStatus(api.FINALIZING))

			select {
			case got := <-outcome:
				require.Equal(t, tc.wantOutcome, got)
			case <-time.After(10 * time.Second):
				t.Fatal("op never settled")
			}
			close(opsChan)
			require.NoError(t, <-doneChan)
			require.Equal(t, tc.wantPromote, promoted.Load())
		})
	}
}

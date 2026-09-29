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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
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
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func newShardSchemaManager(collection, shardName string, nodes ...string) *schema.SchemaManager {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaManager.AddClass(
		buildApplyRequest(collection, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: collection, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{shardName: {BelongsToNodes: nodes}},
			},
		}), "node1", true, false)
	return schemaManager
}

func TestConsumerHydratingDemotesListedTarget(t *testing.T) {
	const (
		opID       = uint64(13)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	tests := []struct {
		name         string
		transfer     api.ShardReplicationTransferType
		replicas     []string
		deleteErr    error
		removal      string
		waitTimeout  time.Duration
		wantCalls    []string
		wantReplicas []string
	}{
		{
			name:         "listed copy target is removed then unloaded before the re-copy",
			transfer:     api.COPY,
			replicas:     []string{"node1", "node2"},
			wantCalls:    []string{"delete", "wait", "unload", "stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1"},
		},
		{
			name:         "listed move target is removed then unloaded before the re-copy",
			transfer:     api.MOVE,
			replicas:     []string{"node1", "node3", "node2"},
			wantCalls:    []string{"delete", "wait", "unload", "stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1", "node3"},
		},
		{
			name:         "removal visible only after a few polls",
			transfer:     api.COPY,
			replicas:     []string{"node1", "node2"},
			removal:      "lagging",
			wantCalls:    []string{"delete", "wait", "unload", "stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1"},
		},
		{
			name:         "removal never visible times out before capture",
			transfer:     api.COPY,
			replicas:     []string{"node1", "node2"},
			removal:      "never",
			waitTimeout:  100 * time.Millisecond,
			wantCalls:    []string{"delete", "wait"},
			wantReplicas: []string{"node1", "node2"},
		},
		{
			name:         "unlisted target left loaded by a pre-add rewind is unloaded",
			transfer:     api.MOVE,
			replicas:     []string{"node1"},
			wantCalls:    []string{"unload", "stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1"},
		},
		{
			name:         "fresh copy runs the no-op unload before capture",
			transfer:     api.COPY,
			replicas:     []string{"node1"},
			wantCalls:    []string{"unload", "stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1"},
		},
		{
			name:         "refused removal stops before capture",
			transfer:     api.COPY,
			replicas:     []string{"node1", "node2"},
			deleteErr:    stderrors.New("unable to delete replica from shard, minimum replication factor 2"),
			wantCalls:    []string{"delete"},
			wantReplicas: []string{"node1", "node2"},
		},
		{
			name:         "other transfer types keep a listed target",
			transfer:     api.ShardReplicationTransferType("REHYDRATE"),
			replicas:     []string{"node1", "node2"},
			wantCalls:    []string{"stop", "start", "copy", "stop"},
			wantReplicas: []string{"node1", "node2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)
			schemaManager := newShardSchemaManager(collection, shardName, tc.replicas...)

			var (
				mu    sync.Mutex
				calls []string
			)
			record := func(name string) {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, name)
			}
			removeNode2 := func() error {
				return schemaManager.DeleteReplicaFromShard(buildApplyRequest(collection,
					api.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD,
					api.DeleteReplicaFromShard{Class: collection, Shard: shardName, TargetNode: "node2"}), true)
			}
			opIDStr := fmt.Sprint(opID)
			fsm.EXPECT().DeleteReplicaFromShard(mock.Anything, collection, shardName, "node2").
				RunAndReturn(func(context.Context, string, string, string) (uint64, error) {
					record("delete")
					if tc.deleteErr != nil {
						return 0, tc.deleteErr
					}
					if tc.removal != "" {
						return 42, nil
					}
					return 42, removeNode2()
				}).Maybe()
			fsm.EXPECT().WaitForUpdate(mock.Anything, uint64(42)).
				RunAndReturn(func(context.Context, uint64) error {
					record("wait")
					if tc.removal == "lagging" {
						time.AfterFunc(50*time.Millisecond, func() {
							if err := removeNode2(); err != nil {
								t.Errorf("delayed removal: %v", err)
							}
						})
					}
					return nil
				}).Maybe()
			copier.EXPECT().UnloadLocalShard(mock.Anything, collection, shardName).
				RunAndReturn(func(context.Context, string, string) error { record("unload"); return nil }).Maybe()
			copier.EXPECT().StopChangeCapture(mock.Anything, "node1", collection, shardName, opIDStr).
				RunAndReturn(func(context.Context, string, string, string, string) error { record("stop"); return nil }).Maybe()
			copier.EXPECT().StartChangeCapture(mock.Anything, "node1", collection, shardName, opIDStr, mock.Anything).
				RunAndReturn(func(context.Context, string, string, string, string, uint64) error { record("start"); return nil }).Maybe()
			copier.EXPECT().CopyReplicaFiles(mock.Anything, mock.Anything, "node1", collection, shardName, mock.Anything).
				RunAndReturn(func(context.Context, strfmt.UUID, string, string, string, uint64) error {
					record("copy")
					return stderrors.New("copy interrupted")
				}).Maybe()
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.HYDRATING, nil)
			registered := make(chan struct{})
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).
				Run(func(context.Context, uint64, string) { close(registered) }).
				Return(nil).Once()

			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1,
				metrics.NewReplicationEngineOpsCallbacksBuilder().Build(),
				schemaManager.NewSchemaReader())
			waitTimeout := 5 * time.Second
			if tc.waitTimeout > 0 {
				waitTimeout = tc.waitTimeout
			}
			replication.SetDemoteWait(consumer, waitTimeout, 10*time.Millisecond)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, tc.transfer),
				replication.NewShardReplicationStatus(api.HYDRATING),
			)

			select {
			case <-registered:
			case <-time.After(10 * time.Second):
				t.Fatal("hydrating attempt never settled")
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.wantCalls, calls)
			replicas, err := schemaManager.NewSchemaReader().ShardReplicas(collection, shardName)
			require.NoError(t, err)
			require.Equal(t, tc.wantReplicas, replicas)
		})
	}
}

func TestConsumerFinalizingAddRefusedOutsideFinalizing(t *testing.T) {
	const (
		opID       = uint64(17)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	tests := []struct {
		name         string
		addErr       error
		wantAdvanced bool
	}{
		{
			name:   "add refused because the op was rewound",
			addErr: stderrors.New("rpc error: code = Unknown desc = op 17 is HYDRATING: " + types.ErrAddReplicaOpNotFinalizing.Error()),
		},
		{
			name:   "add refused because the op moved forward",
			addErr: status.Error(codes.Internal, "op 17 is INTEGRATING: "+types.ErrAddReplicaOpNotFinalizing.Error()),
		},
		{
			name:         "accepted add advances",
			wantAdvanced: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)

			copier.EXPECT().LoadLocalShard(mock.Anything, collection, shardName).Return(nil)
			copier.EXPECT().SnapshotChangeLogLSN(mock.Anything, "node1", collection, shardName, fmt.Sprint(opID)).Return(uint64(3), nil)
			copier.EXPECT().TailAndApply(mock.Anything, "node1", collection, shardName, fmt.Sprint(opID), uint64(3)).Return(uint64(3), nil)
			fsm.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil)
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).Return(api.FINALIZING, nil)

			added := make(chan struct{})
			fsm.EXPECT().ReplicationAddReplicaToShard(mock.Anything, collection, shardName, "node2", opID).
				RunAndReturn(func(context.Context, string, string, string, uint64) (uint64, error) {
					close(added)
					return 0, tc.addErr
				}).Once()
			var registered, updated atomic.Int32
			fsm.EXPECT().ReplicationRegisterError(mock.Anything, opID, mock.Anything).
				RunAndReturn(func(context.Context, uint64, string) error { registered.Add(1); return nil }).Maybe()
			advanced := make(chan struct{}, 1)
			fsm.EXPECT().ReplicationUpdateReplicaOpStatus(mock.Anything, opID, api.INTEGRATING).
				RunAndReturn(func(context.Context, uint64, api.ShardReplicationState) error {
					updated.Add(1)
					advanced <- struct{}{}
					return stderrors.New("stop after the transition")
				}).Maybe()

			var lostCount atomic.Int32
			callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
				WithChangeCaptureLostCallback(func(string) { lostCount.Add(1) }).Build()
			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1, callbacks,
				newShardSchemaReader(collection, shardName, "node1"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			opsChan <- replication.NewShardReplicationOpAndStatus(
				replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, api.COPY),
				replication.NewShardReplicationStatus(api.FINALIZING),
			)

			select {
			case <-added:
			case <-time.After(10 * time.Second):
				t.Fatal("add was never attempted")
			}
			if tc.wantAdvanced {
				select {
				case <-advanced:
				case <-time.After(10 * time.Second):
					t.Fatal("op never advanced")
				}
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			require.Zero(t, lostCount.Load())
			if tc.wantAdvanced {
				require.Equal(t, int32(1), updated.Load())
				return
			}
			require.Zero(t, registered.Load())
			require.Zero(t, updated.Load())
		})
	}
}

func TestConsumerRedispatchSkipsFailurePath(t *testing.T) {
	const (
		opID       = uint64(19)
		collection = "TestCollection"
		shardName  = "shard1"
	)
	lost := stderrors.New("snapshot change-log LSN: shard: " + changelog.ErrMsgChangeLogLost + " for that op-id")
	refused := status.Error(codes.Internal, "op 19 is HYDRATING: "+types.ErrAddReplicaOpNotFinalizing.Error())
	tests := []struct {
		name    string
		snapErr error
		addErr  error
	}{
		{name: "lost change log rewinds to hydrating", snapErr: lost},
		{name: "leader refuses the add of a rewound op", addErr: refused},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			fsm := types.NewMockFSMUpdater(t)
			copier := types.NewMockReplicaCopier(t)
			opIDStr := fmt.Sprint(opID)

			var state atomic.Value
			state.Store(api.FINALIZING)
			firstPassDone := make(chan struct{})
			rewind := func() {
				state.Store(api.HYDRATING)
				close(firstPassDone)
			}
			fsm.EXPECT().ReplicationGetReplicaOpStatus(mock.Anything, opID).
				RunAndReturn(func(context.Context, uint64) (api.ShardReplicationState, error) {
					return state.Load().(api.ShardReplicationState), nil
				})
			fsm.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
			copier.EXPECT().LoadLocalShard(mock.Anything, collection, shardName).Return(nil)
			copier.EXPECT().SnapshotChangeLogLSN(mock.Anything, "node1", collection, shardName, opIDStr).Return(uint64(3), tc.snapErr)
			copier.EXPECT().TailAndApply(mock.Anything, "node1", collection, shardName, opIDStr, uint64(3)).Return(uint64(3), nil).Maybe()
			fsm.EXPECT().ReplicationAddReplicaToShard(mock.Anything, collection, shardName, "node2", opID).
				RunAndReturn(func(context.Context, string, string, string, uint64) (uint64, error) {
					rewind()
					return 0, tc.addErr
				}).Maybe()
			fsm.EXPECT().ReplicationUpdateReplicaOpStatus(mock.Anything, opID, api.HYDRATING).
				RunAndReturn(func(context.Context, uint64, api.ShardReplicationState) error {
					rewind()
					return nil
				}).Maybe()

			var failed atomic.Int32
			var failedAtRedispatch, failureLogsAtRedispatch int
			redispatched := make(chan struct{})
			copier.EXPECT().UnloadLocalShard(mock.Anything, collection, shardName).Return(nil).Maybe()
			copier.EXPECT().StopChangeCapture(mock.Anything, "node1", collection, shardName, opIDStr).Return(nil).Maybe()
			copier.EXPECT().StartChangeCapture(mock.Anything, "node1", collection, shardName, opIDStr, mock.Anything).
				RunAndReturn(func(context.Context, string, string, string, string, uint64) error {
					failedAtRedispatch = int(failed.Load())
					for _, e := range hook.AllEntries() {
						if e.Level == logrus.ErrorLevel && strings.Contains(e.Message, "replication operation failed") {
							failureLogsAtRedispatch++
						}
					}
					close(redispatched)
					return context.Canceled
				}).Once()

			callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
				WithOpFailedCallback(func(string) { failed.Add(1) }).Build()
			consumer := replication.NewCopyOpConsumer(logger, fsm, copier, "node2", &backoff.StopBackOff{},
				replication.NewOpsCache(), 10*time.Second, 1, callbacks,
				newShardSchemaReader(collection, shardName, "node1"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opsChan := make(chan replication.ShardReplicationOpAndStatus, 256)
			doneChan := make(chan error, 1)
			enterrors.GoWrapper(func() { doneChan <- consumer.Consume(ctx, opsChan) }, logger)
			op := replication.NewShardReplicationOp(opID, "node1", "node2", collection, shardName, api.COPY)
			opsChan <- replication.NewShardReplicationOpAndStatus(op, replication.NewShardReplicationStatus(api.FINALIZING))

			select {
			case <-firstPassDone:
			case <-time.After(10 * time.Second):
				t.Fatal("first pass never rewound")
			}
			deadline := time.After(2 * time.Second)
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
		resend:
			for {
				select {
				case <-redispatched:
					break resend
				case <-deadline:
					t.Fatal("rewound op was not re-dispatched before the failure backoff")
				case <-ticker.C:
					opsChan <- replication.NewShardReplicationOpAndStatus(op, replication.NewShardReplicationStatus(api.HYDRATING))
				}
			}
			close(opsChan)
			require.NoError(t, <-doneChan)

			require.Zero(t, failedAtRedispatch)
			require.Zero(t, failureLogsAtRedispatch)
		})
	}
}

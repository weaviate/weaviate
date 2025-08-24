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

package replication_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestConsumerStateChangeOrder tests that the consumer correctly transitions the state of the operation
func TestConsumerStateChangeOrder(t *testing.T) {
	t.Parallel()

	opId := 0

	testCases := []struct {
		name           string
		transferType   api.ShardReplicationTransferType
		setupMocksFunc func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier)
	}{
		{
			name:         "All operations are processed in order in copy mode",
			transferType: api.COPY,
			setupMocksFunc: func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier) {
				wg.Add(1)
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
					Return(api.REGISTERED, nil).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.FINALIZING).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).
					Return(nil).Maybe()
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					// Simulate that the async replication is already done
					Return(models.AsyncReplicationStatus{StartDiffTimeUnixMillis: time.Now().Add(time.Second * 200).UnixMilli(), ObjectsPropagated: 0}, nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
					Run(func(ctx context.Context, opId uint64, state api.ShardReplicationState) {
						wg.Done()
					}).
					Return(nil)
			},
		},
		{
			name:         "consumer resumes on state change failure",
			transferType: api.COPY,
			setupMocksFunc: func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier) {
				wg.Add(1)
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
					Return(api.REGISTERED, nil).
					Times(2) // equal to the op plus number of times the op failed
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(fmt.Errorf("failed to update state")).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.FINALIZING).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).
					Return(nil).Maybe()
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					// Simulate that the async replication is already done
					Return(models.AsyncReplicationStatus{StartDiffTimeUnixMillis: time.Now().Add(time.Second * 200).UnixMilli(), ObjectsPropagated: 0}, nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
					Run(func(ctx context.Context, opId uint64, state api.ShardReplicationState) {
						wg.Done()
					}).
					Return(nil)
			},
		},
		{
			name:         "consumer resumes on replica copier failures",
			transferType: api.COPY,
			setupMocksFunc: func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier) {
				wg.Add(1)
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
					Return(api.REGISTERED, nil).
					Times(2) // equal to the op plus number of times the op failed
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.FINALIZING).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(fmt.Errorf("failed to copy replica")).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationRegisterError(mock.Anything, uint64(opId), fmt.Errorf("failed to copy replica").Error()).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).
					Return(nil).Maybe()
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					// Simulate that the async replication is already done
					Return(models.AsyncReplicationStatus{StartDiffTimeUnixMillis: time.Now().Add(time.Second * 200).UnixMilli(), ObjectsPropagated: 0}, nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
					Run(func(ctx context.Context, opId uint64, state api.ShardReplicationState) {
						wg.Done()
					}).
					Return(nil)
			},
		},
		{
			name:         "consumer resumes on async replication failures",
			transferType: api.COPY,
			setupMocksFunc: func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier) {
				wg.Add(1)
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
					Return(api.REGISTERED, nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.FINALIZING).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(fmt.Errorf("failed to initialize async replication")).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationRegisterError(mock.Anything, uint64(opId), fmt.Errorf("failed to initialize async replication").Error()).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(fmt.Errorf("failed to set async replication target node")).
					Times(1)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).
					Return(nil).Maybe()
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationRegisterError(mock.Anything, uint64(opId), fmt.Errorf("failed to set async replication target node").Error()).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				// Async replication status triggers an internal retry and doesn't register an error
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(models.AsyncReplicationStatus{}, fmt.Errorf("failed to get async replication status")).
					Times(1)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					// Simulate that the async replication is already done
					Return(models.AsyncReplicationStatus{StartDiffTimeUnixMillis: time.Now().Add(time.Second * 200).UnixMilli(), ObjectsPropagated: 0}, nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
					Run(func(ctx context.Context, opId uint64, state api.ShardReplicationState) {
						wg.Done()
					}).
					Return(nil)
			},
		},
		{
			name:         "All operations are processed in order in move mode",
			transferType: api.MOVE,
			setupMocksFunc: func(wg *sync.WaitGroup, mockFSMUpdater *types.MockFSMUpdater, mockReplicaCopier *types.MockReplicaCopier) {
				wg.Add(1)
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
					Return(api.REGISTERED, nil).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.FINALIZING).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).
					Return(nil).Maybe()
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					// Simulate that the async replication is already done
					Return(models.AsyncReplicationStatus{StartDiffTimeUnixMillis: time.Now().Add(time.Second * 200).UnixMilli(), ObjectsPropagated: 0}, nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.DEHYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					DeleteReplicaFromShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(uint64(0), nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
					Run(func(ctx context.Context, opId uint64, state api.ShardReplicationState) {
						wg.Done()
					}).
					Return(nil)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup
			logger, _ := logrustest.NewNullLogger()
			mockFSMUpdater := types.NewMockFSMUpdater(t)
			mockReplicaCopier := types.NewMockReplicaCopier(t)
			reg := prometheus.NewPedanticRegistry()
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
			schemaReader := schemaManager.NewSchemaReader()
			manager := replication.NewManager(schemaReader, reg)

			ctx := t.Context()
			replicateRequest := &api.ReplicationReplicateShardRequest{
				Uuid:             strfmt.UUID(uuid.New().String()),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     tc.transferType.String(),
			}

			consumer := replication.NewCopyOpConsumer(
				logger,
				mockFSMUpdater,
				mockReplicaCopier,
				replicateRequest.TargetNode,
				&backoff.StopBackOff{},
				replication.NewOpsCache(),
				time.Second*20,
				1,
				runtime.NewDynamicValue(time.Second*100),
				metrics.NewReplicationEngineOpsCallbacksBuilder().Build(),
				schemaReader,
			)
			tc.setupMocksFunc(&wg, mockFSMUpdater, mockReplicaCopier)

			producer := replication.NewFSMOpProducer(logger, manager.GetReplicationFSM(), time.Second*1, replicateRequest.TargetNode)

			// Setup the class + shard in the schema
			// We only use the manager + fsm + schema to "kickstart" the producer/consumer read loop, all the subsequent
			// operations are triggered by the producer/consumer themselves and we use the mocks to verify the state changes
			schemaManager.AddClass(buildApplyRequest(replicateRequest.SourceCollection, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: replicateRequest.SourceCollection, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{
						replicateRequest.SourceShard: {BelongsToNodes: []string{replicateRequest.SourceNode}},
						"shard2":                     {BelongsToNodes: []string{replicateRequest.TargetNode}},
					},
				},
			}), "node1", true, false)
			// Start a replicate operation
			err := manager.Replicate(0, buildApplyRequest(replicateRequest.SourceCollection, api.ApplyRequest_TYPE_REPLICATION_REPLICATE, replicateRequest))
			require.NoError(t, err)

			targetOpsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
			defer close(targetOpsChan)
			ctx, cancel := context.WithCancel(ctx)

			consumerDoneChan := make(chan error, 1)
			producerDoneChan := make(chan error, 1)
			go func() {
				producerDoneChan <- producer.Produce(ctx, targetOpsChan)
			}()
			go func() {
				consumerDoneChan <- consumer.Consume(ctx, targetOpsChan)
			}()

			// Ensure that we wait for the waitgroup up to a given amount of time
			waitChan := make(chan struct{})
			go func() {
				wg.Wait()
				waitChan <- struct{}{}
			}()
			select {
			case <-time.After(30 * time.Second):
				cancel()
				t.Fatal("Test timed out waiting for operation completion")
			case <-waitChan:
				cancel()
				// This is here just to make sure the test does not run indefinitely
			}

			err = <-producerDoneChan
			require.ErrorIs(t, err, context.Canceled)
			err = <-consumerDoneChan
			require.ErrorIs(t, err, context.Canceled)

			// Assert that the mock expectations were met
			mockFSMUpdater.AssertExpectations(t)
			mockReplicaCopier.AssertExpectations(t)
		})
	}
}

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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
)

func TestConsumerWithCallbacks(t *testing.T) {
	t.Run("successful operation should trigger expected callbacks", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		schemaManager.AddClass(
			buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
				},
			}), "node1", true, false)

		opId, err := randInt(t, 100, 200)
		require.NoError(t, err, "error generating random operation id")

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
		mockFSMUpdater.EXPECT().
			ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
			Return(nil)
		mockFSMUpdater.EXPECT().
			ReplicationAddReplicaToShard(mock.Anything, "TestCollection", "shard1", "node2", uint64(opId)).
			Return(uint64(0), nil)
		mockFSMUpdater.EXPECT().
			SyncShard(mock.Anything, "TestCollection", "shard1", "node1").
			Return(uint64(0), nil).
			Times(1)
		mockFSMUpdater.EXPECT().
			SyncShard(mock.Anything, "TestCollection", "shard1", "node2").
			Return(uint64(0), nil).
			Times(1)
		mockReplicaCopier.EXPECT().
			CopyReplicaFiles(
				mock.Anything,
				"node1",
				"TestCollection",
				"shard1",
				mock.Anything,
			).
			Once().
			Return(nil)
		mockReplicaCopier.EXPECT().
			LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
			Return(nil)
		mockReplicaCopier.EXPECT().
			InitAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").
			Return(nil)
		mockReplicaCopier.EXPECT().
			AsyncReplicationStatus(mock.Anything, "node1", "node2", "TestCollection", "shard1").
			Return(models.AsyncReplicationStatus{
				ObjectsPropagated:       0,
				StartDiffTimeUnixMillis: time.Now().Add(200 * time.Second).UnixMilli(),
			}, nil)
		mockReplicaCopier.EXPECT().
			AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicaCopier.EXPECT().
			RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).Return(nil)
		mockReplicaCopier.EXPECT().
			RevertAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").Return(nil)

		var (
			prepareProcessingCallbacksCounter int
			pendingCallbacksCounter           int
			skippedCallbacksCounter           int
			startedCallbacksCounter           int
			completedCallbacksCounter         int
			failedCallbacksCounter            int
			completionWg                      sync.WaitGroup
		)
		completionWg.Add(1)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				require.Equal(t, "node2", node, "invalid node in prepare processing callback")
				prepareProcessingCallbacksCounter++
			}).
			WithOpPendingCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in pending op callback")
				pendingCallbacksCounter++
			}).
			WithOpSkippedCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in skipped op callback")
				skippedCallbacksCounter++
			}).
			WithOpStartCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in start op callback")
				startedCallbacksCounter++
			}).
			WithOpCompleteCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in complete op callback")
				completedCallbacksCounter++
				completionWg.Done()
			}).
			WithOpFailedCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in failed op callback")
				failedCallbacksCounter++
				t.Error("Failed callback should not be called for successful operation")
			}).Build()

		consumer := replication.NewCopyOpConsumer(
			logger,
			mockFSMUpdater,
			mockReplicaCopier,
			"node2",
			&backoff.StopBackOff{},
			replication.NewOpsCache(),
			time.Second*10,
			1,
			runtime.NewDynamicValue(time.Second*100),
			metricsCallbacks,
			schemaReader,
		)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		opsChan <- replication.NewShardReplicationOpAndStatus(replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "TestCollection", "shard1", api.COPY), replication.NewShardReplicationStatus(api.REGISTERED))
		waitChan := make(chan struct{})
		go func() {
			completionWg.Wait()
			waitChan <- struct{}{}
		}()

		select {
		case <-waitChan:
			// This is here just to make sure the test does not run indefinitely
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out waiting for operation completion")
		}

		close(opsChan)
		err = <-doneChan

		// THEN
		require.NoError(t, err, "expected operation completing successfully")
		require.Equal(t, 1, prepareProcessingCallbacksCounter, "expected prepare processing callback to be called once")
		require.Equal(t, 1, pendingCallbacksCounter, "Pending callback should be called")
		require.Equal(t, 0, skippedCallbacksCounter, "Skipped callback should be called")
		require.Equal(t, 1, startedCallbacksCounter, "Start callback should be called")
		require.Equal(t, 1, completedCallbacksCounter, "Complete callback should be called")
		require.Equal(t, 0, failedCallbacksCounter, "Failed callback should be called for failed operation")
		mockFSMUpdater.AssertExpectations(t)
		mockReplicaCopier.AssertExpectations(t)
	})

	t.Run("failed operation should trigger failed callback", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		schemaManager.AddClass(
			buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
				},
			}), "node1", true, false)

		opId, err := randInt(t, 100, 200)
		require.NoError(t, err, "error generating random operation id")

		mockFSMUpdater.EXPECT().
			ReplicationGetReplicaOpStatus(mock.Anything, uint64(opId)).
			Return(api.REGISTERED, nil).
			Times(1)
		mockFSMUpdater.EXPECT().
			ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.HYDRATING).
			Return(nil)
		mockReplicaCopier.EXPECT().
			CopyReplicaFiles(
				mock.Anything,
				"node1",
				"TestCollection",
				"shard1",
				mock.Anything,
			).
			Once().
			Return(errors.New("simulated copy failure"))
		mockFSMUpdater.EXPECT().
			ReplicationRegisterError(mock.Anything, uint64(opId), mock.Anything).
			Return(nil)

		var (
			prepareProcessingCallbacksCounter int
			pendingCallbacksCounter           int
			skippedCallbacksCounter           int
			startedCallbacksCounter           int
			completedCallbacksCounter         int
			failedCallbacksCounter            int
			completionWg                      sync.WaitGroup
		)
		completionWg.Add(1)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				require.Equal(t, "node2", node, "invalid node in prepare processing callback")
				prepareProcessingCallbacksCounter++
			}).
			WithOpPendingCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in pending op callback")
				pendingCallbacksCounter++
			}).
			WithOpSkippedCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in skipped op callback")
				skippedCallbacksCounter++
			}).
			WithOpStartCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in start op callback")
				startedCallbacksCounter++
			}).
			WithOpCompleteCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in complete op callback")
				completedCallbacksCounter++
			}).
			WithOpFailedCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in failed op callback")
				failedCallbacksCounter++
				completionWg.Done()
			}).Build()

		consumer := replication.NewCopyOpConsumer(
			logger,
			mockFSMUpdater,
			mockReplicaCopier,
			"node2",
			&backoff.StopBackOff{}, // No retries for test
			replication.NewOpsCache(),
			time.Second*10,
			1,
			runtime.NewDynamicValue(time.Second*100),
			metricsCallbacks,
			schemaReader,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		opsChan <- replication.NewShardReplicationOpAndStatus(replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "TestCollection", "shard1", api.COPY), replication.NewShardReplicationStatus(api.REGISTERED))
		waitChan := make(chan struct{})
		go func() {
			completionWg.Wait()
			waitChan <- struct{}{}
		}()

		select {
		case <-waitChan:
			// This is here just to make sure the test does not run indefinitely
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out waiting for operation completion")
		}

		close(opsChan)
		err = <-doneChan

		// THEN
		require.NoError(t, err, "expected consumer to stop without error")
		require.Equal(t, 1, prepareProcessingCallbacksCounter, "Prepare processing callback should be called")
		require.Equal(t, 1, pendingCallbacksCounter, "Pending callback should be called")
		require.Equal(t, 0, skippedCallbacksCounter, "Skipped callback should be called")
		require.Equal(t, 1, startedCallbacksCounter, "Start callback should be called")
		require.Equal(t, 0, completedCallbacksCounter, "Complete callback should be called once")
		require.Equal(t, 1, failedCallbacksCounter, "Failed callback should be called for failed operation")
		mockFSMUpdater.AssertExpectations(t)
		mockReplicaCopier.AssertExpectations(t)
	})

	t.Run("multiple random concurrent operations should be tracked correctly", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()

		randomNumberOfOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")

		physical := make(map[string]sharding.Physical)
		for i := 0; i < randomNumberOfOps; i++ {
			physical[fmt.Sprintf("shard-%d", i)] = sharding.Physical{BelongsToNodes: []string{"node1"}}
		}
		schemaManager.AddClass(
			buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{Physical: physical},
			}), "node1", true, false)

		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random op id start")
		for i := 0; i < randomNumberOfOps; i++ {
			opId := uint64(randomStartOpId + i)
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
			mockFSMUpdater.EXPECT().
				ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opId), api.READY).
				Return(nil)
			mockReplicaCopier.EXPECT().
				CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			mockReplicaCopier.EXPECT().
				LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			mockFSMUpdater.EXPECT().
				ReplicationAddReplicaToShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything, uint64(opId)).
				Return(uint64(i), nil)
			mockReplicaCopier.EXPECT().
				AsyncReplicationStatus(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(models.AsyncReplicationStatus{
					ObjectsPropagated:       0,
					StartDiffTimeUnixMillis: time.Now().Add(200 * time.Second).UnixMilli(),
				}, nil)
			mockReplicaCopier.EXPECT().
				InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			mockReplicaCopier.EXPECT().
				AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			mockReplicaCopier.EXPECT().
				RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).Return(nil)
			mockReplicaCopier.EXPECT().
				RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			mockFSMUpdater.EXPECT().
				SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(i), nil)
		}

		var (
			mutex                  sync.Mutex
			prepareProcessingCount int
			pendingCount           int
			skippedCount           int
			startCount             int
			completeCount          int
			completionWg           sync.WaitGroup
		)
		completionWg.Add(randomNumberOfOps)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				mutex.Lock()
				prepareProcessingCount++
				mutex.Unlock()
			}).
			WithOpPendingCallback(func(node string) {
				mutex.Lock()
				pendingCount++
				mutex.Unlock()
			}).
			WithOpSkippedCallback(func(node string) {
				mutex.Lock()
				skippedCount++
				mutex.Unlock()
			}).
			WithOpStartCallback(func(node string) {
				mutex.Lock()
				startCount++
				mutex.Unlock()
			}).
			WithOpCompleteCallback(func(node string) {
				mutex.Lock()
				completeCount++
				mutex.Unlock()
				completionWg.Done()
			}).Build()

		consumer := replication.NewCopyOpConsumer(
			logger,
			mockFSMUpdater,
			mockReplicaCopier,
			"node2",
			&backoff.StopBackOff{},
			replication.NewOpsCache(),
			time.Second*10,
			1,
			runtime.NewDynamicValue(time.Second*100),
			metricsCallbacks,
			schemaReader,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOpAndStatus, randomNumberOfOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		for i := 0; i < randomNumberOfOps; i++ {
			shard := fmt.Sprintf("shard-%d", i)
			opsChan <- replication.NewShardReplicationOpAndStatus(replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", "node2", "TestCollection", shard, api.COPY), replication.NewShardReplicationStatus(api.REGISTERED))
		}

		waitChan := make(chan struct{})
		go func() {
			completionWg.Wait()
			waitChan <- struct{}{}
		}()

		select {
		case <-waitChan:
			// All operations completed
		case <-time.After(5 * time.Second):
			// This is here just to make sure the test does not run indefinitely
			t.Fatal("Test timed out waiting for operations to complete")
		}

		close(opsChan)
		err = <-doneChan

		// THEN
		require.NoError(t, err, "expected consumer to stop without error")
		mutex.Lock()
		require.Equal(t, 1, prepareProcessingCount, "Prepare processing callback should be called once")
		require.Equal(t, randomNumberOfOps, pendingCount, "Pending callback should be called for each operation")
		require.Equal(t, randomNumberOfOps, startCount, "Start callback should be called for each operation")
		require.Equal(t, randomNumberOfOps, completeCount, "Complete callback should be called for each operation")
		mutex.Unlock()
	})

	t.Run("all operations are skipped and should trigger skipped callbacks", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		schemaManager.AddClass(
			buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
				},
			}), "node1", true, false)

		totalOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")
		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random number of operations")
		opsCache := replication.NewOpsCache()

		for i := 0; i < totalOps; i++ {
			opId := uint64(randomStartOpId + i)
			opsCache.LoadOrStore(opId)
		}

		var (
			mutex                  sync.Mutex
			prepareProcessingCount int
			pendingCount           int
			skippedCount           int
			startCount             int
			completeCount          int
			failedCount            int
		)

		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				mutex.Lock()
				prepareProcessingCount++
				mutex.Unlock()
			}).
			WithOpPendingCallback(func(node string) {
				mutex.Lock()
				pendingCount++
				mutex.Unlock()
			}).
			WithOpSkippedCallback(func(node string) {
				mutex.Lock()
				skippedCount++
				mutex.Unlock()
			}).
			WithOpStartCallback(func(node string) {
				mutex.Lock()
				startCount++
				mutex.Unlock()
				t.Error("Start callback should not be called when all ops are skipped")
			}).
			WithOpCompleteCallback(func(node string) {
				mutex.Lock()
				completeCount++
				mutex.Unlock()
				t.Error("Complete callback should not be called when all ops are skipped")
			}).
			WithOpFailedCallback(func(node string) {
				mutex.Lock()
				failedCount++
				mutex.Unlock()
				t.Error("Failed callback should not be called when all ops are skipped")
			}).Build()

		consumer := replication.NewCopyOpConsumer(
			logger,
			mockFSMUpdater,
			mockReplicaCopier,
			"node2",
			&backoff.StopBackOff{},
			opsCache,
			time.Second*10,
			1,
			runtime.NewDynamicValue(time.Second*100),
			callbacks,
			schemaReader,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOpAndStatus, totalOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		require.NoError(t, err, "error while generating random op id start")

		for i := 0; i < totalOps; i++ {
			node := fmt.Sprintf("node-%d", i)
			opsChan <- replication.NewShardReplicationOpAndStatus(replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", node, "TestCollection", "shard1", api.COPY), replication.NewShardReplicationStatus(api.REGISTERED))
		}

		close(opsChan)
		err = <-doneChan

		// THEN
		require.NoError(t, err, "expected consumer to stop without error")

		mutex.Lock()
		require.Equal(t, 1, prepareProcessingCount, "Prepare processing callback should be called once")
		require.Equal(t, totalOps, pendingCount, "Pending should be called for each op")
		require.Equal(t, totalOps, skippedCount, "Skipped should be called for each op")
		require.Equal(t, 0, startCount, "Start should not be called when all ops are skipped")
		require.Equal(t, 0, completeCount, "Complete should not be called when all ops are skipped")
		require.Equal(t, 0, failedCount, "Failed should not be called when all ops are skipped")
		mutex.Unlock()

		mockFSMUpdater.AssertExpectations(t)
		mockReplicaCopier.AssertExpectations(t)
	})

	t.Run("some operations are randomly skipped and should trigger corresponding callbacks", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()

		totalOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")

		physical := make(map[string]sharding.Physical)
		for i := 0; i < totalOps; i++ {
			physical[fmt.Sprintf("shard-%d", i)] = sharding.Physical{BelongsToNodes: []string{"node1"}}
		}

		schemaManager.AddClass(
			buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{Physical: physical},
			}), "node1", true, false)

		var (
			mutex                  sync.Mutex
			prepareProcessingCount int
			pendingCount           int
			skippedCount           int
			startCount             int
			completeCount          int
			failedCount            int
			completionWg           sync.WaitGroup
		)

		opsCache := replication.NewOpsCache()

		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random op id start")

		expectedSkipped := 0
		expectedStarted := 0
		expectedCompleted := 0

		for i := 0; i < totalOps; i++ {
			opID := uint64(randomStartOpId + i)
			skip := randomBoolean(t)
			if !skip {
				expectedStarted++
				expectedCompleted++
				mockFSMUpdater.EXPECT().
					WaitForUpdate(mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationGetReplicaOpStatus(mock.Anything, uint64(opID)).
					Return(api.REGISTERED, nil).
					Times(1)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opID), api.HYDRATING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opID), api.FINALIZING).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(opID), api.READY).
					Return(nil)
				mockReplicaCopier.EXPECT().
					CopyReplicaFiles(mock.Anything, "node1", "TestCollection", mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockFSMUpdater.EXPECT().
					ReplicationAddReplicaToShard(mock.Anything, "TestCollection", mock.Anything, mock.Anything, uint64(opID)).
					Return(uint64(i), nil)
				mockReplicaCopier.EXPECT().
					AsyncReplicationStatus(mock.Anything, "node1", mock.Anything, "TestCollection", mock.Anything).
					Return(models.AsyncReplicationStatus{
						ObjectsPropagated:       0,
						StartDiffTimeUnixMillis: time.Now().Add(200 * time.Second).UnixMilli(),
					}, nil)
				mockReplicaCopier.EXPECT().
					AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).Return(nil)
				mockReplicaCopier.EXPECT().
					RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).Return(nil)
				mockReplicaCopier.EXPECT().
					InitAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)
				mockReplicaCopier.EXPECT().
					RevertAsyncReplicationLocally(mock.Anything, mock.Anything, mock.Anything).Return(nil)
				mockFSMUpdater.EXPECT().
					SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(i), nil)
				completionWg.Add(1)
			} else {
				require.False(t, opsCache.LoadOrStore(opID), "operation should not be stored twice in cache")
				expectedSkipped++
			}
		}

		callbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithPrepareProcessing(func(node string) {
				mutex.Lock()
				prepareProcessingCount++
				mutex.Unlock()
			}).
			WithOpPendingCallback(func(node string) {
				mutex.Lock()
				pendingCount++
				mutex.Unlock()
			}).
			WithOpSkippedCallback(func(node string) {
				mutex.Lock()
				skippedCount++
				mutex.Unlock()
			}).
			WithOpStartCallback(func(node string) {
				mutex.Lock()
				startCount++
				mutex.Unlock()
			}).
			WithOpCompleteCallback(func(node string) {
				mutex.Lock()
				completeCount++
				mutex.Unlock()
				completionWg.Done()
			}).
			WithOpFailedCallback(func(node string) {
				mutex.Lock()
				failedCount++
				mutex.Unlock()
				t.Error("Failed callback should not be called in this test")
			}).Build()

		consumer := replication.NewCopyOpConsumer(
			logger,
			mockFSMUpdater,
			mockReplicaCopier,
			"node2",
			&backoff.StopBackOff{},
			opsCache,
			time.Second*10,
			1,
			runtime.NewDynamicValue(time.Second*100),
			callbacks,
			schemaReader,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOpAndStatus, totalOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		for i := 0; i < totalOps; i++ {
			shard := fmt.Sprintf("shard-%d", i)
			opsChan <- replication.NewShardReplicationOpAndStatus(replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", "node2", "TestCollection", shard, api.COPY), replication.NewShardReplicationStatus(api.REGISTERED))
		}

		waitChan := make(chan struct{})
		go func() {
			completionWg.Wait()
			waitChan <- struct{}{}
		}()

		select {
		case <-waitChan:
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out waiting for operation completion")
		}

		close(opsChan)
		err = <-doneChan

		// THEN
		require.NoError(t, err, "expected consumer to stop without error")

		mutex.Lock()
		require.Equal(t, 1, prepareProcessingCount, "Prepare processing should be called once")
		require.Equal(t, totalOps, pendingCount, "Pending should be called for each op")
		require.Equal(t, expectedSkipped, skippedCount, "Skipped count should match")
		require.Equal(t, expectedStarted, startCount, "Started count should match non-skipped ops")
		require.Equal(t, expectedCompleted, completeCount, "Completed count should match non-skipped ops")
		require.Equal(t, 0, failedCount, "No operations should fail")
		mutex.Unlock()

		mockFSMUpdater.AssertExpectations(t)
		mockReplicaCopier.AssertExpectations(t)
	})
}

func TestConsumerOpCancellation(t *testing.T) {
	// GIVEN
	logger, _ := logrustest.NewNullLogger()
	mockFSMUpdater := types.NewMockFSMUpdater(t)
	mockReplicaCopier := types.NewMockReplicaCopier(t)
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaReader := schemaManager.NewSchemaReader()
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)

	mockFSMUpdater.EXPECT().
		ReplicationCancellationComplete(mock.Anything, uint64(1)).
		Return(nil)
	mockFSMUpdater.EXPECT().
		SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(0, nil)

	var completionWg sync.WaitGroup
	var once sync.Once
	completionWg.Add(1)
	metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			require.Equal(t, "node2", node, "invalid node in prepare processing callback")
		}).
		WithOpPendingCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in pending op callback")
		}).
		WithOpSkippedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in skipped op callback")
		}).
		WithOpStartCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in start op callback")
		}).
		WithOpCompleteCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in complete op callback")
		}).
		WithOpFailedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in failed op callback")
		}).
		WithOpCancelledCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in cancelled op callback")
			once.Do(func() {
				// cancelOp in Consumer is a complete noop so can be called multiple times
				// without error. However, completionWg.Done() can only be called once
				completionWg.Done()
			})
		}).
		Build()

	consumer := replication.NewCopyOpConsumer(
		logger,
		mockFSMUpdater,
		mockReplicaCopier,
		"node2",
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		time.Second*10,
		1,
		runtime.NewDynamicValue(time.Second*100),
		metricsCallbacks,
		schemaReader,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opsChan := make(chan replication.ShardReplicationOpAndStatus, 2)
	doneChan := make(chan error, 1)

	// WHEN
	go func() {
		doneChan <- consumer.Consume(ctx, opsChan)
	}()

	mockReplicaCopier.EXPECT().
		CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, sourceNode string, collectionName string, shardName string, schemaVersion uint64) error {
			// Simulate a long-running operation that checks for cancellation every loop
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(1 * time.Second)
			}
		}).Maybe()

	op := replication.NewShardReplicationOp(1, "node1", "node2", "TestCollection", "shard1", api.COPY)

	status := replication.NewShardReplicationStatus(api.HYDRATING)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.HYDRATING, nil)

	status.TriggerCancellation()
	// Simulate the copying step that will loop forever until cancelled in the mock
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	// Tests the cancellation happening before the copying has started (0s) and once it has started (1s)
	time.Sleep(1 * time.Second)
	// Cancel the operation via ShouldCancel or ShouldDelete
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)

	waitChan := make(chan struct{})
	go func() {
		completionWg.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
	case <-time.After(10 * time.Second):
		t.Fatalf("Test timed out waiting for operation completion")
	}

	close(opsChan)
	err := <-doneChan

	// THEN
	require.NoError(t, err, "expected consumer to stop without error")

	mockFSMUpdater.AssertExpectations(t)
	mockReplicaCopier.AssertExpectations(t)
}

func TestConsumerOpDeletion(t *testing.T) {
	// GIVEN
	logger, _ := logrustest.NewNullLogger()
	mockFSMUpdater := types.NewMockFSMUpdater(t)
	mockReplicaCopier := types.NewMockReplicaCopier(t)
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaReader := schemaManager.NewSchemaReader()
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)

	mockFSMUpdater.EXPECT().
		ReplicationRemoveReplicaOp(mock.Anything, uint64(1)).
		Return(nil)
	mockFSMUpdater.EXPECT().
		SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(0, nil)

	var completionWg sync.WaitGroup
	var once sync.Once
	completionWg.Add(1)
	metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			require.Equal(t, "node2", node, "invalid node in prepare processing callback")
		}).
		WithOpPendingCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in pending op callback")
		}).
		WithOpSkippedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in skipped op callback")
		}).
		WithOpStartCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in start op callback")
		}).
		WithOpCompleteCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in complete op callback")
		}).
		WithOpFailedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in failed op callback")
		}).
		WithOpCancelledCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in cancelled op callback")
			once.Do(func() {
				// cancelOp in Consumer is a complete noop so can be called multiple times
				// without error. However, completionWg.Done() can only be called once
				completionWg.Done()
			})
		}).
		Build()

	consumer := replication.NewCopyOpConsumer(
		logger,
		mockFSMUpdater,
		mockReplicaCopier,
		"node2",
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		time.Second*10,
		1,
		runtime.NewDynamicValue(time.Second*100),
		metricsCallbacks,
		schemaReader,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opsChan := make(chan replication.ShardReplicationOpAndStatus, 2)
	doneChan := make(chan error, 1)

	// WHEN
	go func() {
		doneChan <- consumer.Consume(ctx, opsChan)
	}()

	mockReplicaCopier.EXPECT().
		CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, sourceNode string, collectionName string, shardName string, schemaVersion uint64) error {
			// Simulate a long-running operation that checks for cancellation every loop
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(1 * time.Second)
			}
		}).Maybe()

	op := replication.NewShardReplicationOp(1, "node1", "node2", "TestCollection", "shard1", api.COPY)

	status := replication.NewShardReplicationStatus(api.HYDRATING)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.HYDRATING, nil)

	status.TriggerDeletion()
	// Simulate the copying step that will loop forever until cancelled in the mock
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	// Tests the cancellation happening before the copying has started (0s) and once it has started (1s)
	time.Sleep(1 * time.Second)
	// Cancel the operation via ShouldCancel or ShouldDelete
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)

	waitChan := make(chan struct{})
	go func() {
		completionWg.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
	case <-time.After(10 * time.Second):
		t.Fatalf("Test timed out waiting for operation completion")
	}

	close(opsChan)
	err := <-doneChan

	// THEN
	require.NoError(t, err, "expected consumer to stop without error")

	mockFSMUpdater.AssertExpectations(t)
	mockReplicaCopier.AssertExpectations(t)
}

func TestConsumerOpDuplication(t *testing.T) {
	// GIVEN
	logger, _ := logrustest.NewNullLogger()
	mockFSMUpdater := types.NewMockFSMUpdater(t)
	mockReplicaCopier := types.NewMockReplicaCopier(t)
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)
	schemaReader := schemaManager.NewSchemaReader()
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)

	var completionWg sync.WaitGroup

	metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			require.Equal(t, "node2", node, "invalid node in prepare processing callback")
		}).
		WithOpPendingCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in pending op callback")
		}).
		WithOpSkippedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in skipped op callback")
			completionWg.Done()
		}).
		WithOpStartCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in start op callback")
		}).
		WithOpCompleteCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in complete op callback")
			completionWg.Done()
		}).
		WithOpFailedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in failed op callback")
		}).
		WithOpCancelledCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in cancelled op callback")
		}).
		Build()

	consumer := replication.NewCopyOpConsumer(
		logger,
		mockFSMUpdater,
		mockReplicaCopier,
		"node2",
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		time.Second*10,
		1,
		runtime.NewDynamicValue(time.Second*100),
		metricsCallbacks,
		schemaReader,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opsChan := make(chan replication.ShardReplicationOpAndStatus, 1)
	doneChan := make(chan error, 1)

	// WHEN
	go func() {
		doneChan <- consumer.Consume(ctx, opsChan)
	}()

	mockFSMUpdater.EXPECT().
		WaitForUpdate(mock.Anything, mock.Anything).
		Return(nil)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.FINALIZING, nil)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.READY, nil)
	mockFSMUpdater.EXPECT().
		ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(1), api.READY).
		Return(nil)
	mockFSMUpdater.EXPECT().
		ReplicationAddReplicaToShard(mock.Anything, "TestCollection", "shard1", "node2", uint64(1)).
		Return(uint64(1), nil)
	mockReplicaCopier.EXPECT().
		LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	mockReplicaCopier.EXPECT().
		AsyncReplicationStatus(mock.Anything, "node1", "node2", "TestCollection", "shard1").
		RunAndReturn(func(context.Context, string, string, string, string) (models.AsyncReplicationStatus, error) {
			time.Sleep(5 * time.Second)
			return models.AsyncReplicationStatus{
				ObjectsPropagated:       0,
				StartDiffTimeUnixMillis: time.Now().Add(200 * time.Second).UnixMilli(),
			}, nil
		})
	mockReplicaCopier.EXPECT().
		AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockReplicaCopier.EXPECT().
		RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).Return(nil)
	mockReplicaCopier.EXPECT().
		InitAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").
		Return(nil)
	mockReplicaCopier.EXPECT().
		RevertAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").Return(nil)
	mockFSMUpdater.EXPECT().
		SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(1), nil)

	op := replication.NewShardReplicationOp(1, "node1", "node2", "TestCollection", "shard1", api.COPY)
	status := replication.NewShardReplicationStatus(api.FINALIZING)

	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	completionWg.Add(1)

	// Send the same operation again to make sure it isn't reprocessed after a state change
	// as mocked in the above expectations
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	completionWg.Add(1)

	waitChan := make(chan struct{})
	go func() {
		completionWg.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
	case <-time.After(30 * time.Second):
		t.Fatalf("Test timed out waiting for operation completion")
	}

	close(opsChan)
	err := <-doneChan

	// THEN
	require.NoError(t, err, "expected consumer to stop without error")

	mockFSMUpdater.AssertExpectations(t)
	mockReplicaCopier.AssertExpectations(t)
}

func TestConsumerOpSkip(t *testing.T) {
	// GIVEN
	logger, _ := logrustest.NewNullLogger()
	mockFSMUpdater := types.NewMockFSMUpdater(t)
	mockReplicaCopier := types.NewMockReplicaCopier(t)
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)
	schemaReader := schemaManager.NewSchemaReader()
	schemaManager.AddClass(
		buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)

	var completionWg sync.WaitGroup

	metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			require.Equal(t, "node2", node, "invalid node in prepare processing callback")
		}).
		WithOpPendingCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in pending op callback")
		}).
		WithOpSkippedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in skipped op callback")
			completionWg.Done()
		}).
		WithOpStartCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in start op callback")
		}).
		WithOpCompleteCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in complete op callback")
			completionWg.Done()
		}).
		WithOpFailedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in failed op callback")
		}).
		WithOpCancelledCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in cancelled op callback")
		}).
		Build()

	consumer := replication.NewCopyOpConsumer(
		logger,
		mockFSMUpdater,
		mockReplicaCopier,
		"node2",
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		time.Second*10,
		3,
		runtime.NewDynamicValue(time.Second*100),
		metricsCallbacks,
		schemaReader,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opsChan := make(chan replication.ShardReplicationOpAndStatus, 4)
	doneChan := make(chan error, 1)

	// WHEN
	go func() {
		doneChan <- consumer.Consume(ctx, opsChan)
	}()

	mockFSMUpdater.EXPECT().
		WaitForUpdate(mock.Anything, mock.Anything).
		Return(nil)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.FINALIZING, nil)
	mockFSMUpdater.EXPECT().
		ReplicationGetReplicaOpStatus(mock.Anything, uint64(1)).
		Return(api.READY, nil)
	mockReplicaCopier.EXPECT().
		LoadLocalShard(mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	mockFSMUpdater.EXPECT().
		ReplicationUpdateReplicaOpStatus(mock.Anything, uint64(1), api.READY).
		Return(nil)
	mockFSMUpdater.EXPECT().
		ReplicationAddReplicaToShard(mock.Anything, "TestCollection", "shard1", "node2", uint64(1)).
		Return(uint64(1), nil)
	mockReplicaCopier.EXPECT().
		AsyncReplicationStatus(mock.Anything, "node1", "node2", "TestCollection", "shard1").
		RunAndReturn(func(context.Context, string, string, string, string) (models.AsyncReplicationStatus, error) {
			time.Sleep(5 * time.Second)
			return models.AsyncReplicationStatus{
				ObjectsPropagated:       0,
				StartDiffTimeUnixMillis: time.Now().Add(200 * time.Second).UnixMilli(),
			}, nil
		})
	mockReplicaCopier.EXPECT().
		AddAsyncReplicationTargetNode(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockReplicaCopier.EXPECT().
		RemoveAsyncReplicationTargetNode(mock.Anything, mock.Anything).Return(nil)
	mockReplicaCopier.EXPECT().
		InitAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").
		Return(nil)
	mockReplicaCopier.EXPECT().
		RevertAsyncReplicationLocally(mock.Anything, "TestCollection", "shard1").Return(nil)
	mockFSMUpdater.EXPECT().
		SyncShard(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(1), nil)
	op := replication.NewShardReplicationOp(1, "node1", "node2", "TestCollection", "shard1", api.COPY)
	status := replication.NewShardReplicationStatus(api.FINALIZING)

	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	completionWg.Add(1)

	// Send the same operation again twice to make sure it is skipped
	opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
	completionWg.Add(1)

	waitChan := make(chan struct{})
	go func() {
		completionWg.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
	case <-time.After(30 * time.Second):
		t.Fatalf("Test timed out waiting for operation completion")
	}

	close(opsChan)
	err := <-doneChan

	// THEN
	require.NoError(t, err, "expected consumer to stop without error")

	mockFSMUpdater.AssertExpectations(t)
	mockReplicaCopier.AssertExpectations(t)
}

func TestConsumerShutdown(t *testing.T) {
	// GIVEN
	logger, _ := logrustest.NewNullLogger()
	mockFSMUpdater := types.NewMockFSMUpdater(t)
	mockReplicaCopier := types.NewMockReplicaCopier(t)
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
	schemaReader := schemaManager.NewSchemaReader()

	var completionWg sync.WaitGroup
	metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
		WithPrepareProcessing(func(node string) {
			require.Equal(t, "node2", node, "invalid node in prepare processing callback")
		}).
		WithOpPendingCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in pending op callback")
		}).
		WithOpSkippedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in skipped op callback")
		}).
		WithOpStartCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in start op callback")
		}).
		WithOpCompleteCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in complete op callback")
		}).
		WithOpFailedCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in failed op callback")
			completionWg.Done()
		}).
		WithOpCancelledCallback(func(node string) {
			require.Equal(t, "node2", node, "invalid node in cancelled op callback")
		}).
		Build()

	consumer := replication.NewCopyOpConsumer(
		logger,
		mockFSMUpdater,
		mockReplicaCopier,
		"node2",
		&backoff.StopBackOff{},
		replication.NewOpsCache(),
		time.Second*30,
		5,
		runtime.NewDynamicValue(time.Second*100),
		metricsCallbacks,
		schemaReader,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opsChan := make(chan replication.ShardReplicationOpAndStatus, 16)
	doneChan := make(chan error, 1)

	// WHEN
	go func() {
		doneChan <- consumer.Consume(ctx, opsChan)
	}()

	mockReplicaCopier.EXPECT().
		CopyReplicaFiles(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, sourceNode string, collectionName string, shardName string, schemaVersion uint64) error {
			// Simulate a long-running operation that checks for cancellation every loop
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(1 * time.Second)
				// Simulate a long-running operation
			}
		}).
		Times(5)

	// Add five long running ops to the consumer
	for i := 0; i < 5; i++ {
		mockFSMUpdater.EXPECT().
			ReplicationGetReplicaOpStatus(mock.Anything, uint64(i)).
			Return(api.HYDRATING, nil)
		op := replication.NewShardReplicationOp(uint64(i), "node1", "node2", "TestCollection", "test-shard", api.COPY)
		status := replication.NewShardReplicationStatus(api.HYDRATING)
		opsChan <- replication.NewShardReplicationOpAndStatus(op, status)
		completionWg.Add(1)
	}
	// Wait for a second for the ops to start processing
	time.Sleep(1 * time.Second)
	// Shutdown the consumer
	close(opsChan)

	waitChan := make(chan struct{})
	go func() {
		completionWg.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
	case <-time.After(10 * time.Second):
		t.Fatalf("Test timed out waiting for operation completion")
	}

	err := <-doneChan

	// THEN
	require.NoError(t, err, "expected consumer to stop without error")

	mockFSMUpdater.AssertExpectations(t)
	mockReplicaCopier.AssertExpectations(t)
}

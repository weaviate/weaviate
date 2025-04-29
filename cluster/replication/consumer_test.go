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

	"github.com/weaviate/weaviate/cluster/replication/types"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
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
		mockTimeProvider := replication.NewMockTimeProvider(t)

		opId, err := randInt(t, 100, 200)
		require.NoError(t, err, "error generating random operation id")

		mockFSMUpdater.On("ReplicationUpdateReplicaOpStatus", uint64(opId), api.HYDRATING).Return(nil)
		mockReplicaCopier.On("CopyReplica",
			mock.Anything,
			"node1",
			"TestCollection",
			mock.Anything,
		).Once().Return(nil)
		mockFSMUpdater.On("AddReplicaToShard",
			mock.Anything,
			"TestCollection",
			mock.Anything,
			"node2",
		).Once().Return(uint64(0), nil)
		mockTimeProvider.On("Now").Return(time.Now())

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
			func(op replication.ShardReplicationOp) bool {
				return false
			},
			mockFSMUpdater,
			mockReplicaCopier,
			mockTimeProvider,
			"node2",
			&backoff.StopBackOff{},
			time.Second*10,
			1,
			metricsCallbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOp, 1)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		opsChan <- replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "TestCollection", "test-shard")
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
		mockTimeProvider := replication.NewMockTimeProvider(t)

		opId, err := randInt(t, 100, 200)
		require.NoError(t, err, "error generating random operation id")

		mockFSMUpdater.On("ReplicationUpdateReplicaOpStatus", uint64(opId), api.HYDRATING).Return(nil)
		mockReplicaCopier.On("CopyReplica",
			mock.Anything,
			"node1",
			"TestCollection",
			"test-shard",
		).Once().Return(errors.New("simulated copy failure"))
		mockTimeProvider.On("Now").Return(time.Now())

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
			func(op replication.ShardReplicationOp) bool {
				return false
			},
			mockFSMUpdater,
			mockReplicaCopier,
			mockTimeProvider,
			"node2",
			&backoff.StopBackOff{}, // No retries for test
			time.Second*10,
			1,
			metricsCallbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOp, 1)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		opsChan <- replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "TestCollection", "test-shard")
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
		require.Equal(t, 0, completedCallbacksCounter, "Complete callback should not be called for failed operation")
		require.Equal(t, 1, failedCallbacksCounter, "Failed callback should be called for failed operation")
		mockFSMUpdater.AssertExpectations(t)
		mockReplicaCopier.AssertExpectations(t)
	})

	t.Run("multiple random concurrent operations should be tracked correctly", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockFSMUpdater := types.NewMockFSMUpdater(t)
		mockReplicaCopier := types.NewMockReplicaCopier(t)
		mockTimeProvider := replication.NewMockTimeProvider(t)

		randomNumberOfOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")

		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random op id start")
		for i := 0; i < randomNumberOfOps; i++ {
			opId := uint64(randomStartOpId + i)
			mockFSMUpdater.On("ReplicationUpdateReplicaOpStatus", opId, api.HYDRATING).Return(nil)
			mockReplicaCopier.On("CopyReplica", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			mockFSMUpdater.On("AddReplicaToShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(uint64(i), nil)
		}

		mockTimeProvider.On("Now").Return(time.Now())

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
			func(op replication.ShardReplicationOp) bool {
				return false
			},
			mockFSMUpdater,
			mockReplicaCopier,
			mockTimeProvider,
			"node2",
			&backoff.StopBackOff{},
			time.Second*10,
			1,
			metricsCallbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOp, randomNumberOfOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		for i := 0; i < randomNumberOfOps; i++ {
			shard := fmt.Sprintf("shard-%d", i)
			opsChan <- replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", "node2", "TestCollection", shard)
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
		mockTimeProvider := replication.NewMockTimeProvider(t)

		totalOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")

		mockTimeProvider.On("Now").Return(time.Now()).Maybe()

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
			func(op replication.ShardReplicationOp) bool {
				// Skip all operations
				return true
			},
			mockFSMUpdater,
			mockReplicaCopier,
			mockTimeProvider,
			"node2",
			&backoff.StopBackOff{},
			time.Second*10,
			1,
			callbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOp, totalOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random op id start")

		for i := 0; i < totalOps; i++ {
			shard := fmt.Sprintf("shard-%d", i)
			opsChan <- replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", "node2", "TestCollection", shard)
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
		mockTimeProvider := replication.NewMockTimeProvider(t)

		totalOps, err := randInt(t, 10, 20)
		require.NoError(t, err, "error while generating random number of operations")

		mockTimeProvider.On("Now").Return(time.Now()).Maybe()

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

		skipMap := make(map[uint64]bool)

		randomStartOpId, err := randInt(t, 1000, 2000)
		require.NoError(t, err, "error while generating random op id start")

		for i := 0; i < totalOps; i++ {
			opID := uint64(randomStartOpId + i)
			skip := randomBoolean(t)
			skipMap[opID] = skip
			if !skip {
				mockFSMUpdater.On("ReplicationUpdateReplicaOpStatus", opID, api.HYDRATING).Return(nil)
				mockReplicaCopier.On("CopyReplica", mock.Anything, "node1", "TestCollection", mock.Anything).Return(nil)
				mockFSMUpdater.On("AddReplicaToShard", mock.Anything, "TestCollection", mock.Anything, "node2").Return(uint64(i), nil)
				completionWg.Add(1)
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
			func(op replication.ShardReplicationOp) bool {
				return skipMap[op.ID]
			},
			mockFSMUpdater,
			mockReplicaCopier,
			mockTimeProvider,
			"node2",
			&backoff.StopBackOff{},
			time.Second*10,
			1,
			callbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opsChan := make(chan replication.ShardReplicationOp, totalOps)
		doneChan := make(chan error, 1)

		// WHEN
		go func() {
			doneChan <- consumer.Consume(ctx, opsChan)
		}()

		for i := 0; i < totalOps; i++ {
			shard := fmt.Sprintf("shard-%d", i)
			opsChan <- replication.NewShardReplicationOp(uint64(randomStartOpId+i), "node1", "node2", "TestCollection", shard)
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

		expectedSkipped := 0
		expectedStarted := 0
		expectedCompleted := 0

		for _, skipped := range skipMap {
			if skipped {
				expectedSkipped++
			} else {
				expectedStarted++
				expectedCompleted++
			}
		}

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

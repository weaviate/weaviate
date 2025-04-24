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
	"github.com/stretchr/testify/assert"
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
			pendingCallbacksCounter   int
			startedCallbacksCounter   int
			completedCallbacksCounter int
			failedCallbacksCounter    int
			completionWg              sync.WaitGroup
		)
		completionWg.Add(1)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				require.Equal(t, "node2", node, "invalid node in pending op callback")
				pendingCallbacksCounter++
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
		assert.Equal(t, 1, pendingCallbacksCounter, "Pending callback should be called")
		assert.Equal(t, 1, startedCallbacksCounter, "Start callback should be called")
		assert.Equal(t, 1, completedCallbacksCounter, "Complete callback should be called")
		assert.Equal(t, 0, failedCallbacksCounter, "Failed callback should be called for failed operation")
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
			pendingCallbacksCounter   int
			startedCallbacksCounter   int
			completedCallbacksCounter int
			failedCallbacksCounter    int
			completionWg              sync.WaitGroup
		)
		completionWg.Add(1)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				assert.Equal(t, "node2", node)
				pendingCallbacksCounter++
			}).
			WithOpStartCallback(func(node string) {
				assert.Equal(t, "node2", node)
				startedCallbacksCounter++
			}).
			WithOpCompleteCallback(func(node string) {
				assert.Equal(t, "node2", node)
				completedCallbacksCounter++
			}).
			WithOpFailedCallback(func(node string) {
				assert.Equal(t, "node2", node)
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
		assert.Equal(t, 1, pendingCallbacksCounter, "Pending callback should be called")
		assert.Equal(t, 1, startedCallbacksCounter, "Start callback should be called")
		assert.Equal(t, 0, completedCallbacksCounter, "Complete callback should not be called for failed operation")
		assert.Equal(t, 1, failedCallbacksCounter, "Failed callback should be called for failed operation")
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
			mutex         sync.Mutex
			pendingCount  int
			startCount    int
			completeCount int
			completionWg  sync.WaitGroup
		)
		completionWg.Add(randomNumberOfOps)

		metricsCallbacks := metrics.NewReplicationEngineOpsCallbacksBuilder().
			WithOpPendingCallback(func(node string) {
				mutex.Lock()
				pendingCount++
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
		require.NoError(t, err)
		mutex.Lock()
		assert.Equal(t, randomNumberOfOps, pendingCount, "Pending callback should be called for each operation")
		assert.Equal(t, randomNumberOfOps, startCount, "Start callback should be called for each operation")
		assert.Equal(t, randomNumberOfOps, completeCount, "Complete callback should be called for each operation")
		mutex.Unlock()
	})
}

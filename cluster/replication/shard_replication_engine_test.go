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
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
)

func TestShardReplicationEngine(t *testing.T) {
	t.Run("replication engine cancel graceful handling", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStartedChan := make(chan struct{})
		consumerStartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1, 1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should report not running before start")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(ctx)
			require.ErrorIs(t, engineStartErr, context.Canceled)
		}()

		<-producerStartedChan
		<-consumerStartedChan

		require.True(t, engine.IsRunning(), "engine should be running after producer and consumer started")

		// WHEN
		cancel()

		wg.Wait()

		// THEN
		require.ErrorIs(t, engineStartErr, context.Canceled, "engine should return context.Canceled")
		require.False(t, engine.IsRunning(), "engine should not be running after context cancellation")
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine consumer failure", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Return(errors.New("unexpected consumer error"))

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should report not running before start")

		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(context.Background())
		}()

		// Wait for producer but not for the consumer which will err
		<-producerStartedChan

		// Wait for engine start
		wg.Wait()

		// THEN
		require.Error(t, engineStartErr)
		require.Contains(t, engineStartErr.Error(), "unexpected consumer error")
		require.False(t, engine.IsRunning(), "engine should report not running after consumer error")
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine producer failure", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		consumerStartedChan := make(chan struct{})

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Return(context.Canceled)

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Return(errors.New("unexpected producer error"))

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should report not running before start")

		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(context.Background())
		}()

		// Wait for consumer but not for the producer which will err
		<-consumerStartedChan

		wg.Wait()

		// THEN
		require.Error(t, engineStartErr)
		require.Contains(t, engineStartErr.Error(), "unexpected producer error")
		require.False(t, engine.IsRunning(), "engine should not be running after consumer error")
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine stop graceful handling", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStartedChan := make(chan struct{})
		consumerStartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{} // producer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{} // consumer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()
		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should report not running before start")

		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(context.Background())
		}()

		// Wait for producer and consumer to start
		<-producerStartedChan
		<-consumerStartedChan

		// THEN
		require.True(t, engine.IsRunning(), "engine should be running before Stop")

		engine.Stop() // stop while the engine is still running
		wg.Wait()

		// THEN
		require.NoError(t, engineStartErr, "engine should stop without error")
		require.False(t, engine.IsRunning(), "engine should not be running after stop")

		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine started twice", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStarted := make(chan struct{})
		consumerStarted := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStarted <- struct{}{} // producer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStarted <- struct{}{} // consumer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()
		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should not be running before start")

		var wg sync.WaitGroup
		wg.Add(1)
		var firstEngineStartErr error
		go func() {
			defer wg.Done()
			firstEngineStartErr = engine.Start(context.Background())
		}()

		// Wait for producer and consumer to start
		<-producerStarted
		<-consumerStarted

		require.True(t, engine.IsRunning(), "engine should be running after first Start")

		secondEngineStartErr := engine.Start(context.Background())

		// THEN
		require.NoError(t, secondEngineStartErr, "second start should return nil when already running")
		require.True(t, engine.IsRunning(), "engine should still be running after second Start")

		engine.Stop()
		wg.Wait()

		require.NoError(t, firstEngineStartErr, "first start should complete without error")
		require.False(t, engine.IsRunning(), "engine should no longer be running after Stop")

		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("two replication engines run independently on different nodes", func(t *testing.T) {
		// GIVEN
		mockProducer1 := replication.NewMockOpProducer(t)
		mockConsumer1 := replication.NewMockOpConsumer(t)
		mockProducer2 := replication.NewMockOpProducer(t)
		mockConsumer2 := replication.NewMockOpConsumer(t)

		producer1StartedChan := make(chan struct{})
		consumer1StartedChan := make(chan struct{})
		producer2StartedChan := make(chan struct{})
		consumer2StartedChan := make(chan struct{})

		mockProducer1.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producer1StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer1.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumer1StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockProducer2.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producer2StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer2.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumer2StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()
		engine1 := replication.NewShardReplicationEngine(logger,
			"node1",
			mockProducer1,
			mockConsumer1,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		engine2 := replication.NewShardReplicationEngine(logger,
			"node2",
			mockProducer2,
			mockConsumer2,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine1.IsRunning(), "engine1 should not be running before start")
		require.False(t, engine2.IsRunning(), "engine2 should not be running before start")

		// WHEN
		var wg sync.WaitGroup
		wg.Add(2)

		var engine1StartErr error
		var engine2StartErr error

		go func() {
			defer wg.Done()
			engine1StartErr = engine1.Start(context.Background())
		}()

		go func() {
			defer wg.Done()
			engine2StartErr = engine2.Start(context.Background())
		}()

		<-producer1StartedChan
		<-consumer1StartedChan
		<-producer2StartedChan
		<-consumer2StartedChan

		// THEN
		require.True(t, engine1.IsRunning(), "engine1 should be running")
		require.True(t, engine2.IsRunning(), "engine2 should be running")

		engine1.Stop()
		engine2.Stop()

		// Wait for both engines to complete
		wg.Wait()

		require.NoError(t, engine1StartErr, "engine1 should stop without error")
		require.NoError(t, engine2StartErr, "engine2 should stop without error")
		require.False(t, engine1.IsRunning(), "engine1 should not be running after stop")
		require.False(t, engine2.IsRunning(), "engine2 should not be running after stop")
		mockProducer1.AssertExpectations(t)
		mockConsumer1.AssertExpectations(t)
		mockProducer2.AssertExpectations(t)
		mockConsumer2.AssertExpectations(t)
	})

	t.Run("replication engine stop is idempotent", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStarted := make(chan struct{})
		consumerStarted := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStarted <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStarted <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()
		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should not be running before start")

		// WHEN
		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(context.Background())
		}()

		<-producerStarted
		<-consumerStarted
		require.True(t, engine.IsRunning(), "engine should report running after start")

		// THEN
		engine.Stop()
		engine.Stop() // second stop should be idempotent (no-op)
		wg.Wait()

		// THEN
		require.NoError(t, engineStartErr, "engine should stop without error")
		require.False(t, engine.IsRunning(), "engine should not be running after stop")

		engine.Stop() // third stop after already stopped is still idempotent (no-op)
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine start-stop-start-stop works correctly", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		// First start/stop cycle
		producer1StartedChan := make(chan struct{})
		consumer1StartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producer1StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumer1StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		// Second start/stop cycle
		producer2StartedChan := make(chan struct{})
		consumer2StartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producer2StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumer2StartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		require.False(t, engine.IsRunning(), "engine should not be running before start")

		// WHEN (start first cycle)
		var wg sync.WaitGroup
		wg.Add(1)
		var firstCycleErr error

		go func() {
			defer wg.Done()
			firstCycleErr = engine.Start(context.Background())
		}()

		// Wait for producer and consumer to start
		<-producer1StartedChan
		<-consumer1StartedChan

		require.True(t, engine.IsRunning(), "engine should be running in first cycle")

		engine.Stop()

		// Wait for first cycle to complete
		wg.Wait()

		require.NoError(t, firstCycleErr, "first cycle should complete without error")
		require.False(t, engine.IsRunning(), "engine should not be running after first stop")

		// WHEN (start second cycle)
		wg.Add(1)
		var secondCycleErr error

		go func() {
			defer wg.Done()
			secondCycleErr = engine.Start(context.Background())
		}()

		// Wait for producer and consumer to start again
		<-producer2StartedChan
		<-consumer2StartedChan

		require.True(t, engine.IsRunning(), "engine should be running in second cycle")

		engine.Stop()

		// Wait for second cycle to complete
		wg.Wait()

		require.NoError(t, secondCycleErr, "second cycle should complete without error")
		require.False(t, engine.IsRunning(), "engine should not be running after second stop")
		mockProducer.AssertNumberOfCalls(t, "Produce", 2)
		mockConsumer.AssertNumberOfCalls(t, "Consume", 2)
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("replication engine supports multiple start/stop cycles", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		require.False(t, engine.IsRunning(), "engine should not be running before start")

		// Run multiple start/stop cycles
		cycles, err := randInt(t, 5, 10)
		require.NoError(t, err, "unexpected error when generating rando value")

		for cycle := 1; cycle <= cycles; cycle++ {
			producerStartedChan := make(chan struct{})
			consumerStartedChan := make(chan struct{})

			mockProducer.EXPECT().
				Produce(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
					producerStartedChan <- struct{}{}
					<-ctx.Done()
				}).
				Once().
				Return(context.Canceled)

			mockConsumer.EXPECT().
				Consume(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
					consumerStartedChan <- struct{}{}
					<-ctx.Done()
				}).
				Once().
				Return(context.Canceled)

			var wg sync.WaitGroup
			wg.Add(1)
			var cycleErr error

			go func() {
				defer wg.Done()
				cycleErr = engine.Start(context.Background())
			}()

			// Wait for producer and consumer to start
			<-producerStartedChan
			<-consumerStartedChan

			require.True(t, engine.IsRunning(), "engine should be running in cycle %d", cycle)

			engine.Stop()

			// Wait for cycle to complete
			wg.Wait()

			require.NoError(t, cycleErr, "cycle %d should complete without error", cycle)
			require.False(t, engine.IsRunning(), "engine should not be running after cycle %d", cycle)
			mockProducer.AssertExpectations(t)
			mockConsumer.AssertExpectations(t)
		}
		mockProducer.AssertNumberOfCalls(t, "Produce", cycles)
		mockConsumer.AssertNumberOfCalls(t, "Consume", cycles)
	})

	t.Run("replication engine stop without start is a no-op", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		logger, _ := logrustest.NewNullLogger()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		require.False(t, engine.IsRunning(), "engine should not be running initially")

		// WHEN
		engine.Stop() // Stop without ever starting

		// THEN
		require.False(t, engine.IsRunning(), "engine should still not be running after Stop")
		mockProducer.AssertNotCalled(t, "Produce")
		mockConsumer.AssertNotCalled(t, "Consume")
	})

	t.Run("replication engine custom op channel size", func(t *testing.T) {
		// GIVEN
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		producerStartedChan := make(chan struct{})
		consumerStartedChan := make(chan struct{})

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{} // producer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{} // consumer started event
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		logger, _ := logrustest.NewNullLogger()
		randomOpBufferSize, err := randInt(t, 16, 128)
		require.NoError(t, err, "error generating random operation buffer")
		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			randomOpBufferSize,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)
		require.False(t, engine.IsRunning(), "engine should report not running before start")

		// WHEN
		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(context.Background())
		}()

		// Wait for producer and consumer to start
		<-producerStartedChan
		<-consumerStartedChan

		// THEN
		require.True(t, engine.IsRunning(), "engine should be running after start")
		require.Equal(t, randomOpBufferSize, engine.OpChannelCap(), "channel capacity should match the configured size")
		require.Equal(t, 0, engine.OpChannelLen(), "channel length should be 0 when no ops are queued")

		engine.Stop()
		wg.Wait()

		require.NoError(t, engineStartErr, "engine should stop without error")
		require.False(t, engine.IsRunning(), "engine should not be running after stop")
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("producer creates and consumer processes random operations", func(t *testing.T) {
		logger, _ := logrustest.NewNullLogger()
		opsCount, err := randInt(t, 20, 30)
		require.NoError(t, err, "error generating random operation count")

		producedOpsChan := make(chan replication.ShardReplicationOp, opsCount)
		consumedOpsChan := make(chan uint64, opsCount)
		completedOpsChan := make(chan uint64, opsCount)
		doneChan := make(chan struct{})

		opIds, err := randomOpIds(t, opsCount)
		require.NoError(t, err, "error generating operation IDs")

		var producerWg sync.WaitGroup
		producerWg.Add(1)

		mockProducer := replication.NewMockOpProducer(t)
		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				defer producerWg.Done()
				for _, opId := range opIds {
					randomSleepTime, e := randInt(t, 10, 50)
					require.NoErrorf(t, e, "error generating random sleep time")
					time.Sleep(time.Millisecond * time.Duration(randomSleepTime))
					op := replication.NewShardReplicationOp(opId, "node1", "node2", "TestCollection", "shard1", api.COPY)

					select {
					case out <- replication.NewShardReplicationOpAndStatus(op, replication.NewShardReplicationStatus(api.REGISTERED)):
						producedOpsChan <- op
					case <-ctx.Done():
						return
					}
				}
			}).
			Return(nil)

		mockConsumer := replication.NewMockOpConsumer(t)
		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				processedOps := 0
				for {
					select {
					case <-ctx.Done():
						return
					case op, ok := <-in:
						if !ok {
							return
						}

						randomSleepTime, e := randInt(t, 10, 50)
						require.NoErrorf(t, e, "error generating random sleep time")
						time.Sleep(time.Millisecond * time.Duration(randomSleepTime))

						consumedOpsChan <- op.Op.ID
						completedOpsChan <- op.Op.ID

						processedOps++
						if processedOps == opsCount {
							close(doneChan)
							return
						}
					}
				}
			}).
			Return(nil)

		engine := replication.NewShardReplicationEngine(
			logger,
			"node2",
			mockProducer,
			mockConsumer,
			opsCount,
			1,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		var engineStartErr error
		go func() {
			defer wg.Done()
			engineStartErr = engine.Start(ctx)
		}()

		var producedOps, consumedOps, completedOps []uint64

		select {
		case <-doneChan:
		case <-time.After(1 * time.Minute): // this is here just to prevent the test from running indefinitely for too long
			t.Fatal("timeout waiting for operations to complete")
		}

		engine.Stop()
		producerWg.Wait()

		close(producedOpsChan)
		close(consumedOpsChan)
		close(completedOpsChan)

		for op := range producedOpsChan {
			producedOps = append(producedOps, op.ID)
		}
		for opID := range consumedOpsChan {
			consumedOps = append(consumedOps, opID)
		}
		for opID := range completedOpsChan {
			completedOps = append(completedOps, opID)
		}

		engine.Stop()
		wg.Wait()

		require.NoError(t, engineStartErr, "engine should start without error")
		require.Equal(t, opsCount, len(producedOps), "all operations should be produced")
		require.Equal(t, opsCount, len(consumedOps), "all operations should be consumed")
		require.Equal(t, opsCount, len(completedOps), "all operations should be completed")
		require.ElementsMatch(t, producedOps, consumedOps, "produced and consumed operations should match")
		require.ElementsMatch(t, producedOps, completedOps, "produced and completed operations should match")
		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("producer error during operation is handled gracefully and engine can restart", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()

		producerStartedChan := make(chan struct{}, 1)
		producerErrorChan := make(chan struct{}, 1)
		engineStoppedChan := make(chan struct{}, 1)
		producerRestartChan := make(chan struct{}, 1)
		consumerStartedChan := make(chan struct{}, 1)

		opId, err := randInt(t, 1000, 2000)
		require.NoErrorf(t, err, "error generating random op id")
		expectedErr := errors.New(fmt.Sprintf("producer error after sending operation %d", uint64(opId)))

		// First attempt - producer sends one operation then errors
		mockProducer := replication.NewMockOpProducer(t)
		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{}

				op := replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "collection1", "shard1", api.COPY)
				select {
				case <-ctx.Done():
					return
				case out <- replication.NewShardReplicationOpAndStatus(op, replication.NewShardReplicationStatus(api.REGISTERED)):
					// Error after sending a valid op
					producerErrorChan <- struct{}{}
				}
			}).
			Once().
			Return(expectedErr)

		// Second attempt - producer runs normally until canceled
		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerRestartChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		// Consumer runs normally processing operations
		mockConsumer := replication.NewMockOpConsumer(t)
		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{}
				<-ctx.Done()
			}).
			Return(context.Canceled).
			Twice()

		randomBufferSize, err := randInt(t, 10, 20)
		require.NoErrorf(t, err, "error generating random buffer size")

		randomWorkers, err := randInt(t, 2, 5)
		require.NoErrorf(t, err, "error generating random workers")

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			randomBufferSize,
			randomWorkers,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		// WHEN - First attempt fails due to producer facing an unexpected error
		var wg sync.WaitGroup
		wg.Add(1)
		var firstEngineStartErr error

		go func() {
			defer wg.Done()
			firstEngineStartErr = engine.Start(context.Background())
			// Wait for the engine to stop after the producer fails
			engineStoppedChan <- struct{}{}
		}()

		<-producerStartedChan
		<-consumerStartedChan
		<-producerErrorChan

		// Wait for engine to stop as a result of producer error
		<-engineStoppedChan
		wg.Wait()

		// THEN - First attempt should have failed with expected error
		require.Error(t, firstEngineStartErr, "first attempt should return error")
		require.Contains(t, firstEngineStartErr.Error(), expectedErr.Error(),
			"error should contain expected message")
		require.False(t, engine.IsRunning(), "engine should not be running after error")

		// WHEN - Second attempt
		wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())

		var engineRestartErr error
		go func() {
			defer wg.Done()
			engineRestartErr = engine.Start(ctx)
		}()

		// Wait for producer and consumer to start again after restarting the engine
		<-producerRestartChan
		<-consumerStartedChan

		// THEN
		require.NoError(t, engineRestartErr, "engine should restart after error")
		require.True(t, engine.IsRunning(), "engine should be running on second attempt")

		cancel()
		wg.Wait()

		require.False(t, engine.IsRunning(), "engine should not be running after stop")

		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})

	t.Run("consumer error during operation is handled gracefully and engine can restart", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()

		producerStartedChan := make(chan struct{}, 1)
		consumerStartedChan := make(chan struct{}, 1)
		consumerErrorChan := make(chan struct{}, 1)
		engineStoppedChan := make(chan struct{}, 1)
		producerRestartChan := make(chan struct{}, 1)
		consumerRestartChan := make(chan struct{}, 1)

		opId, err := randInt(t, 1000, 2000)
		require.NoErrorf(t, err, "error generating random op id")
		expectedErr := errors.New(fmt.Sprintf("consumer error while processing operation %d", opId))

		mockProducer := replication.NewMockOpProducer(t)

		// First attempt - producer sends operation and waits for cancellation
		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerStartedChan <- struct{}{}

				op := replication.NewShardReplicationOp(uint64(opId), "node1", "node2", "collection1", "shard1", api.COPY)
				select {
				case <-ctx.Done():
					return
				case out <- replication.NewShardReplicationOpAndStatus(op, replication.NewShardReplicationStatus(api.REGISTERED)):
				}

				// Wait for cancellation
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		// Second attempt - producer runs normally again after restarting the engine
		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, out chan<- replication.ShardReplicationOpAndStatus) {
				producerRestartChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		mockConsumer := replication.NewMockOpConsumer(t)

		// First consumer attempt - fails with error
		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerStartedChan <- struct{}{}

				// Process one operation then fail
				select {
				case <-ctx.Done():
					return
				case <-in:
					consumerErrorChan <- struct{}{}
					return
				}
			}).
			Once().
			Return(expectedErr)

		// Second consumer attempt - succeeds after restarting the engine
		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Run(func(ctx context.Context, in <-chan replication.ShardReplicationOpAndStatus) {
				consumerRestartChan <- struct{}{}
				<-ctx.Done()
			}).
			Once().
			Return(context.Canceled)

		randomBufferSize, err := randInt(t, 10, 20)
		require.NoErrorf(t, err, "error generating random buffer size")

		randomWorkers, err := randInt(t, 2, 5)
		require.NoErrorf(t, err, "error generating random workers")

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			randomBufferSize,
			randomWorkers,
			1*time.Minute,
			metrics.NewReplicationEngineCallbacks(prometheus.NewPedanticRegistry()),
		)

		// WHEN - First attempt fails due to consumer error
		var wg sync.WaitGroup
		wg.Add(1)

		var firstEngineStartErr error
		go func() {
			defer wg.Done()
			firstEngineStartErr = engine.Start(context.Background())
			engineStoppedChan <- struct{}{}
		}()

		<-producerStartedChan
		<-consumerStartedChan
		<-consumerErrorChan

		// Wait for engine to stop as a result of consumer error
		<-engineStoppedChan
		wg.Wait()

		// THEN
		require.Error(t, firstEngineStartErr, "first attempt should return error")
		require.Contains(t, firstEngineStartErr.Error(), expectedErr.Error(),
			"error should contain expected message")
		require.False(t, engine.IsRunning(), "engine should not be running after error")

		wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())

		var engineRestartErr error
		go func() {
			defer wg.Done()
			engineRestartErr = engine.Start(ctx)
		}()

		<-producerRestartChan
		<-consumerRestartChan

		// THEN
		require.True(t, engine.IsRunning(), "engine should be running on second attempt")

		cancel()
		wg.Wait()

		require.ErrorIs(t, engineRestartErr, context.Canceled, "engine should stop with context.Canceled")
		require.False(t, engine.IsRunning(), "engine should not be running after stop")

		mockProducer.AssertExpectations(t)
		mockConsumer.AssertExpectations(t)
	})
}

func TestEngineWithCallbacks(t *testing.T) {
	t.Run("should trigger engine/producer/consumer start and stop callbacks", func(t *testing.T) {
		// GIVEN
		logger, _ := logrustest.NewNullLogger()
		mockProducer := replication.NewMockOpProducer(t)
		mockConsumer := replication.NewMockOpConsumer(t)

		// Use channels to track callback execution
		engineStarted := make(chan struct{}, 1)
		engineStopped := make(chan struct{}, 1)
		producerStarted := make(chan struct{}, 1)
		producerStopped := make(chan struct{}, 1)
		consumerStarted := make(chan struct{}, 1)
		consumerStopped := make(chan struct{}, 1)

		engineStartCount := 0
		engineStopCount := 0
		producerStartCount := 0
		producerStopCount := 0
		consumerStartCount := 0
		consumerStopCount := 0

		callbacks := metrics.NewReplicationEngineCallbacksBuilder().
			WithEngineStartCallback(func(node string) {
				require.Equal(t, "node1", node)
				engineStartCount++
				engineStarted <- struct{}{}
			}).
			WithEngineStopCallback(func(node string) {
				require.Equal(t, "node1", node)
				engineStopCount++
				engineStopped <- struct{}{}
			}).
			WithProducerStartCallback(func(node string) {
				require.Equal(t, "node1", node)
				producerStartCount++
				producerStarted <- struct{}{}
			}).
			WithProducerStopCallback(func(node string) {
				require.Equal(t, "node1", node)
				producerStopCount++
				producerStopped <- struct{}{}
			}).
			WithConsumerStartCallback(func(node string) {
				consumerStartCount++
				require.Equal(t, "node1", node)
				consumerStarted <- struct{}{}
			}).
			WithConsumerStopCallback(func(node string) {
				consumerStopCount++
				require.Equal(t, "node1", node)
				consumerStopped <- struct{}{}
			}).
			Build()

		mockProducer.EXPECT().
			Produce(mock.Anything, mock.Anything).
			Return(nil).Maybe()
		mockConsumer.EXPECT().
			Consume(mock.Anything, mock.Anything).
			Return(nil).Maybe()

		engine := replication.NewShardReplicationEngine(
			logger,
			"node1",
			mockProducer,
			mockConsumer,
			1,
			1,
			1*time.Second,
			callbacks,
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var engineStartErr error
		var engineStartWG sync.WaitGroup
		engineStartWG.Add(1)

		// WHEN
		go func() {
			defer engineStartWG.Done()
			engineStartErr = engine.Start(ctx)
		}()

		// Wait for all start callbacks
		select {
		case <-engineStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for engine start callback")
		}
		select {
		case <-producerStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for producer start callback")
		}
		select {
		case <-consumerStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for consumer start callback")
		}

		engine.Stop()

		// Wait for all stop callbacks
		select {
		case <-engineStopped:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for engine stop callback")
		}
		select {
		case <-producerStopped:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for producer stop callback")
		}
		select {
		case <-consumerStopped:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for consumer stop callback")
		}

		engineStartWG.Wait()

		// THEN
		require.NoErrorf(t, engineStartErr, "engine start should not return error")
		require.Equal(t, engineStartCount, 1, "engine start count should be 1")
		require.Equal(t, engineStopCount, 1, "engine stop count should be 1")
		require.Equal(t, producerStartCount, 1, "producer start count should be 1")
		require.Equal(t, producerStopCount, 1, "producer stop count should be 1")
		require.Equal(t, consumerStartCount, 1, "consumer start count should be 1")
		require.Equal(t, consumerStopCount, 1, "consumer stop count should be 1")
	})
}

func randomOpIds(t *testing.T, count int) ([]uint64, error) {
	t.Helper()
	startId, err := randInt(t, 1000, 10000)
	if err != nil {
		return nil, err
	}

	opIds := make([]uint64, count)
	currId := uint64(startId)

	for i := 0; i < count; i++ {
		opIds[i] = currId
		currId += 1
	}

	return opIds, nil
}

func randInt(t *testing.T, min, max int) (int, error) {
	t.Helper()
	var randValue [1]byte
	_, err := rand.Read(randValue[:])
	if err != nil {
		return 0, err
	}
	return min + int(randValue[0])%(max-min+1), nil
}

func randomBoolean(t *testing.T) bool {
	t.Helper()
	var b [1]byte
	_, err := rand.Read(b[:])
	if err != nil {
		t.Fatalf("failed to generate random boolean: %v", err)
	}
	return b[0]%2 == 0
}

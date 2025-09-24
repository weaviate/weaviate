//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestHandler(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	t.Run("Send", func(t *testing.T) {
		t.Run("send objects using the scheduler", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			// Arrange
			req := &pb.BatchSendRequest{
				StreamId: "test-stream",
				Message: &pb.BatchSendRequest_Objects_{
					Objects: &pb.BatchSendRequest_Objects{
						Values: []*pb.BatchObject{{Collection: "TestClass"}},
					},
				},
			}

			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			defer shutdownCancel()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(shutdownCtx, &sendWg, &streamWg, nil, writeQueues, readQueues, logger)
			var sWg sync.WaitGroup
			batch.StartScheduler(shutdownCtx, &sWg, writeQueues, internalQueue, logger)

			writeQueues.Make(req.StreamId, nil, 0, 0)
			res, err := handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending objects")
			require.Equal(t, int32(10), res.NextBatchSize, "Expected to be told to scale up by an order of magnitude")

			// Verify that the internal queue has the object
			obj := <-internalQueue
			require.NotNil(t, obj, "Expected object to be sent to internal queue")

			// Shutdown
			shutdownCancel()

			_, err = handler.Send(ctx, req)
			require.Equal(t, "grpc shutdown in progress, no more requests are permitted on this node", err.Error(), "Expected error when sending after shutdown")
		})

		t.Run("dynamic batch size calulation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(ctx, &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			// Send 8000 objects
			req := &pb.BatchSendRequest{
				StreamId: StreamId,
				Message: &pb.BatchSendRequest_Objects_{
					Objects: &pb.BatchSendRequest_Objects{},
				},
			}
			for i := 0; i < 8000; i++ {
				req.GetObjects().Values = append(req.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
			}
			res, err := handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending 8000 objects")
			require.Equal(t, int32(799), res.NextBatchSize, "Expected to be told to send 799 objects next")
			require.Equal(t, float32(1.2499998), res.BackoffSeconds, "Expected to be told to backoff by 1.2499998 seconds")

			// Saturate the buffer
			req = &pb.BatchSendRequest{
				StreamId: StreamId,
				Message: &pb.BatchSendRequest_Objects_{
					Objects: &pb.BatchSendRequest_Objects{},
				},
			}
			for i := 0; i < 2000; i++ {
				req.GetObjects().Values = append(req.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
			}
			res, err = handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending 2000 objects")
			require.Equal(t, int32(640), res.NextBatchSize, "Expected to be told to send 640 objects once buffer is saturated")
			require.Equal(t, float32(2.1599982), res.BackoffSeconds, "Expected to be told to backoff by 2.1599982 seconds")
		})
	})

	t.Run("Stream", func(t *testing.T) {
		t.Run("start and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Start_{
					Start: &pb.BatchStreamMessage_Start{},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Stop_{
					Stop: &pb.BatchStreamMessage_Stop{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
		})

		t.Run("start and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Start_{
					Start: &pb.BatchStreamMessage_Start{},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Stop_{
					Stop: &pb.BatchStreamMessage_Stop{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				close(ch)
			}()

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to shutdown", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			shutdownHandlersCtx, shutdownHandlersCancel := context.WithCancel(context.Background())
			shutdownFinished := make(chan struct{})
			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Start_{
					Start: &pb.BatchStreamMessage_Start{},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_ShuttingDown_{
					ShuttingDown: &pb.BatchStreamMessage_ShuttingDown{},
				},
			}).RunAndReturn(func(*pb.BatchStreamMessage) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				close(shutdownFinished) // Trigger shutdown, which emits the shutdown message
				return nil
			}).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Shutdown_{
					Shutdown: &pb.BatchStreamMessage_Shutdown{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(shutdownHandlersCtx, &sendWg, &streamWg, shutdownFinished, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			readQueues.Make(StreamId)

			shutdownHandlersCancel() // Trigger shutdown of handlers, which emits the shutting down message

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start process error and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Start_{
					Start: &pb.BatchStreamMessage_Start{},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Error_{
					Error: &pb.BatchStreamMessage_Error{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Stop_{
					Stop: &pb.BatchStreamMessage_Stop{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchStreamMessage_Error{{Error: "processing error"}})
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
		})

		t.Run("start process error and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Start_{
					Start: &pb.BatchStreamMessage_Start{},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Error_{
					Error: &pb.BatchStreamMessage_Error{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				StreamId: StreamId,
				Message: &pb.BatchStreamMessage_Stop_{
					Stop: &pb.BatchStreamMessage_Stop{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(ctx, &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil, 0, 0)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchStreamMessage_Error{{Error: "processing error"}})
				close(ch)
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})
	})
}

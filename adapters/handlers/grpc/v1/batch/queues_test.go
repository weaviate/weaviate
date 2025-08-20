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
				Message: &pb.BatchSendRequest_Objects{
					Objects: &pb.BatchObjects{
						Values: []*pb.BatchObject{{Collection: "TestClass"}},
					},
				},
			}

			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			defer shutdownCancel()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			handler := batch.NewQueuesHandler(shutdownCtx, shutdownCtx, writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(shutdownCtx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(req.StreamId, nil)
			next, err := handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending objects")
			require.Equal(t, 1, next, "Expected to send one object")

			// Verify that the internal queue has the object
			obj := <-internalQueue
			require.NotNil(t, obj, "Expected object to be sent to internal queue")

			// Shutdown the scheduler
			shutdownCancel()
		})

		t.Run("test dynamic batch size calulation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			handler := batch.NewQueuesHandler(ctx, ctx, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			// Send 8000 objects
			req := &pb.BatchSendRequest{
				StreamId: StreamId,
				Message: &pb.BatchSendRequest_Objects{
					Objects: &pb.BatchObjects{},
				},
			}
			for i := 0; i < 8000; i++ {
				req.GetObjects().Values = append(req.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
			}
			next, err := handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending 8000 objects")
			require.Equal(t, next, 5120, "Expected to be told to send 5120 objects next")

			// Saturate the buffer
			req = &pb.BatchSendRequest{
				StreamId: StreamId,
				Message: &pb.BatchSendRequest_Objects{
					Objects: &pb.BatchObjects{},
				},
			}
			for i := 0; i < 2000; i++ {
				req.GetObjects().Values = append(req.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
			}
			next, err = handler.Send(ctx, req)
			require.NoError(t, err, "Expected no error when sending 2000 objects")
			require.Equal(t, next, 1411, "Expected to be told to send 1411 objects once buffer is saturated")
		})
	})

	t.Run("Stream", func(t *testing.T) {
		t.Run("start and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()

			handler := batch.NewQueuesHandler(context.Background(), context.Background(), writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
		})

		t.Run("start and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()

			handler := batch.NewQueuesHandler(ctx, ctx, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewStopReadObject()
			}()

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to shutdown", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			shutdownHandlersCtx, shutdownHandlersCancel := context.WithCancel(context.Background())
			shutdownSchedulerCtx, shutdownSchedulerCancel := context.WithCancel(context.Background())

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_ShuttingDown{
					ShuttingDown: &pb.BatchShuttingDown{
						StreamId: StreamId,
					},
				},
			}).RunAndReturn(func(*pb.BatchStreamMessage) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				shutdownHandlersCancel() // Trigger shutdown of handlers, which emits the shutdown message
				return nil
			}).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Shutdown{
					Shutdown: &pb.BatchShutdown{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()

			handler := batch.NewQueuesHandler(shutdownHandlersCtx, shutdownSchedulerCtx, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)

			shutdownSchedulerCancel() // Trigger shutdown of scheduler, which emits the shutting down message

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start process error and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Error{
					Error: &pb.BatchError{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()

			handler := batch.NewQueuesHandler(context.Background(), context.Background(), writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
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
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Error{
					Error: &pb.BatchError{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()

			handler := batch.NewQueuesHandler(ctx, ctx, writeQueues, readQueues, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
				ch <- batch.NewStopReadObject()
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})
	})
}

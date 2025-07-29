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

	writeQueue := batch.NewBatchWriteQueue()
	readQueues := batch.NewBatchReadQueues()
	logger := logrus.New()

	t.Run("Send", func(t *testing.T) {
		// Arrange
		req := &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Send{
				Send: &pb.BatchSend{
					StreamId: "test-stream",
					Objects:  []*pb.BatchObject{{Collection: "TestClass"}},
				},
			},
		}

		shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
		defer shutdownCancel()
		handler := batch.NewHandler(shutdownCtx, writeQueue, readQueues, logger)

		// Act
		howMany := handler.Send(ctx, req)

		// Assert
		require.Equal(t, int64(1), howMany, "Expected to send one object")

		// Verify that the write queue has the object
		obj := <-writeQueue
		require.Equal(t, req, obj, "Expected write queue to contain the sent object")
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
					Stop: &pb.BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			handler := batch.NewHandler(context.Background(), writeQueue, readQueues, logger)

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
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
					Stop: &pb.BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			handler := batch.NewHandler(context.Background(), writeQueue, readQueues, logger)

			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewStopObject()
			}()

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to shutdown", func(t *testing.T) {
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
				Message: &pb.BatchStreamMessage_Shutdown{
					Shutdown: &pb.BatchShutdown{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			handler := batch.NewHandler(shutdownCtx, writeQueue, readQueues, logger)
			readQueues.Make(StreamId)
			shutdownCancel() // Trigger shutdown
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start, process error, and stop due to cancellation", func(t *testing.T) {
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
					Stop: &pb.BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			handler := batch.NewHandler(context.Background(), writeQueue, readQueues, logger)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})

		t.Run("start, process error, and stop due to sentinel", func(t *testing.T) {
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
					Stop: &pb.BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			handler := batch.NewHandler(context.Background(), writeQueue, readQueues, logger)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
				ch <- batch.NewStopObject()
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})
	})
}

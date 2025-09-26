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
	"io"
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

	t.Run("Stream-in", func(t *testing.T) {
		t.Run("send objects using the scheduler", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			// Arrange
			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Recv().Return(&pb.BatchStreamRequest{
				Message: &pb.BatchStreamRequest_Objects_{
					Objects: &pb.BatchStreamRequest_Objects{
						Values: []*pb.BatchObject{{Collection: "TestClass"}},
					},
				},
			}, nil).Once() // Send 1 objects
			stream.EXPECT().Recv().Return(nil, io.EOF).Once() // End the stream

			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			defer shutdownCancel()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			processingQueue := batch.NewBatchProcessingQueue(1)
			reportingQueue := batch.NewBatchReportingQueue(1)
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(shutdownCtx, &sendWg, &streamWg, nil, writeQueues, readQueues, nil, logger)
			var sWg sync.WaitGroup
			batch.StartScheduler(shutdownCtx, &sWg, writeQueues, processingQueue, reportingQueue, nil, logger)

			writeQueues.Make(StreamId, nil)
			handler.RecvWgAdd()
			go func() {
				err := handler.StreamRecv(ctx, StreamId, stream)
				require.NoError(t, err, "Expected no error when streaming in objects")
			}()

			// Verify that the internal queue has the object
			obj := <-processingQueue
			require.NotNil(t, obj, "Expected object to be sent to internal queue")
		})

		// t.Run("dynamic batch size calulation", func(t *testing.T) {
		// 	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		// 	defer cancel()

		// 	writeQueues := batch.NewBatchWriteQueues()
		// 	readQueues := batch.NewBatchReadQueues()
		// 	var sendWg sync.WaitGroup
		// 	var streamWg sync.WaitGroup
		// 	handler := batch.NewQueuesHandler(ctx, &sendWg, &streamWg, nil, writeQueues, readQueues, logger)

		// 	writeQueues.Make(StreamId, nil)
		// 	readQueues.Make(StreamId)

		// 	// Send 8000 objects
		// 	req1 := &pb.BatchStreamRequest{
		// 		Message: &pb.BatchStreamRequest_Objects_{
		// 			Objects: &pb.BatchStreamRequest_Objects{},
		// 		},
		// 	}
		// 	for i := 0; i < 8000; i++ {
		// 		req1.GetObjects().Values = append(req1.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
		// 	}
		// 	// Saturate the buffer
		// 	req2 := &pb.BatchStreamRequest{
		// 		Message: &pb.BatchStreamRequest_Objects_{
		// 			Objects: &pb.BatchStreamRequest_Objects{},
		// 		},
		// 	}
		// 	for i := 0; i < 2000; i++ {
		// 		req2.GetObjects().Values = append(req2.GetObjects().Values, &pb.BatchObject{Collection: "TestClass"})
		// 	}

		// 	stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
		// 	stream.EXPECT().Recv().Return(req1, nil).Once()   // Send first 8000 objects
		// 	stream.EXPECT().Recv().Return(req2, nil).Once()   // Send second request to saturate the buffer
		// 	stream.EXPECT().Recv().Return(nil, io.EOF).Once() // End the stream
		// 	stream.EXPECT().Send(&pb.BatchStreamReply{
		// 		Message: &pb.BatchStreamReply_Backoff_{
		// 			Backoff: &pb.BatchStreamReply_Backoff{
		// 				BackoffSeconds: 1.2499998,
		// 				NextBatchSize:  799,
		// 			},
		// 		},
		// 	}).Return(nil).Once() // Expected reply after first request
		// 	stream.EXPECT().Send(&pb.BatchStreamReply{
		// 		Message: &pb.BatchStreamReply_Backoff_{
		// 			Backoff: &pb.BatchStreamReply_Backoff{
		// 				BackoffSeconds: 2.1599982,
		// 				NextBatchSize:  640,
		// 			},
		// 		},
		// 	}).Return(nil).Once() // Expected reply after second request

		// 	wg := sync.WaitGroup{}
		// 	wg.Add(2)
		// 	go func() {
		// 		defer wg.Done()
		// 		err := handler.StreamRecv(ctx, StreamId, stream)
		// 		require.NoError(t, err, "Expected no error when streaming in objects")
		// 	}()
		// 	go func() {
		// 		defer wg.Done()
		// 		err := handler.StreamSend(ctx, StreamId, stream)
		// 		require.NoError(t, err, "Expected no error when streaming out objects")
		// 	}()
		// 	wg.Wait()
		// })
	})

	t.Run("Stream-out", func(t *testing.T) {
		t.Run("start and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Stop_{
					Stop: &pb.BatchStreamReply_Stop{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, nil, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			done := make(chan struct{})
			handler.SendWgAdd()
			err := handler.StreamSend(ctx, StreamId, stream)
			require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
			select {
			case <-done:
				t.Fatal("Expected done channel to not be closed")
			default:
			}
		})

		t.Run("start and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			done := make(chan struct{})

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Stop_{
					Stop: &pb.BatchStreamReply_Stop{},
				},
			}).RunAndReturn(func(*pb.BatchStreamReply) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				close(done)
				return nil
			}).Once()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, nil, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				close(ch)
			}()

			handler.SendWgAdd()
			err := handler.StreamSend(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to shutdown", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			shutdownHandlersCtx, shutdownHandlersCancel := context.WithCancel(context.Background())
			shutdownFinished := make(chan struct{})
			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_ShutdownTriggered_{
					ShutdownTriggered: &pb.BatchStreamReply_ShutdownTriggered{},
				},
			}).RunAndReturn(func(*pb.BatchStreamReply) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				close(shutdownFinished) // Trigger shutdown, which emits the shutdown message
				return nil
			}).Once()
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_ShutdownFinished_{
					ShutdownFinished: &pb.BatchStreamReply_ShutdownFinished{},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(shutdownHandlersCtx, &sendWg, &streamWg, shutdownFinished, writeQueues, readQueues, nil, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)

			shutdownHandlersCancel() // Trigger shutdown of handlers, which emits the shutting down message

			handler.SendWgAdd()
			err := handler.StreamSend(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start process error and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			done := make(chan struct{})
			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Error_{
					Error: &pb.BatchStreamReply_Error{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Stop_{
					Stop: &pb.BatchStreamReply_Stop{},
				},
			}).RunAndReturn(func(*pb.BatchStreamReply) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				close(done)
				return nil
			}).Once()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(context.Background(), &sendWg, &streamWg, nil, writeQueues, readQueues, nil, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchStreamReply_Error{{Error: "processing error"}})
			}()

			readQueues.Make(StreamId)
			handler.SendWgAdd()
			err := handler.StreamSend(ctx, StreamId, stream)
			require.Equal(t, ctx.Err(), err, "Expected context cancelled error")
			_, ok = <-done
			require.False(t, ok, "Expected done channel to be closed")
		})

		t.Run("start process error and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			done := make(chan struct{})
			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Error_{
					Error: &pb.BatchStreamReply_Error{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamReply{
				Message: &pb.BatchStreamReply_Stop_{
					Stop: &pb.BatchStreamReply_Stop{},
				},
			}).RunAndReturn(func(*pb.BatchStreamReply) error {
				// Ensure handler cancel call comes after this message has been emitted to avoid races
				close(done)
				return nil
			}).Once()

			writeQueues := batch.NewBatchWriteQueues(1)
			readQueues := batch.NewBatchReadQueues()
			var sendWg sync.WaitGroup
			var streamWg sync.WaitGroup
			handler := batch.NewQueuesHandler(ctx, &sendWg, &streamWg, nil, writeQueues, readQueues, nil, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchStreamReply_Error{{Error: "processing error"}})
				close(ch)
			}()

			readQueues.Make(StreamId)
			handler.SendWgAdd()
			err := handler.StreamSend(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
			_, ok = <-done
			require.False(t, ok, "Expected done channel to be closed")
		})
	})
}

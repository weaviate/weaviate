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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

var StreamId string = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"

func TestWorkerLoop(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	writeQueue := batch.NewBatchWriteQueue()
	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	logger := logrus.New()

	t.Run("should process objects and send them without error", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil).Times(1)
		batch.StartBatchWorkers(ctx, 1, writeQueue, readQueues, mockBatcher, logger)

		// Send data
		writeQueue <- &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Send{
				Send: &pb.BatchSend{StreamId: StreamId},
			},
		}

		// Send sentinel
		writeQueue <- &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Stop{
				Stop: &pb.BatchStop{StreamId: StreamId},
			},
		}

		// Accept the stop message
		ch, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist and to contain message")
		stop := <-ch
		require.True(t, stop.Shutdown, "Expected shutdown signal to be true")
	})

	t.Run("should process objects and send them with a returned error", func(t *testing.T) {
		mockBatcher := mocks.NewMockBatcher(t)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		errors := []*pb.BatchObjectsReply_BatchError{
			{
				Error: "batch error",
				Index: 0,
			},
		}
		mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: errors,
		}, nil)
		batch.StartBatchWorkers(ctx, 1, writeQueue, readQueues, mockBatcher, logger)

		// Send data
		obj := &pb.BatchObject{}
		writeQueue <- &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Send{
				Send: &pb.BatchSend{
					StreamId: StreamId,
					Objects:  []*pb.BatchObject{obj},
				},
			},
		}

		// Send sentinel
		writeQueue <- &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Stop{
				Stop: &pb.BatchStop{StreamId: StreamId},
			},
		}

		ch, ok := readQueues.Get(StreamId)
		require.True(t, ok, "Expected read queue to exist and to contain message")

		// Read error
		errs := <-ch
		require.NotNil(t, errs.Errors, "Expected errors to be returned")
		require.Len(t, errs.Errors, 1, "Expected one error to be returned")
		require.Equal(t, "batch error", errs.Errors[0].Error, "Expected error message to match")
		require.Equal(t, obj, errs.Errors[0].Object, "Expected object to match the one sent")

		// Read sentinel
		stop := <-ch
		require.True(t, stop.Shutdown, "Expected shutdown signal to be true")
	})
}

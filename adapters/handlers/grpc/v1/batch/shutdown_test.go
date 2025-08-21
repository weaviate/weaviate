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

func TestShutdownLogic(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	writeQueues := batch.NewBatchWriteQueues()
	writeQueues.Make(StreamId, nil)
	wq, ok := writeQueues.GetQueue(StreamId)
	require.Equal(t, true, ok, "write queue should exist")
	internalQueue := batch.NewBatchInternalQueue()

	howManyObjs := 5000
	// 5000 objs will be sent five times in batches of 1000
	mockBatcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
		time.Sleep(1 * time.Second)
		return &pb.BatchObjectsReply{
			Took:   float32(1),
			Errors: nil,
		}, nil
	}).Times(5)

	for i := 0; i < howManyObjs; i++ {
		wq <- batch.NewWriteObject(&pb.BatchObject{})
	}

	shutdown := batch.NewShutdown(ctx)
	batch.NewQueuesHandler(shutdown.HandlersCtx, shutdown.HandlersWg, shutdown.ShutdownFinished, writeQueues, readQueues, logger)
	batch.StartScheduler(shutdown.SchedulerCtx, shutdown.SchedulerWg, writeQueues, internalQueue, logger)
	batch.StartBatchWorkers(shutdown.WorkersCtx, shutdown.WorkersWg, 1, internalQueue, readQueues, writeQueues, mockBatcher, logger)
	shutdown.Drain(logger)
}

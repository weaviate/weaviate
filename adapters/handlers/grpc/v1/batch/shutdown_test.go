//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io

package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestShutdownLogic(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	logger := logrus.New()

	mockBatcher := mocks.NewMockBatcher(t)

	readQueues := batch.NewBatchReadQueues()
	readQueues.Make(StreamId)
	writeQueues := batch.NewBatchWriteQueues()
	writeQueues.Make(StreamId, nil)
	wq, _ := writeQueues.GetQueue(StreamId)
	internalQueue := batch.NewBatchInternalQueue()

	howManyObjs := 2000
	// 2000 objs will be sent twice in batches of 1000
	mockBatcher.EXPECT().BatchObjects(ctx, mock.Anything).Return(&pb.BatchObjectsReply{
		Took:   float32(1),
		Errors: nil,
	}, nil).Times(2)

	shutdown := v1.NewGrpcShutdown(ctx)
	batch.StartScheduler(ctx, shutdown.SchedulerWg, writeQueues, internalQueue, logger)
	batch.StartBatchWorkers(ctx, shutdown.WorkersWg, 1, internalQueue, readQueues, writeQueues, mockBatcher, logger)

	go func() {
		for i := 0; i < howManyObjs; i++ {
			wq <- batch.NewWriteObject(&pb.BatchObject{})
		}
	}()

	shutdown.Drain(logger)
}

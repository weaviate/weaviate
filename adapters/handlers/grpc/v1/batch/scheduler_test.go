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
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestScheduler(t *testing.T) {
	ctx := context.Background()

	logger := logrus.New()

	t.Run("dynamic", func(t *testing.T) {
		shutdownCtx, shutdownCancel := context.WithCancel(ctx)
		defer shutdownCancel()

		writeQueues := batch.NewBatchWriteQueues()
		internalQueue := batch.NewBatchInternalQueue()

		writeQueues.Make("test-stream", nil)
		var wg sync.WaitGroup
		batch.StartScheduler(shutdownCtx, &wg, writeQueues, internalQueue, logger)

		queue, ok := writeQueues.GetQueue("test-stream")
		require.True(t, ok, "Expected write queue to exist")

		obj := &pb.BatchObject{}
		queue <- batch.NewWriteObject(obj)

		require.Eventually(t, func() bool {
			select {
			case receivedObj := <-internalQueue:
				return receivedObj.Objects.Values[0] == obj
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "Expected object to be sent to internal queue")

		shutdownCancel() // Trigger shutdown
		wg.Wait()        // Wait for scheduler to finish

		require.Empty(t, internalQueue, "Expected internal queue to be empty after shutdown")
		ch, ok := writeQueues.Get("test-stream")
		require.True(t, ok, "Expected write queue to still exist after shutdown")
		require.Empty(t, ch, "Expected write queue to be empty after shutdown")
	})
}

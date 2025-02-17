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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestGRPCBatchStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	booksClass := books.ClassContextionaryVectorizer()
	// compose, err := docker.New().
	// 	WithWeaviateWithGRPC().
	// 	WithText2VecContextionary().
	// 	Start(ctx)
	// require.Nil(t, err)

	helper.SetupClient("localhost:8080")
	helper.SetupGRPCClient(t, "localhost:50051")

	defer func() {
		helper.DeleteClass(t, booksClass.Class)
		helper.ResetClient()
		// require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	client := helper.ClientGRPC(t)

	t.Run("setup", func(t *testing.T) {
		helper.CreateClass(t, booksClass)
	})

	t.Run("Batch import - all success", func(t *testing.T) {
		batching, err := client.Batch(ctx)
		require.NoError(t, err)

		// init batch
		err = batching.Send(&pb.BatchMessage{
			Message: &pb.BatchMessage_Init{
				Init: &pb.BatchStart{
					ConsistencyLevel: ptr(pb.ConsistencyLevel_CONSISTENCY_LEVEL_ALL),
				},
			},
		})
		require.NoError(t, err)

		// send objs
		for _, object := range books.BatchObjects() {
			err = batching.Send(&pb.BatchMessage{
				Message: &pb.BatchMessage_Object{
					Object: object,
				},
			})
			require.NoError(t, err)
		}

		// finish batch
		err = batching.Send(&pb.BatchMessage{
			Message: &pb.BatchMessage_Sentinel{
				Sentinel: &pb.BatchStop{},
			},
		})
		require.NoError(t, err)

		err = batching.CloseSend()
		require.NoError(t, err)

		// receive response
		for {
			resp, err := batching.Recv()
			if err != nil {
				t.Logf("Error: %v", err)
				break
			}
			require.NotNil(t, resp)
			require.Equal(t, 0, len(resp.Errors))
		}

		resp, err := client.Aggregate(ctx, &pb.AggregateRequest{
			Collection:   booksClass.Class,
			ObjectsCount: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(3), *resp.GetSingleResult().ObjectsCount)
	})

	t.Run("Batch import - one error", func(t *testing.T) {
		batching, err := client.Batch(ctx)
		require.NoError(t, err)

		// init batch
		err = batching.Send(&pb.BatchMessage{
			Message: &pb.BatchMessage_Init{
				Init: &pb.BatchStart{
					ConsistencyLevel: ptr(pb.ConsistencyLevel_CONSISTENCY_LEVEL_ALL),
				},
			},
		})
		require.NoError(t, err)

		// send objs
		for idx, object := range books.BatchObjects() {
			if idx == 1 {
				object.Collection = "invalid"
			}
			err = batching.Send(&pb.BatchMessage{
				Message: &pb.BatchMessage_Object{
					Object: object,
				},
			})
			require.NoError(t, err)
		}

		// finish batch
		err = batching.Send(&pb.BatchMessage{
			Message: &pb.BatchMessage_Sentinel{
				Sentinel: &pb.BatchStop{},
			},
		})
		require.NoError(t, err)

		err = batching.CloseSend()
		require.NoError(t, err)

		// receive response
		for {
			resp, err := batching.Recv()
			if err != nil {
				t.Logf("Error: %v", err)
				break
			}
			require.NotNil(t, resp)
			assert.Len(t, resp.Errors, 1)
			for _, err := range resp.Errors {
				assert.Equal(t, int32(1), err.Index)
				assert.Contains(t, err.Error, "invalid")
			}
		}

		resp, err := client.Aggregate(ctx, &pb.AggregateRequest{
			Collection:   booksClass.Class,
			ObjectsCount: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(3), *resp.GetSingleResult().ObjectsCount)
	})
}

func ptr[T any](v T) *T {
	return &v
}

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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestGRPCBatchStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	booksClass := books.ClassContextionaryVectorizer()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithText2VecContextionary().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	defer func() {
		helper.DeleteClass(t, booksClass.Class)
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
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
		count := 0
		for {
			resp, err := batching.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			t.Logf("Received unexpected error: %v", resp)
			count++
		}
		assert.Equal(t, 0, count)

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
		count := 0
		for {
			resp, err := batching.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			require.NotNil(t, resp)
			assert.Equal(t, int32(1), resp.Index)
			assert.Contains(t, resp.Error, "invalid")
			count++
		}
		assert.Equal(t, 1, count)

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

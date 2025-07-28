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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestGRPC_Batching(t *testing.T) {
	ctx := context.Background()
	grpcClient, _ := newClient(t)

	// delete if exists and then re-create Books class
	booksClass := books.ClassNamedContextionaryVectorizer()
	helper.DeleteClass(t, booksClass.Class)
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("Server-side batching", func(t *testing.T) {
		// Open up a stream to read messages from
		stream, err := grpcClient.BatchStream(ctx, nil)
		require.NoError(t, err, "BatchStream should not return an error")

		// Read first message, which starts the batching process
		resp, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		start := resp.GetStart()
		require.NotNil(t, start, "Start message should not be nil")
		streamId := start.GetStreamId()

		// Send objects in send message
		objs := books.BatchObjects()
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Send{Send: &pb.BatchSend{Objects: objs, StreamId: streamId}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		// Send stop message
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			Message: &pb.BatchSendRequest_Stop{Stop: &pb.BatchStop{StreamId: streamId}},
		})
		require.NoError(t, err, "BatchSend Stop should not return an error")

		// Read the stop message
		resp, err = stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		end := resp.GetStop()
		require.NotNil(t, end, "End message should not be nil")

		// Validate the number of objects created
		list, err := helper.ListObjects(t, booksClass.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, list.Objects, len(objs), "Number of objects created should match the number sent")
	})
}

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
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestGRPC_Batching(t *testing.T) {
	helper.SetupClient("localhost:8080")
	ctx := context.Background()
	grpcClient, _ := newClient(t)

	clsA := articles.ArticlesClass()
	clsP := articles.ParagraphsClass()

	helper.DeleteClass(t, clsP.Class)
	helper.DeleteClass(t, clsA.Class)
	// Create the schema
	helper.CreateClass(t, clsP)
	helper.CreateClass(t, clsA)
	defer func() {
		helper.DeleteClass(t, clsP.Class)
		helper.DeleteClass(t, clsA.Class)
	}()

	t.Run("Server-side batching", func(t *testing.T) {
		// Open up a stream to read messages from
		stream, err := grpcClient.BatchStream(ctx, &pb.BatchStreamRequest{})
		require.NoError(t, err, "BatchStream should not return an error")

		// Read first message, which starts the batching process
		resp, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		start := resp.GetStart()
		require.NotNil(t, start, "Start message should not be nil")
		streamId := start.GetStreamId()

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
			{Collection: clsP.Class, Uuid: UUID2},
		}
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Objects{Objects: &pb.BatchObjects{Values: objects}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message: &pb.BatchSendRequest_References{References: &pb.BatchReferences{
				Values: references,
			}},
		})
		require.NoError(t, err, "BatchSend References should not return an error")

		// Send stop message
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Stop{Stop: &pb.BatchSendRequest_BatchStop{}},
		})
		require.NoError(t, err, "BatchSend Stop should not return an error")

		// Read the stop message
		resp, err = stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		end := resp.GetStop()
		require.NotNil(t, end, "End message should not be nil")

		// Validate the number of articles created
		listA, err := helper.ListObjects(t, clsA.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, listA.Objects, 1, "Number of articles created should match the number sent")
		require.NotNil(t, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], "hasParagraphs should not be nil")
		require.Len(t, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], 2, "Article should have 2 paragraphs")

		listP, err := helper.ListObjects(t, clsP.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, listP.Objects, 2, "Number of paragraphs created should match the number sent")
	})
}

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

	setupClasses := func() func() {
		helper.DeleteClass(t, clsA.Class)
		helper.DeleteClass(t, clsP.Class)
		// Create the schema
		helper.CreateClass(t, clsP)
		helper.CreateClass(t, clsA)
		return func() {
			helper.DeleteClass(t, clsA.Class)
			helper.DeleteClass(t, clsP.Class)
		}
	}

	t.Run("send objects and references without errors", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, streamId := startStream(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
			{Collection: clsP.Class, Uuid: UUID2},
		}
		_, err := grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
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
		resp, err := stream.Recv()
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

	t.Run("send objects that should partially error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, streamId := startStream(ctx, t, grpcClient)

		// Send a list of articles, one with a tenant incorrectly specified
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: UUID1},
			{Collection: clsA.Class, Uuid: UUID2},
		}
		_, err := grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Objects{Objects: &pb.BatchObjects{Values: objects}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		errMsg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, errMsg.GetPartialError().Error, "class Article has multi-tenancy disabled, but request was with tenant")
		require.Equal(t, errMsg.GetPartialError().Index, int32(1), "Error index should be 1")
		require.True(t, errMsg.GetPartialError().IsObject, "IsObject should be true for object errors")
		require.False(t, errMsg.GetPartialError().IsReference, "IsReference should be false for object errors")

		list, err := helper.ListObjects(t, clsA.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, list.Objects, 2, "There should be two articles")
	})

	t.Run("send objects that should full error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, streamId := startStream(ctx, t, grpcClient)

		// Send a list of articles, one with a tenant incorrectly specified
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Tenant: "tenant", Uuid: UUID0},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: UUID1},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: UUID2},
		}
		_, err := grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Objects{Objects: &pb.BatchObjects{Values: objects}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		errMsg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, errMsg.GetFullError().Error, "class Article has multi-tenancy disabled, but request was with tenant")
		require.Equal(t, errMsg.GetFullError().Indices, []int32{0, 1, 2}, "Error indices should be [0, 1, 2]")
		require.True(t, errMsg.GetFullError().IsObject, "IsObject should be true for object errors")
		require.False(t, errMsg.GetFullError().IsReference, "IsReference should be false for object errors")
		require.False(t, errMsg.GetFullError().Retriable, "Retriable should be false for this error")

		list, err := helper.ListObjects(t, clsA.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, list.Objects, 0, "There should be zero articles")
	})

	t.Run("send references that should partial error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, streamId := startStream(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
		}
		_, err := grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Objects{Objects: &pb.BatchObjects{Values: objects}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		// Send a list of references, one pointing to a non-existent object
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message: &pb.BatchSendRequest_References{References: &pb.BatchReferences{
				Values: references,
			}},
		})
		require.NoError(t, err, "BatchSend References should not return an error")

		errMsg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, errMsg.GetPartialError().Error, "property hasParagraphss does not exist for class Article")
		require.Equal(t, errMsg.GetPartialError().Index, int32(1), "Error index should be 1")
		require.True(t, errMsg.GetPartialError().IsReference, "IsReference should be true for reference errors")
		require.False(t, errMsg.GetPartialError().IsObject, "IsObject should be false for reference errors")

		obj, err := helper.GetObject(t, clsA.Class, UUID0)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Equal(t, 1, len(obj.Properties.(map[string]any)["hasParagraphs"].([]any)), "Article should have 1 paragraph")
	})

	t.Run("send references that should full error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, streamId := startStream(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
		}
		_, err := grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message:  &pb.BatchSendRequest_Objects{Objects: &pb.BatchObjects{Values: objects}},
		})
		require.NoError(t, err, "BatchSend should not return an error")

		// Send a list of references, all with incorrect prop name
		references := []*pb.BatchReference{
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		_, err = grpcClient.BatchSend(ctx, &pb.BatchSendRequest{
			StreamId: streamId,
			Message: &pb.BatchSendRequest_References{References: &pb.BatchReferences{
				Values: references,
			}},
		})
		require.NoError(t, err, "BatchSend References should not return an error")

		errMsg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, errMsg.GetFullError().Error, "property hasParagraphss does not exist for class Article")
		require.Equal(t, errMsg.GetFullError().Indices, []int32{0, 1}, "Error indices should be [0, 1]")
		require.False(t, errMsg.GetFullError().IsObject, "IsObject should be false for reference errors")
		require.True(t, errMsg.GetFullError().IsReference, "IsReference should be true for reference errors")
		require.False(t, errMsg.GetFullError().Retriable, "Retriable should be false for this error")

		obj, err := helper.GetObject(t, clsA.Class, UUID0)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Nil(t, obj.Properties.(map[string]any)["hasParagraphs"], "Article should have 0 paragraphs")
	})
}

func startStream(ctx context.Context, t *testing.T, grpcClient pb.WeaviateClient) (pb.Weaviate_BatchStreamClient, string) {
	stream, err := grpcClient.BatchStream(ctx, &pb.BatchStreamRequest{})
	require.NoError(t, err, "BatchStream should not return an error")

	// Read first message, which starts the batching process
	resp, err := stream.Recv()
	require.NoError(t, err, "BatchStream should return a response")
	start := resp.GetStart()
	require.NotNil(t, start, "Start message should not be nil")
	streamId := start.GetStreamId()

	return stream, streamId
}

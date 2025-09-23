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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"google.golang.org/grpc"
)

func TestGRPC_Batching(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, _ := client(t, compose.GetWeaviate().GrpcURI())

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
		stream := start(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
			{Collection: clsP.Class, Uuid: UUID2},
		}
		sendObjects(t, stream, objects)

		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		sendReferences(t, stream, references)

		// Send stop message
		sendStop(t, stream)

		// Validate the number of articles created
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			listA, err := helper.ListObjects(t, clsA.Class)
			require.NoError(t, err, "ListObjects should not return an error")
			require.Len(ct, listA.Objects, 1, "Number of articles created should match the number sent")
			require.NotNil(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], "hasParagraphs should not be nil")
			require.Len(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], 2, "Article should have 2 paragraphs")

			listP, err := helper.ListObjects(t, clsP.Class)
			require.NoError(t, err, "ListObjects should not return an error")
			require.Len(ct, listP.Objects, 2, "Number of paragraphs created should match the number sent")
		}, 10*time.Second, 1*time.Second, "Objects not created within time")
	})

	t.Run("send objects that should partially error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient)

		// Send a list of articles, one with a tenant incorrectly specified
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: UUID1},
			{Collection: clsA.Class, Uuid: UUID2},
		}
		sendObjects(t, stream, objects)

		// Read the error message
		errMsg, err := stream.Recv()
		if errMsg.GetBackoff() != nil {
			// if we got a backoff message, read the next message which should be the error
			errMsg, err = stream.Recv()
		}
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, "class Article has multi-tenancy disabled, but request was with tenant", errMsg.GetError().Error)
		require.Equal(t, objects[1].Tenant, errMsg.GetError().GetObject().Tenant, "Errored object should be the second one")

		list, err := helper.ListObjects(t, clsA.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, list.Objects, 2, "There should be two articles")
	})

	t.Run("send references that should error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
		}
		sendObjects(t, stream, objects)

		// Send a list of references, one pointing to a non-existent object
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		sendReferences(t, stream, references)

		// Read the error message
		errMsg, err := stream.Recv()
		if errMsg.GetBackoff() != nil {
			// if we got a backoff message, read the next message which should be the error
			errMsg, err = stream.Recv()
		}
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.Equal(t, "property hasParagraphss does not exist for class Article", errMsg.GetError().Error)
		require.Equal(t, references[1].ToUuid, errMsg.GetError().GetReference().ToUuid, "Errored reference should be the second one")

		obj, err := helper.GetObject(t, clsA.Class, UUID0)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Equal(t, 1, len(obj.Properties.(map[string]any)["hasParagraphs"].([]any)), "Article should have 1 paragraph")
	})

	t.Run("send 100000 objects then immediately restart the node to trigger shutdown and ensure all are present afterwards", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient)

		// Send 100000 articles
		var objects []*pb.BatchObject
		for i := 0; i < 100000; i++ {
			objects = append(objects, &pb.BatchObject{Collection: clsA.Class, Uuid: helper.IntToUUID(uint64(i)).String()})
		}
		sendObjects(t, stream, objects)
		t.Log("Done adding objects to stream")

		// Stop the node
		t.Log("Stopping node...")
		common.StopNodeAt(ctx, t, compose, 0)
		t.Log("Stopped node")

		// Restart the node
		t.Log("Restarting node...")
		common.StartNodeAt(ctx, t, compose, 0)
		t.Log("Restarted node")
		helper.SetupClient(compose.GetWeaviate().URI())
		grpcClient, _ = client(t, compose.GetWeaviate().GrpcURI())

		// Verify that all objects are present
		res, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection:   clsA.Class,
			ObjectsCount: true,
		})
		require.NoError(t, err, "Aggregate should not return an error")
		require.Equal(t, int64(100000), *res.GetSingleResult().ObjectsCount, "Number of articles created should match the number sent")
	})
}

func TestGRPCCluster_Batching(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	// Point client at node-0
	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, _ := client(t, compose.GetWeaviate().GrpcURI())

	clsA := articles.ArticlesClass()
	clsA.ReplicationConfig = &models.ReplicationConfig{
		Factor:       3,
		AsyncEnabled: true,
	}
	clsP := articles.ParagraphsClass()
	clsP.ReplicationConfig = &models.ReplicationConfig{
		Factor:       3,
		AsyncEnabled: true,
	}

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
		stream := start(ctx, t, grpcClient)

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0},
			{Collection: clsP.Class, Uuid: UUID1},
			{Collection: clsP.Class, Uuid: UUID2},
		}
		sendObjects(t, stream, objects)

		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		sendReferences(t, stream, references)

		// Send stop message
		sendStop(t, stream)

		// Validate the number of articles created
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			listA, err := helper.ListObjects(t, clsA.Class)
			require.NoError(t, err, "ListObjects should not return an error")
			require.Len(ct, listA.Objects, 1, "Number of articles created should match the number sent")
			require.NotNil(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], "hasParagraphs should not be nil")
			require.Len(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], 2, "Article should have 2 paragraphs")

			listP, err := helper.ListObjects(t, clsP.Class)
			require.NoError(ct, err, "ListObjects should not return an error")
			require.Len(ct, listP.Objects, 2, "Number of paragraphs created should match the number sent")
		}, 30*time.Second, 3*time.Second, "Objects not replicated within time")
	})

	t.Run("test client/server behaviour when reconnecting between nodes due to shutdown", func(t *testing.T) {
		defer setupClasses()()
		firstNode := 2
		secondNode := 1

		helper.SetupClient(compose.GetWeaviateNode(firstNode).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(firstNode).GrpcURI())
		stream := start(ctx, t, grpcClient)

		var shuttingDown atomic.Bool
		var shutdown atomic.Bool
		var stopped atomic.Bool
		var wg sync.WaitGroup

		// start a goroutine that continuously sends objects
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]*pb.BatchObject, 0, 1000)
			for i := 0; i < 100000; i++ {
				if stopped.Load() {
					// stop sending if we received a stop from the server
					fmt.Printf("%s Stopping sending objects as we received a stop from the server\n", time.Now().Format("15:04:05"))
					return
				}
				for shuttingDown.Load() {
					fmt.Printf("%s Can't send, server is shutting down\n", time.Now().Format("15:04:05"))
					time.Sleep(1 * time.Second)
					continue
				}
				if shutdown.Load() {
					fmt.Printf("%s Shutdown completed, reconnecting to node-0\n", time.Now().Format("15:04:05"))
					// reconnect to different node if shutdown completed
					grpcClient, _ = client(t, compose.GetWeaviateNode(secondNode).GrpcURI())
					stream = start(ctx, t, grpcClient)
					shutdown.Store(false)
				}
				batch = append(batch, &pb.BatchObject{
					Collection: clsA.Class,
					Uuid:       helper.IntToUUID(uint64(i)).String(),
				})
				if len(batch) == 1000 {
					sendObjects(t, stream, batch)
					time.Sleep(10 * time.Millisecond) // wait a bit before sending the next batch
					batch = make([]*pb.BatchObject, 0, 1000)
				}
			}
			fmt.Printf("%s Done sending objects\n", time.Now().Format("15:04:05"))
			// Send stop message
			stream.Send(&pb.BatchStreamRequest{
				Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
			})
		}()

		// start a goroutine that continuously reads messages
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				resp, err := stream.Recv()
				if err != nil {
					return
				}
				if resp.GetError() != nil {
					fmt.Printf("%s Received unexpected error from server: %v\n", time.Now().Format("15:04:05"), resp.GetError())
				}
				if resp.GetShutdownTriggered() != nil {
					fmt.Printf("%s Shutdown triggered\n", time.Now().Format("15:04:05"))
					shuttingDown.Store(true)
				}
				if resp.GetShutdownFinished() != nil {
					fmt.Printf("%s Shutdown finished\n", time.Now().Format("15:04:05"))
					shuttingDown.Store(false)
					shutdown.Store(true)
				}
				if resp.GetStop() != nil {
					fmt.Printf("%s Received stop from server\n", time.Now().Format("15:04:05"))
					stopped.Store(true)
					return
				}
			}
		}()

		// Restart node-firstNode
		t.Logf("Stopping node %v...", firstNode)
		common.StopNodeAt(ctx, t, compose, firstNode-1)
		t.Logf("Restarting node %v...", firstNode)
		common.StartNodeAt(ctx, t, compose, firstNode-1)

		helper.SetupClient(compose.GetWeaviateNode(secondNode).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(secondNode).GrpcURI())

		t.Log("Waiting for goroutines to finish...")
		wg.Wait()

		// Verify that all objects are present
		t.Log("Verifying that all objects are present...")
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection:   clsA.Class,
				ObjectsCount: true,
			})
			require.NoError(t, err, "Aggregate should not return an error")
			require.GreaterOrEqual(ct, *res.GetSingleResult().ObjectsCount, int64(100000), "Number of articles created should match the number sent")
		}, 300*time.Second, 5*time.Second, "Objects not replicated within time")
	})
}

func start(ctx context.Context, t *testing.T, grpcClient pb.WeaviateClient) pb.Weaviate_BatchStreamClient {
	stream, err := grpcClient.BatchStream(ctx)
	require.NoError(t, err, "BatchStream should not return an error")

	// Send request to start the batching process
	err = stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}},
	})
	require.NoError(t, err, "sending Start over the stream should not return an error")

	return stream
}

func client(t *testing.T, host string) (pb.WeaviateClient, *grpc.ClientConn) {
	conn, err := helper.CreateGrpcConnectionClient(host)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}

func sendObjects(t *testing.T, stream pb.Weaviate_BatchStreamClient, message []*pb.BatchObject) {
	// Send objects over
	err := stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Objects_{Objects: &pb.BatchStreamRequest_Objects{Values: message}},
	})
	require.NoError(t, err, "sending Objects over the stream should not return an error")
}

func sendReferences(t *testing.T, stream pb.Weaviate_BatchStreamClient, message []*pb.BatchReference) {
	// Send references over
	err := stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_References_{References: &pb.BatchStreamRequest_References{Values: message}},
	})
	require.NoError(t, err, "sending References over the stream should not return an error")
}

func sendStop(t *testing.T, stream pb.Weaviate_BatchStreamClient) {
	// Send stop message
	err := stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
	})
	require.NoError(t, err, "sending Stop over the stream should not return an error")

	// Read the stop message
	resp, err := stream.Recv()
	require.NoError(t, err, "Expected no error when reading Stop from stream; got %v", err)
	end := resp.GetStop()
	require.NotNil(t, end, "End message should not be nil")
}

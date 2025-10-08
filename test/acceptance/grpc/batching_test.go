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
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
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

		uuid0 := uuid.NewString()
		uuid1 := uuid.NewString()
		uuid2 := uuid.NewString()
		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: uuid0},
			{Collection: clsP.Class, Uuid: uuid1},
			{Collection: clsP.Class, Uuid: uuid2},
		}
		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: uuid1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: uuid2},
		}
		err := send(stream, objects, references)
		require.NoError(t, err, "sending Objects and References over the stream should not return an error")
		stream.CloseSend()

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
			{Collection: clsA.Class, Uuid: uuid.NewString()},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: uuid.NewString()},
			{Collection: clsA.Class, Uuid: uuid.NewString()},
		}
		err := send(stream, objects, nil)
		require.NoError(t, err, "sending Objects over the stream should not return an error")
		stream.CloseSend()

		// Read the error message
		errMsg, err := stream.Recv()
		for errMsg.GetBackoff() != nil {
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

		uuid0 := uuid.NewString()
		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: uuid0},
			{Collection: clsP.Class, Uuid: uuid.NewString()},
		}
		// Send a list of references, one pointing to a non-existent object
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: UUID1},
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: UUID2},
		}
		err := send(stream, objects, references)
		require.NoError(t, err, "sending Objects and References over the stream should not return an error")
		stream.CloseSend()

		// Read the error message
		errMsg, err := stream.Recv()
		for errMsg.GetBackoff() != nil {
			// if we got a backoff message, read the next message which should be the error
			errMsg, err = stream.Recv()
		}
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, errMsg, "Error message should not be nil")
		require.NotNil(t, errMsg.GetError(), "Error message should not be nil")
		require.Equal(t, "property hasParagraphss does not exist for class Article", errMsg.GetError().Error)
		require.Equal(t, references[1].ToUuid, errMsg.GetError().GetReference().ToUuid, "Errored reference should be the second one")

		obj, err := helper.GetObject(t, clsA.Class, strfmt.UUID(uuid0))
		require.NoError(t, err, "ListObjects should not return an error")
		require.Equal(t, 1, len(obj.Properties.(map[string]any)["hasParagraphs"].([]any)), "Article should have 1 paragraph")
	})

	t.Run("send 50000 objects as fast as possible", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient)
		defer stream.CloseSend()

		// Send 50000 articles
		var objects []*pb.BatchObject
		for i := 0; i < 50000; i++ {
			objects = append(objects, &pb.BatchObject{Collection: clsA.Class, Uuid: uuid.NewString()})
			if len(objects) == 1000 {
				err := send(stream, objects, nil)
				require.NoError(t, err, "sending Objects over the stream should not return an error")
				objects = nil
				t.Logf("Sent %d objects", i+1)
			}
		}
		t.Log("Done adding objects to stream")

		go func() {
			// Verify no errors returned from the stream
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					// server closed the stream
					t.Log("Stream closed by server")
					return
				}
				if err != nil {
					t.Errorf("Stream recv returned error: %v", err)
					return
				}
				if resp.GetError() != nil {
					t.Errorf("Received unexpected error from server: %v", resp.GetError())
				}
			}
		}()

		// Verify that all objects are present after shutdown and restart
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection:   clsA.Class,
				ObjectsCount: true,
			})
			require.NoError(t, err, "Aggregate should not return an error")
			require.Equal(ct, int64(50000), *res.GetSingleResult().ObjectsCount, "Number of articles created should match the number sent")
		}, 120*time.Second, 5*time.Second, "Objects not created within time")
	})

	t.Run("send 50000 objects then immediately restart the node to trigger shutdown and ensure all are present afterwards", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient)

		// Send 50000 articles
		var objects []*pb.BatchObject
		for i := 0; i < 50000; i++ {
			objects = append(objects, &pb.BatchObject{Collection: clsA.Class, Uuid: uuid.NewString()})
			if len(objects) == 1000 {
				err := send(stream, objects, nil)
				require.NoError(t, err, "sending Objects over the stream should not return an error")
				objects = nil
				t.Logf("Sent %d objects", i+1)
			}
		}
		stream.CloseSend()
		t.Log("Done adding objects to stream")

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Verify no errors returned from the stream
			for {
				resp, err := stream.Recv()
				if errors.Is(err, batch.ErrShutdown) {
					// we expect this error when the server is shutting down
					t.Log("Stream closed by server due to shutdown")
					return
				}
				if errors.Is(err, io.EOF) {
					// server closed the stream
					t.Log("Stream closed by server")
					return
				}
				if err != nil {
					t.Errorf("Stream recv returned error: %v", err)
					return
				}
				if resp.GetError() != nil {
					t.Errorf("Received unexpected error from server: %v", resp.GetError())
				}
			}
		}()

		// Stop the node
		t.Log("Stopping node...")
		common.StopNodeAtWithTimeout(ctx, t, compose, 0, 300*time.Second)
		t.Log("Stopped node")

		// Restart the node
		t.Log("Restarting node...")
		common.StartNodeAt(ctx, t, compose, 0)
		t.Log("Restarted node")
		helper.SetupClient(compose.GetWeaviate().URI())
		grpcClient, _ = client(t, compose.GetWeaviate().GrpcURI())

		// Verify that all objects are present after shutdown and restart
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection:   clsA.Class,
				ObjectsCount: true,
			})
			require.NoError(t, err, "Aggregate should not return an error")
			require.Equal(ct, int64(50000), *res.GetSingleResult().ObjectsCount, "Number of articles created should match the number sent")
		}, 120*time.Second, 5*time.Second, "Objects not created within time")
	})
}

func TestGRPC_ClusterBatching(t *testing.T) {
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
		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		err := send(stream, objects, references)
		require.NoError(t, err, "sending Objects and References over the stream should not return an error")
		stream.CloseSend()

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

	t.Run("test client-server behaviour when reconnecting between nodes due to shutdown", func(t *testing.T) {
		defer setupClasses()()
		firstNode := 2
		secondNode := 1

		helper.SetupClient(compose.GetWeaviateNode(firstNode).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(firstNode).GrpcURI())
		stream := start(ctx, t, grpcClient)

		var shuttingDown atomic.Bool
		var sendWg sync.WaitGroup
		var recvWg sync.WaitGroup
		var streamRestartLock sync.RWMutex

		// start a goroutine that continuously sends objects
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			batch := make([]*pb.BatchObject, 0, 1000)
			for i := 0; i < 50000; i++ {
				for shuttingDown.Load() {
					stream.CloseSend()
					fmt.Printf("%s Can't send, server is shutting down\n", time.Now().Format("15:04:05"))
					time.Sleep(5 * time.Second)
					continue
				}
				batch = append(batch, &pb.BatchObject{
					Collection: clsA.Class,
					Uuid:       helper.IntToUUID(uint64(i)).String(),
				})
				if len(batch) == 1000 {
					fmt.Printf("%s Sending %vth batch of 1000 objects\n", time.Now().Format("15:04:05"), i/1000)
					streamRestartLock.RLock()
					err := send(stream, batch, nil)
					streamRestartLock.RUnlock()
					if errors.Is(err, io.EOF) {
						// Server has closed due to shutdown, continue and loop back to either shuttingDown or shutdown
						fmt.Printf("%s Server closed the stream\n", time.Now().Format("15:04:05"))
						continue
					}
					time.Sleep(100 * time.Millisecond) // wait a bit before sending the next batch
					batch = make([]*pb.BatchObject, 0, 1000)
				}
			}
			fmt.Printf("%s Done sending objects\n", time.Now().Format("15:04:05"))
			stream.CloseSend()
		}()

		// start a goroutine that continuously reads messages
		recvWg.Add(1)
		go func() {
			defer recvWg.Done()
			for {
				resp, err := stream.Recv()
				if errors.Is(err, batch.ErrShutdown) {
					stream.CloseSend()
					fmt.Printf("%s Stream closed by server due to shutdown\n", time.Now().Format("15:04:05"))
					grpcClient, _ = client(t, compose.GetWeaviateNode(secondNode).GrpcURI())
					streamRestartLock.Lock()
					stream = start(ctx, t, grpcClient)
					streamRestartLock.Unlock()
					shuttingDown.Store(false)
					continue // we expect this error when the server is shutting down
				}
				if errors.Is(err, io.EOF) {
					fmt.Printf("%s Stream closed by server\n", time.Now().Format("15:04:05"))
					return // server closed the stream
				}
				if err != nil {
					fmt.Printf("%s Stream recv returned error: %v\n", time.Now().Format("15:04:05"), err)
					return
				}
				if resp.GetError() != nil {
					fmt.Printf("%s Received unexpected error from server: %v\n", time.Now().Format("15:04:05"), resp.GetError())
				}
				if resp.GetShuttingDown() != nil {
					fmt.Printf("%s Shutdown triggered\n", time.Now().Format("15:04:05"))
					shuttingDown.Store(true)
				}
			}
		}()

		// Restart node-firstNode
		t.Logf("Stopping node %v...", firstNode)
		common.StopNodeAtWithTimeout(ctx, t, compose, firstNode-1, 300*time.Second)
		t.Logf("Restarting node %v...", firstNode)
		common.StartNodeAt(ctx, t, compose, firstNode-1)

		t.Log("Waiting for send goroutine to finish...")
		sendWg.Wait()
		t.Log("Send goroutine finished")
		t.Log("Waiting for recv goroutine to finish...")
		recvWg.Wait()
		t.Log("Recv goroutine finished")

		helper.SetupClient(compose.GetWeaviateNode(secondNode).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(secondNode).GrpcURI())

		// Verify that all objects are present
		t.Log("Verifying that all objects are present...")
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
				Collection:   clsA.Class,
				ObjectsCount: true,
			})
			require.NoError(t, err, "Aggregate should not return an error")
			require.GreaterOrEqual(ct, *res.GetSingleResult().ObjectsCount, int64(50000), "Number of articles created should match the number sent")
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

func send(stream pb.Weaviate_BatchStreamClient, objs []*pb.BatchObject, refs []*pb.BatchReference) error {
	// Send objects over
	data := &pb.BatchStreamRequest_Data{}
	if len(objs) > 0 {
		data.Objects = &pb.BatchStreamRequest_Data_Objects{Values: objs}
	}
	if len(refs) > 0 {
		data.References = &pb.BatchStreamRequest_Data_References{Values: refs}
	}
	return stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Data_{Data: data},
	})
}

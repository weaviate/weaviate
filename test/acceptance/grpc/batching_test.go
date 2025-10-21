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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
		stream := start(ctx, t, grpcClient, "")

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
		stop(stream)
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
		stream := start(ctx, t, grpcClient, "")

		// Send a list of articles, one with a tenant incorrectly specified
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: uuid.NewString()},
			{Collection: clsA.Class, Tenant: "tenant", Uuid: uuid.NewString()},
			{Collection: clsA.Class, Uuid: uuid.NewString()},
		}
		err := send(stream, objects, nil)
		require.NoError(t, err, "sending Objects over the stream should not return an error")
		stop(stream)
		stream.CloseSend()

		// Read the results message
		msg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, msg, "Message should not be nil")
		require.NotNil(t, msg.GetResults(), "Results message should not be nil")
		require.Len(t, msg.GetResults().GetErrors(), 1, "There should be one error")
		require.Len(t, msg.GetResults().GetSuccesses(), 2, "There should be two successes")

		require.Equal(t, objects[0].Uuid, msg.GetResults().GetSuccesses()[0].GetUuid(), "First object UUID should match")
		require.Equal(t, objects[2].Uuid, msg.GetResults().GetSuccesses()[1].GetUuid(), "Third object UUID should match")

		require.Equal(t, "class Article has multi-tenancy disabled, but request was with tenant", msg.GetResults().GetErrors()[0].GetError())
		require.Equal(t, objects[1].Uuid, msg.GetResults().GetErrors()[0].GetUuid(), "Errored object should be the second one")

		list, err := helper.ListObjects(t, clsA.Class)
		require.NoError(t, err, "ListObjects should not return an error")
		require.Len(t, list.Objects, 2, "There should be two articles")
	})

	t.Run("send references that should error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient, "")

		uuid0 := uuid.NewString()
		uuid1 := uuid.NewString()
		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: uuid0},
			{Collection: clsP.Class, Uuid: uuid1},
		}
		// Send a list of references, one pointing to a non-existent object
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: uuid0},
			{Name: "hasParagraphss", FromCollection: clsA.Class, FromUuid: uuid0, ToUuid: uuid1},
		}
		err := send(stream, objects, references)
		require.NoError(t, err, "sending Objects and References over the stream should not return an error")
		stop(stream)
		stream.CloseSend()

		// Read the results message
		msg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, msg, "Message should not be nil")
		require.NotNil(t, msg.GetResults(), "Results message should not be nil")
		require.Len(t, msg.GetResults().GetErrors(), 1, "There should be one error")
		require.Len(t, msg.GetResults().GetSuccesses(), 3, "There should be three successes")

		require.Equal(t, uuid0, msg.GetResults().GetSuccesses()[0].GetUuid(), "First object UUID should match")
		require.Equal(t, objects[1].Uuid, msg.GetResults().GetSuccesses()[1].GetUuid(), "Second object UUID should match")
		require.Equal(t, toBeacon(references[0]), msg.GetResults().GetSuccesses()[2].GetBeacon(), "First reference beacon should match")

		require.Equal(t, "property hasParagraphss does not exist for class Article", msg.GetResults().GetErrors()[0].GetError())
		require.Equal(t, toBeacon(references[1]), msg.GetResults().GetErrors()[0].GetBeacon(), "Second reference beacon should match")

		obj, err := helper.GetObject(t, clsA.Class, strfmt.UUID(uuid0))
		require.NoError(t, err, "ListObjects should not return an error")
		require.Equal(t, 1, len(obj.Properties.(map[string]any)["hasParagraphs"].([]any)), "Article should have 1 paragraph")
	})

	t.Run("send 50000 objects as fast as possible", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient, "")
		// Send 50000 articles
		objects := make([]*pb.BatchObject, 0, 1000)
		for i := 0; i < 50000; i++ {
			objects = append(objects, &pb.BatchObject{Collection: clsA.Class, Uuid: uuid.NewString()})
			if len(objects) == 1000 {
				err := send(stream, objects, nil)
				require.NoError(t, err, "sending Objects over the stream should not return an error")
				objects = objects[:0] // reset slice but keep capacity
				t.Logf("Sent %d objects", i+1)
			}
		}
		t.Log("Done adding objects to stream")
		stop(stream)
		stream.CloseSend()

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
				if len(resp.GetResults().GetErrors()) != 0 {
					t.Error("Received unexpected errors from server:")
					for _, e := range resp.GetResults().GetErrors() {
						t.Errorf("Error: %s", e.GetError())
					}
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
		stream := start(ctx, t, grpcClient, "")

		// Send 50000 articles
		objects := make([]*pb.BatchObject, 0, 1000)
		for i := 0; i < 50000; i++ {
			objects = append(objects, &pb.BatchObject{Collection: clsA.Class, Uuid: uuid.NewString()})
			if len(objects) == 1000 {
				err := send(stream, objects, nil)
				require.NoError(t, err, "sending Objects over the stream should not return an error")
				objects = objects[:0] // reset slice but keep capacity
				t.Logf("Sent %d objects", i+1)
			}
		}
		stop(stream)
		stream.CloseSend()
		t.Log("Done adding objects to stream")

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
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
				if len(resp.GetResults().GetErrors()) != 0 {
					t.Error("Received unexpected errors from server:")
					for _, e := range resp.GetResults().GetErrors() {
						t.Errorf("Error: %s", e.GetError())
					}
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
		stream := start(ctx, t, grpcClient, "")

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: UUID0, Vectors: []*pb.Vectors{{Name: "default", VectorBytes: randomByteVector(512)}}},
			{Collection: clsP.Class, Uuid: UUID1, Vectors: []*pb.Vectors{{Name: "default", VectorBytes: randomByteVector(512)}}},
			{Collection: clsP.Class, Uuid: UUID2, Vectors: []*pb.Vectors{{Name: "default", VectorBytes: randomByteVector(512)}}},
		}
		// Send some references between the articles and paragraphs
		references := []*pb.BatchReference{
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID1},
			{Name: "hasParagraphs", FromCollection: clsA.Class, FromUuid: UUID0, ToUuid: UUID2},
		}
		err := send(stream, objects, references)
		require.NoError(t, err, "sending Objects and References over the stream should not return an error")
		stop(stream)
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

	t.Run("verify expected client-server behaviour when reconnecting between nodes due to shutdown", func(t *testing.T) {
		defer setupClasses()()
		firstNode := 2
		secondNode := 1

		helper.SetupClient(compose.GetWeaviateNode(firstNode).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(firstNode).GrpcURI())
		stream := start(ctx, t, grpcClient, "")

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
					t.Logf("%s Can't send, server is shutting down\n", time.Now().Format("15:04:05"))
					time.Sleep(5 * time.Second)
					continue
				}
				batch = append(batch, &pb.BatchObject{
					Collection: clsA.Class,
					Uuid:       helper.IntToUUID(uint64(i)).String(),
					Vectors:    []*pb.Vectors{{Name: "default", VectorBytes: randomByteVector(256)}},
				})
				if len(batch) == 1000 {
					t.Logf("%s Sending %vth batch of 1000 objects\n", time.Now().Format("15:04:05"), i/1000)
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
			stop(stream)
			stream.CloseSend()
		}()

		// start a goroutine that continuously reads messages
		recvWg.Add(1)
		go func() {
			defer recvWg.Done()
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					t.Logf("%s Stream closed by server\n", time.Now().Format("15:04:05"))
					return // server closed the stream
				}
				if err != nil {
					t.Errorf("%s Stream recv returned error: %v\n", time.Now().Format("15:04:05"), err)
					return
				}
				if len(resp.GetResults().GetErrors()) != 0 {
					t.Error("Received unexpected errors from server:")
					for _, e := range resp.GetResults().GetErrors() {
						t.Errorf("Error: %s", e.GetError())
					}
				}
				if resp.GetShuttingDown() != nil {
					t.Logf("%s Shutdown triggered\n", time.Now().Format("15:04:05"))
					shuttingDown.Store(true)
				}
				if resp.GetShutdown() != nil {
					stream.CloseSend()
					t.Logf("%s Stream closed by server due to shutdown\n", time.Now().Format("15:04:05"))
					grpcClient, _ = client(t, compose.GetWeaviateNode(secondNode).GrpcURI())
					streamRestartLock.Lock()
					stream = start(ctx, t, grpcClient, "")
					streamRestartLock.Unlock()
					shuttingDown.Store(false)
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

	t.Run("verify that server still shuts down if client hangs up without closing its end of the stream", func(t *testing.T) {
		defer setupClasses()()
		node := 2

		helper.SetupClient(compose.GetWeaviateNode(node).URI())
		grpcClient, _ = client(t, compose.GetWeaviateNode(node).GrpcURI())
		stream := start(ctx, t, grpcClient, "")

		batch := make([]*pb.BatchObject, 0, 1000)
		for i := 0; i < 1000; i++ {
			batch = append(batch, &pb.BatchObject{
				Collection: clsA.Class,
				Uuid:       helper.IntToUUID(uint64(i)).String(),
			})
		}
		err := send(stream, batch, nil)
		require.NoError(t, err, "sending Objects over the stream should not return an error")

		// Restart node
		t.Logf("Stopping node %v...", node)
		common.StopNodeAtWithTimeout(ctx, t, compose, node-1, 300*time.Second)
		t.Logf("Restarting node %v...", node)
		common.StartNodeAt(ctx, t, compose, node-1)
		t.Log("Node was restarted successfully in time")

		// Setup again to allow cleanup to work in defer
		helper.SetupClient(compose.GetWeaviateNode(node).URI())
	})
}

func TestGRPC_AuthzBatching(t *testing.T) {
	ctx := context.Background()

	adminUser := "admin-user"
	adminKey := "admin-key"
	customUser := "custom-user"
	customKey := "custom-key"

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithRBAC().
		WithApiKey().
		WithRbacRoots(adminUser).
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(customUser, customKey).
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
		helper.DeleteClassAuth(t, clsA.Class, adminKey)
		helper.DeleteClassAuth(t, clsP.Class, adminKey)
		// Create the schema
		helper.CreateClassAuth(t, clsP, adminKey)
		helper.CreateClassAuth(t, clsA, adminKey)
		return func() {
			helper.DeleteClassAuth(t, clsA.Class, adminKey)
			helper.DeleteClassAuth(t, clsP.Class, adminKey)
		}
	}

	t.Run("send objects and references without errors", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient, adminKey)

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
		stop(stream)
		stream.CloseSend()

		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "receiving from stream should not return an error")
			require.Len(t, msg.GetResults().GetErrors(), 0, "received message should not contain an error")
		}

		// Validate the number of articles created
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			listA, err := helper.ListObjectsAuth(t, clsA.Class, adminKey)
			require.NoError(t, err, "ListObjects should not return an error")
			require.Len(ct, listA.Objects, 1, "Number of articles created should match the number sent")
			require.NotNil(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], "hasParagraphs should not be nil")
			require.Len(ct, listA.Objects[0].Properties.(map[string]any)["hasParagraphs"], 2, "Article should have 2 paragraphs")

			listP, err := helper.ListObjectsAuth(t, clsP.Class, adminKey)
			require.NoError(t, err, "ListObjects should not return an error")
			require.Len(ct, listP.Objects, 2, "Number of paragraphs created should match the number sent")
		}, 10*time.Second, 1*time.Second, "Objects not created within time")
	})

	t.Run("fail to start stream due to lacking auth", func(t *testing.T) {
		defer setupClasses()()

		// Open up a stream to read messages from
		stream, err := grpcClient.BatchStream(ctx)
		require.NoError(t, err, "BatchStream should not return an error")

		// Read the first "Started" message, which should now error
		_, err = stream.Recv()
		require.Error(t, err, "BatchStream Recv should return an error")
		require.Contains(t, err.Error(), "unauthorized: invalid api key")
	})

	t.Run("send objects that should authz error and read the errors correctly", func(t *testing.T) {
		defer setupClasses()()

		// Create and assign role that can only create Articles
		roleName := "article-creator"
		helper.CreateRole(t, adminKey, &models.Role{Name: &roleName, Permissions: []*models.Permission{
			{
				Action: &authorization.CreateData,
				Data:   &models.PermissionData{Collection: &clsA.Class},
			},
			{
				Action: &authorization.UpdateData,
				Data:   &models.PermissionData{Collection: &clsA.Class},
			},
		}})
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer func() {
			helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
			helper.DeleteRole(t, adminKey, roleName)
		}()

		// Open up a stream to read messages from
		stream := start(ctx, t, grpcClient, customKey)

		uuid0 := uuid.NewString()
		uuid1 := uuid.NewString()
		uuid2 := uuid.NewString()

		// Send some articles and paragraphs in send message
		objects := []*pb.BatchObject{
			{Collection: clsA.Class, Uuid: uuid0},
			{Collection: clsA.Class, Uuid: uuid1},
			{Collection: clsP.Class, Uuid: uuid2},
		}
		err := send(stream, objects, nil)
		require.NoError(t, err, "sending Objects over the stream should not return an error")
		stop(stream)
		stream.CloseSend()

		// Read the error message
		msg, err := stream.Recv()
		require.NoError(t, err, "BatchStream should return a response")
		require.NotNil(t, msg, "Message should not be nil")
		require.NotNil(t, msg.GetResults(), "Results message should not be nil")

		require.Equal(t, "rbac: authorization, forbidden action: user 'custom-user' has insufficient permissions to update_data [[Domain: data, Collection: Paragraph, Tenant: *, Object: *]]", msg.GetResults().GetErrors()[0].Error)
		require.Equal(t, objects[2].Uuid, msg.GetResults().GetErrors()[0].GetUuid(), "Errored object should be the third one")
	})
}

func start(ctx context.Context, t *testing.T, grpcClient pb.WeaviateClient, key string) pb.Weaviate_BatchStreamClient {
	if key != "" {
		ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", key))
	}
	stream, err := grpcClient.BatchStream(ctx)
	require.NoError(t, err, "BatchStream should not return an error")

	// Send request to start the batching process
	err = stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}},
	})
	require.NoError(t, err, "sending Start over the stream should not return an error")

	// Read the first "Started" message
	msg, err := stream.Recv()
	require.NoError(t, err, "BatchStream Recv should not return an error")
	require.NotNil(t, msg.GetStarted(), "First message should be a Started message")

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

func stop(stream pb.Weaviate_BatchStreamClient) {
	// Send request to stop the batching process
	err := stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
	})
	require.NoError(nil, err, "sending Stop over the stream should not return an error")
}

func toBeacon(ref *pb.BatchReference) string {
	return fmt.Sprintf("weaviate://localhost/%s/%s/%s", ref.FromCollection, ref.FromUuid, ref.Name)
}

func randomByteVector(dim int) []byte {
	vector := make([]byte, dim*4)

	for i := 0; i < dim; i++ {
		binary.LittleEndian.PutUint32(vector[i*4:i*4+4], math.Float32bits(rand.Float32()))
	}

	return vector
}

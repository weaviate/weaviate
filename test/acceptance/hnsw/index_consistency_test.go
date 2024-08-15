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

package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"google.golang.org/grpc"
)

func newGRPCClient(t *testing.T, address string) (pb.WeaviateClient, *grpc.ClientConn) {
	conn, err := helper.CreateGrpcConnectionClient(address)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}

func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}

func randomByteVector(dim int) []byte {
	vector := make([]byte, dim*4)

	for i := 0; i < dim; i++ {
		binary.LittleEndian.PutUint32(vector[i*4:i*4+4], math.Float32bits(rand.Float32()))
	}

	return vector
}

func batchInsert(t *testing.T, ctx context.Context, grpcClient pb.WeaviateClient, className string, batchSize int) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context done in batchInsert")
			return
		default:
			batch := make([]*pb.BatchObject, batchSize)
			for j := 0; j < batchSize; j++ {
				batch[j] = &pb.BatchObject{
					Collection:  className,
					VectorBytes: randomByteVector(256),
					Uuid:        uuid.New().String(),
				}
			}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			reply, err := grpcClient.BatchObjects(ctx, &pb.BatchObjectsRequest{
				Objects: batch,
			})
			require.NoError(t, err)
			if len(reply.Errors) > 0 {
				t.Errorf(reply.Errors[0].Error)
			}
			fmt.Printf("Inserted %d objects\n", batchSize)
			time.Sleep(3 * time.Second)
		}
	}
}

func patchMany(t *testing.T, ctx context.Context, grpcClient pb.WeaviateClient, className string, howMany uint32) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context done in patchMany")
			return
		default:
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			reply, err := grpcClient.Search(ctx, &pb.SearchRequest{
				Collection: className,
				NearVector: &pb.NearVector{
					VectorBytes: randomByteVector(256),
				},
				Limit:       howMany,
				Metadata:    &pb.MetadataRequest{Uuid: true},
				Uses_123Api: true,
				Filters: &pb.Filters{
					Operator: pb.Filters_OPERATOR_LESS_THAN_EQUAL,
					TestValue: &pb.Filters_ValueText{
						ValueText: time.Now().UTC().Add(-time.Second * 10).Format(time.RFC3339), // helps to avoid patching objects that are being indexed
					},
					Target: &pb.FilterTarget{
						Target: &pb.FilterTarget_Property{
							Property: "_creationTimeUnix",
						},
					},
				},
			})
			require.NoError(t, err)
			if len(reply.Results) == 0 {
				time.Sleep(1 * time.Second)
			}
			for _, result := range reply.Results {
				require.NotNil(t, result)
				func(id string) {
					object := &models.Object{
						Class:  className,
						ID:     strfmt.UUID(id),
						Vector: randomVector(256),
					}
					params := objects.NewObjectsPatchParams().WithID(object.ID).WithBody(object)
					resp, err := helper.Client(t).Objects.ObjectsPatch(params, nil)
					if err != nil {
						switch x := (err).(type) {
						case *objects.ObjectsPatchNotFound:
							return
						case *objects.ObjectsPatchInternalServerError:
							t.Logf(x.Payload.Error[0].Message)
							return
						default:
							t.Logf(err.Error())
						}
					}
					helper.AssertRequestOk(t, resp, err, nil)
				}(result.Metadata.Id)
			}
			fmt.Printf("Patched %d objects\n", len(reply.Results))
			time.Sleep(1 * time.Second)
		}
	}
}

func deleteMany(t *testing.T, ctx context.Context, grpcClient pb.WeaviateClient, className string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context done in deleteMany")
			return
		default:
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			when := time.Now().UTC().Add(-time.Second * 20).Format(time.RFC3339)
			fmt.Println("Deleting objects older than", when)
			reply, err := grpcClient.BatchDelete(ctx, &pb.BatchDeleteRequest{
				Collection: className,
				Filters: &pb.Filters{
					Operator: pb.Filters_OPERATOR_LESS_THAN_EQUAL,
					TestValue: &pb.Filters_ValueText{
						ValueText: when,
					},
					Target: &pb.FilterTarget{
						Target: &pb.FilterTarget_Property{
							Property: "_creationTimeUnix",
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), reply.Failed)
			if reply.Matches == 0 {
				fmt.Println("No objects to delete, sleeping for 5s")
				time.Sleep(5 * time.Second)
			} else {
				fmt.Printf("Deleted %d objects\n", reply.Successful)
			}
		}
	}
}

func runOps(t *testing.T, ctx context.Context, client pb.WeaviateClient, className string) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		batchInsert(t, ctx, client, className, 1000)
	}(ctx)
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		patchMany(t, ctx, client, className, 100)
	}(ctx)
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		deleteMany(t, ctx, client, className)
	}(ctx)
	wg.Wait()
}

func assertGraphConsistencies(t *testing.T, compose *docker.DockerCompose, className string, isCluster bool) {
	nodes := helper.GetNodes(t)
	for _, node := range nodes {
		var profilingUri string
		if isCluster {
			profilingUri = compose.GetContainerByClusterHostName(node.Name).GetEndpoint(docker.PROF)
		} else {
			profilingUri = compose.GetWeaviate().GetEndpoint(docker.PROF)
		}
		for _, shard := range node.Shards {
			require.NotNil(t, shard)

			resp, err := http.Get(fmt.Sprintf("http://%s/debug/graph/collection/%s/shards/%s", profilingUri, className, shard.Name))
			require.NoError(t, err)
			defer resp.Body.Close()

			var data *rest.DebugGraph
			decoder := json.NewDecoder(resp.Body)
			err = decoder.Decode(&data)
			require.NoError(t, err)

			require.Equal(t, []int{0, 0}, []int{len(data.Ghosts), len(data.Orphans)})
		}
	}
}

func graphConsistencyNonReplicated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, _ := newGRPCClient(t, compose.GetWeaviate().GetEndpoint(docker.GRPC))

	className := "NonReplicatedClass"
	defer helper.DeleteClass(t, className)

	t.Run("Create class", func(t *testing.T) {
		helper.CreateClass(t, &models.Class{
			Class: className,
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps: true,
			},
			ReplicationConfig: &models.ReplicationConfig{
				Factor: 1,
			},
			Vectorizer: "none",
		})
	})

	t.Run("Perform concurrent operations", func(t *testing.T) {
		runOps(t, ctx, grpcClient, className)
	})

	t.Run("Wait for any async indexing to finish", func(t *testing.T) {
		ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()
		helper.WaitForAsyncIndexing(t, ctx)
		fmt.Println("Async indexing finished")
	})

	t.Run("Call the graph debug endpoints to check for consistency", func(t *testing.T) {
		time.Sleep(5 * time.Second) // give index time to ensure any orphans are successfully added to the graph
		assertGraphConsistencies(t, compose, className, false)
	})
}

func graphConsistencyReplicated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, _ := newGRPCClient(t, compose.GetWeaviate().GetEndpoint(docker.GRPC))

	className := "ReplicatedClass"
	defer helper.DeleteClass(t, className)

	t.Run("Create class", func(t *testing.T) {
		helper.CreateClass(t, &models.Class{
			Class: className,
			InvertedIndexConfig: &models.InvertedIndexConfig{
				IndexTimestamps: true,
			},
			ReplicationConfig: &models.ReplicationConfig{
				Factor: 2,
			},
			Vectorizer: "none",
		})
	})

	t.Run("Perform concurrent operations", func(t *testing.T) {
		runOps(t, ctx, grpcClient, className)
	})

	t.Run("Wait for any async indexing to finish", func(t *testing.T) {
		ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()
		helper.WaitForAsyncIndexing(t, ctx)
		fmt.Println("Async indexing finished")
	})

	t.Run("Call the graph debug endpoints to check for consistency", func(t *testing.T) {
		time.Sleep(5 * time.Second) // give index time to ensure any orphans are successfully added to the graph
		assertGraphConsistencies(t, compose, className, true)
	})
}

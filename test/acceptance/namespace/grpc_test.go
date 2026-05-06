//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package namespace

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func newGrpcClient(t *testing.T) (pb.WeaviateClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(sharedCompose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	require.NotNil(t, conn)
	return helper.CreateGrpcWeaviateClient(conn), conn
}

func authCtx(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", key))
}

// searchReq builds a SearchRequest with the API-version flags every gRPC
// caller in this suite needs. Tests can mutate the returned struct (e.g. to
// add Properties/Metadata) before sending.
func searchReq(collection string, limit uint32) *pb.SearchRequest {
	return &pb.SearchRequest{
		Collection:  collection,
		Limit:       limit,
		Uses_123Api: true,
		Uses_125Api: true,
		Uses_127Api: true,
	}
}

// TestNamespaces_GRPC exercises namespace fan-out for the gRPC endpoints
// (BatchObjects, Search, Aggregate). Each namespaced caller submits the
// short collection name; the handler must qualify it via namespacing.Resolve
// so the request only ever touches the caller's namespace shard.
func TestNamespaces_GRPC(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "MoviesGRPC"
	setupClassInBothNamespaces(t, class, user1Key, user2Key)

	id1 := strfmt.UUID("aaaaaaaa-1111-1111-1111-111111111111")
	id2 := strfmt.UUID("bbbbbbbb-2222-2222-2222-222222222222")

	makeBatchObj := func(id strfmt.UUID, collection, title string) *pb.BatchObject {
		return &pb.BatchObject{
			Uuid:       id.String(),
			Collection: collection,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title": structpb.NewStringValue(title),
					},
				},
			},
		}
	}

	t.Run("BatchObjects fans out per namespace", func(t *testing.T) {
		// user1 inserts under the short class name; the handler qualifies
		// to customer1:MoviesGRPC.
		resp, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				makeBatchObj(id1, class, "ns1-The Matrix"),
				makeBatchObj(id2, class, "ns1-Inception"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.Errors)

		// user2 inserts the same UUIDs under the same short class name;
		// the handler qualifies to customer2:MoviesGRPC, so there is no
		// id collision between the two namespaces.
		resp, err = grpcClient.BatchObjects(authCtx(user2Key), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				makeBatchObj(id1, class, "ns2-Memento"),
				makeBatchObj(id2, class, "ns2-Tenet"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.Errors)

		// Cross-check via the REST get path that the rows landed under their
		// respective qualified class names.
		got1, err := helper.GetObjectAuth(t, class, id1, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "customer1:"+class, got1.Class)
		assert.Equal(t, "ns1-The Matrix", got1.Properties.(map[string]any)["title"])

		got2, err := helper.GetObjectAuth(t, class, id1, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "customer2:"+class, got2.Class)
		assert.Equal(t, "ns2-Memento", got2.Properties.(map[string]any)["title"])
	})

	t.Run("Search is namespace-scoped", func(t *testing.T) {
		req := func(key string) (*pb.SearchReply, error) {
			r := searchReq(class, 10)
			r.Properties = &pb.PropertiesRequest{NonRefProperties: []string{"title"}}
			r.Metadata = &pb.MetadataRequest{Uuid: true}
			return grpcClient.Search(authCtx(key), r)
		}

		resp1, err := req(user1Key)
		require.NoError(t, err)
		require.Len(t, resp1.Results, 2)
		titles1 := []string{
			resp1.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue(),
			resp1.Results[1].Properties.NonRefProps.Fields["title"].GetTextValue(),
		}
		assert.ElementsMatch(t, []string{"ns1-The Matrix", "ns1-Inception"}, titles1)

		resp2, err := req(user2Key)
		require.NoError(t, err)
		require.Len(t, resp2.Results, 2)
		titles2 := []string{
			resp2.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue(),
			resp2.Results[1].Properties.NonRefProps.Fields["title"].GetTextValue(),
		}
		assert.ElementsMatch(t, []string{"ns2-Memento", "ns2-Tenet"}, titles2)
	})

	t.Run("Aggregate count is namespace-scoped", func(t *testing.T) {
		// gRPC Aggregate is not yet namespace-aware. Two gaps stand in the
		// way: the handler doesn't qualify the class via namespacing.Resolve,
		// and count(*) fan-out goes through a cluster-API HTTP route whose
		// URL regex is built from ClassNameRegexCore — that regex excludes
		// ":", so namespace-qualified index names never match the route.
		t.Skip("aggregate on namespaced classes not yet wired; tracked separately")

		for _, key := range []string{user1Key, user2Key} {
			resp, err := grpcClient.Aggregate(authCtx(key), &pb.AggregateRequest{
				Collection:   class,
				ObjectsCount: true,
			})
			require.NoError(t, err)
			require.NotNil(t, resp.GetSingleResult())
			assert.Equal(t, int64(2), resp.GetSingleResult().GetObjectsCount())
		}
	})

	t.Run("Search via alias resolves per namespace", func(t *testing.T) {
		// Both namespaces register the same short alias name pointing at
		// their own copy of MoviesGRPC. The handler must qualify the alias
		// to "<ns>:MoviesGRPCAlias" before alias→target lookup, otherwise
		// user1's request would land on user2's data (or vice versa).
		// Aggregate-via-alias is intentionally not exercised — gRPC Aggregate
		// is not yet namespace-aware (see Aggregate subtest above).
		const aliasName = "MoviesGRPCAlias"
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.Client(t).Schema.AliasesCreate(
				schemaCli.NewAliasesCreateParams().WithBody(&models.Alias{Alias: aliasName, Class: class}),
				helper.CreateAuth(key),
			)
			require.NoError(t, err)
		}
		defer helper.Client(t).Schema.AliasesDelete(
			schemaCli.NewAliasesDeleteParams().WithAliasName("customer1:"+aliasName),
			helper.CreateAuth(adminKey),
		)
		defer helper.Client(t).Schema.AliasesDelete(
			schemaCli.NewAliasesDeleteParams().WithAliasName("customer2:"+aliasName),
			helper.CreateAuth(adminKey),
		)

		for _, key := range []string{user1Key, user2Key} {
			searchResp, err := grpcClient.Search(authCtx(key), searchReq(aliasName, 10))
			require.NoError(t, err)
			assert.Len(t, searchResp.Results, 2)
		}
	})

	t.Run("namespaced caller submitting :-qualified collection double-prefixes", func(t *testing.T) {
		// user1 supplying "customer1:MoviesGRPC" gets qualified again to
		// "customer1:customer1:MoviesGRPC" — no such class on disk.
		// Aggregate is not exercised here — gRPC Aggregate is not yet
		// namespace-aware (see Aggregate subtest above).
		qualified := "customer1:" + class

		_, err := grpcClient.Search(authCtx(user1Key), searchReq(qualified, 1))
		require.Error(t, err)

		batchResp, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				makeBatchObj(strfmt.UUID("dddddddd-4444-4444-4444-444444444444"), qualified, "double"),
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, batchResp.Errors)
	})

	t.Run("global admin succeeds with qualified name and fails with short name", func(t *testing.T) {
		// Admin has no namespace, so qualify is a no-op and the supplied
		// name flows through. Short name → no class on disk → error.
		// Qualified name → matches storage → success.
		// Aggregate is not exercised — gRPC Aggregate is not yet
		// namespace-aware (see Aggregate subtest above).
		shortReq := func() (*pb.SearchReply, error) {
			return grpcClient.Search(authCtx(adminKey), searchReq(class, 10))
		}
		qualifiedReq := func() (*pb.SearchReply, error) {
			return grpcClient.Search(authCtx(adminKey), searchReq("customer1:"+class, 10))
		}

		_, err := shortReq()
		require.Error(t, err)
		searchResp, err := qualifiedReq()
		require.NoError(t, err)
		assert.Len(t, searchResp.Results, 2)

		// BatchObjects: short → per-object error; qualified → success and
		// the row shows up via REST.
		shortBatch, err := grpcClient.BatchObjects(authCtx(adminKey), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{makeBatchObj(strfmt.UUID("eeeeeeee-5555-5555-5555-555555555555"), class, "admin-short")},
		})
		require.NoError(t, err)
		require.NotEmpty(t, shortBatch.Errors)

		adminID := strfmt.UUID("ffffffff-6666-6666-6666-666666666666")
		qualifiedBatch, err := grpcClient.BatchObjects(authCtx(adminKey), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{makeBatchObj(adminID, "customer1:"+class, "admin-qualified")},
		})
		require.NoError(t, err)
		require.Empty(t, qualifiedBatch.Errors)

		got, err := helper.GetObjectAuth(t, "customer1:"+class, adminID, adminKey)
		require.NoError(t, err)
		assert.Equal(t, "customer1:"+class, got.Class)
		assert.Equal(t, "admin-qualified", got.Properties.(map[string]any)["title"])
	})
}

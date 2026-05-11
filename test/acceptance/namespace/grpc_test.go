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

	t.Run("BatchDelete by filter is namespace-scoped", func(t *testing.T) {
		// Self-contained against the rest of TestNamespaces_GRPC: own class,
		// own UUIDs, no reliance on (or mutation of) the MoviesGRPC rows above.
		const delClass = "BatchDeleteGRPC"
		setupClassInBothNamespaces(t, delClass, user1Key, user2Key)

		killID := strfmt.UUID("33333333-cccc-cccc-cccc-cccccccccccc")
		keepID := strfmt.UUID("44444444-dddd-dddd-dddd-dddddddddddd")

		seedDelete := func(key, killTitle, keepTitle string) {
			r, err := grpcClient.BatchObjects(authCtx(key), &pb.BatchObjectsRequest{
				Objects: []*pb.BatchObject{
					makeBatchObj(killID, delClass, killTitle),
					makeBatchObj(keepID, delClass, keepTitle),
				},
			})
			require.NoError(t, err)
			require.Empty(t, r.Errors)
		}
		seedDelete(user1Key, "kill", "keep")
		seedDelete(user2Key, "kill", "keep")

		// Delete by filter from user1: title == "kill" → only ns1's kill row.
		delResp, err := grpcClient.BatchDelete(authCtx(user1Key), &pb.BatchDeleteRequest{
			Collection: delClass,
			Filters: &pb.Filters{
				Operator:  pb.Filters_OPERATOR_EQUAL,
				TestValue: &pb.Filters_ValueText{ValueText: "kill"},
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), delResp.Successful)
		assert.Equal(t, int64(0), delResp.Failed)
		assert.Equal(t, int64(1), delResp.Matches)

		// ns1: kill is gone, keep survives.
		_, err = helper.GetObjectAuth(t, delClass, killID, user1Key)
		require.Error(t, err)
		got1Keep, err := helper.GetObjectAuth(t, delClass, keepID, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got1Keep.Properties.(map[string]any)["title"])

		// ns2: both rows untouched.
		got2Kill, err := helper.GetObjectAuth(t, delClass, killID, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "kill", got2Kill.Properties.(map[string]any)["title"])
		got2Keep, err := helper.GetObjectAuth(t, delClass, keepID, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got2Keep.Properties.(map[string]any)["title"])
	})

	t.Run("BatchDelete by filter via namespace-local alias", func(t *testing.T) {
		// Each namespace registers its own short alias for its copy of the
		// class. namespacing.Resolve in the gRPC handler resolves the alias
		// after qualifying with the principal's namespace, so user1's delete
		// only touches ns1 data.
		const (
			delClass = "BatchDeleteAliasGRPC"
			alias    = "BDAliasGRPC"
		)
		setupClassInBothNamespaces(t, delClass, user1Key, user2Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: delClass}, user1Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: delClass}, user2Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+alias, helper.CreateAuth(adminKey))
			helper.DeleteAliasWithAuthz(t, "customer2:"+alias, helper.CreateAuth(adminKey))
		})

		killID := strfmt.UUID("77777777-7777-7777-7777-777777777777")
		keepID := strfmt.UUID("88888888-8888-8888-8888-888888888888")

		seed := func(key string) {
			r, err := grpcClient.BatchObjects(authCtx(key), &pb.BatchObjectsRequest{
				Objects: []*pb.BatchObject{
					makeBatchObj(killID, delClass, "kill"),
					makeBatchObj(keepID, delClass, "keep"),
				},
			})
			require.NoError(t, err)
			require.Empty(t, r.Errors)
		}
		seed(user1Key)
		seed(user2Key)

		// Delete via the alias — retry to absorb leader→follower alias lag.
		var delResp *pb.BatchDeleteReply
		retryOnAliasLag(t, func() error {
			var err error
			delResp, err = grpcClient.BatchDelete(authCtx(user1Key), &pb.BatchDeleteRequest{
				Collection: alias,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: "kill"},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
				},
			})
			return err
		})
		assert.Equal(t, int64(1), delResp.Successful)
		assert.Equal(t, int64(0), delResp.Failed)

		// ns1: kill gone, keep survives.
		_, err := helper.GetObjectAuth(t, delClass, killID, user1Key)
		require.Error(t, err)
		got1Keep, err := helper.GetObjectAuth(t, delClass, keepID, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got1Keep.Properties.(map[string]any)["title"])

		// ns2 untouched.
		got2Kill, err := helper.GetObjectAuth(t, delClass, killID, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "kill", got2Kill.Properties.(map[string]any)["title"])
		got2Keep, err := helper.GetObjectAuth(t, delClass, keepID, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got2Keep.Properties.(map[string]any)["title"])
	})

	t.Run("BatchDelete as global admin via qualified class name", func(t *testing.T) {
		// Admin has no namespace, so namespacing.Resolve is a no-op on the
		// class portion: the qualified name flows through to storage and
		// matches; the short name does not exist on disk.
		const delClass = "BatchDeleteAdminGRPC"
		setupClassInNs1(t, delClass, user1Key)

		killID := strfmt.UUID("bbbbbbbb-3333-3333-3333-333333333333")
		keepID := strfmt.UUID("cccccccc-4444-4444-4444-444444444444")
		r, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				makeBatchObj(killID, delClass, "kill"),
				makeBatchObj(keepID, delClass, "keep"),
			},
		})
		require.NoError(t, err)
		require.Empty(t, r.Errors)

		mkReq := func(collection string) *pb.BatchDeleteRequest {
			return &pb.BatchDeleteRequest{
				Collection: collection,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: "kill"},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
				},
			}
		}

		// Short class name → not found in storage, request errors.
		_, err = grpcClient.BatchDelete(authCtx(adminKey), mkReq(delClass))
		require.Error(t, err)

		// Qualified class name → resolves to storage, delete succeeds.
		delResp, err := grpcClient.BatchDelete(authCtx(adminKey), mkReq("customer1:"+delClass))
		require.NoError(t, err)
		assert.Equal(t, int64(1), delResp.Successful)
		assert.Equal(t, int64(0), delResp.Failed)

		// kill is gone, keep survives — verify via the namespaced user.
		_, err = helper.GetObjectAuth(t, delClass, killID, user1Key)
		require.Error(t, err)
		got, err := helper.GetObjectAuth(t, delClass, keepID, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got.Properties.(map[string]any)["title"])
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

	t.Run("Search and Aggregate via alias resolve per namespace", func(t *testing.T) {
		// Both namespaces register the same short alias name pointing at
		// their own copy of MoviesGRPC. The handler must qualify the alias
		// to "<ns>:MoviesGRPCAlias" before alias→target lookup, otherwise
		// user1's request would land on user2's data (or vice versa).
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
			// retryOnAliasLag absorbs the brief window where the alias
			// entry has been applied on the leader but the follower has
			// not yet replicated it. Once the first Search succeeds the
			// alias is locally visible and the Aggregate below is safe.
			var searchResp *pb.SearchReply
			retryOnAliasLag(t, func() error {
				var err error
				searchResp, err = grpcClient.Search(authCtx(key), searchReq(aliasName, 10))
				return err
			})
			assert.Len(t, searchResp.Results, 2)

			aggResp, err := grpcClient.Aggregate(authCtx(key), &pb.AggregateRequest{
				Collection:   aliasName,
				ObjectsCount: true,
			})
			require.NoError(t, err)
			require.NotNil(t, aggResp.GetSingleResult())
			assert.Equal(t, int64(2), aggResp.GetSingleResult().GetObjectsCount())
		}
	})

	t.Run("namespaced caller submitting :-qualified collection double-prefixes", func(t *testing.T) {
		// user1 supplying "customer1:MoviesGRPC" gets qualified again to
		// "customer1:customer1:MoviesGRPC" — no such class on disk.
		qualified := "customer1:" + class

		_, err := grpcClient.Search(authCtx(user1Key), searchReq(qualified, 1))
		require.Error(t, err)

		_, err = grpcClient.Aggregate(authCtx(user1Key), &pb.AggregateRequest{
			Collection:   qualified,
			ObjectsCount: true,
		})
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

		_, err = grpcClient.Aggregate(authCtx(adminKey), &pb.AggregateRequest{
			Collection:   class,
			ObjectsCount: true,
		})
		require.Error(t, err)
		aggResp, err := grpcClient.Aggregate(authCtx(adminKey), &pb.AggregateRequest{
			Collection:   "customer1:" + class,
			ObjectsCount: true,
		})
		require.NoError(t, err)
		require.NotNil(t, aggResp.GetSingleResult())
		assert.Equal(t, int64(2), aggResp.GetSingleResult().GetObjectsCount())

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

// TestNamespaces_GRPC_MultiShardAggregate spreads a namespaced class across
// every node in the 3-node cluster and asserts that count(*) fan-out
// returns the right number. With shards on remote nodes the gRPC entry
// point cannot answer count locally and must hop the cluster-API
// /indices/<ns>:<class>/shards/<sh>/objects/_count route.
func TestNamespaces_GRPC_MultiShardAggregate(t *testing.T) {
	user1Key, _ := twoNamespaces(t)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "MoviesGRPCMultiShard"
	helper.CreateClassAuth(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
		ShardingConfig: map[string]any{"desiredCount": 3},
	}, user1Key)
	t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

	const objCount = 12
	objects := make([]*pb.BatchObject, 0, objCount)
	for i := range objCount {
		objects = append(objects, &pb.BatchObject{
			Uuid:       strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)).String(),
			Collection: class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title": structpb.NewStringValue(fmt.Sprintf("title-%d", i)),
					},
				},
			},
		})
	}
	resp, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{Objects: objects})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Errors)

	aggResp, err := grpcClient.Aggregate(authCtx(user1Key), &pb.AggregateRequest{
		Collection:   class,
		ObjectsCount: true,
	})
	require.NoError(t, err)
	require.NotNil(t, aggResp.GetSingleResult())
	assert.Equal(t, int64(objCount), aggResp.GetSingleResult().GetObjectsCount())
}

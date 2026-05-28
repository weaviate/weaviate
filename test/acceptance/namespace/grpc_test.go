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
	"errors"
	"fmt"
	"io"
	"strings"
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
		assert.Equal(t, class, got1.Class)
		assert.Equal(t, "ns1-The Matrix", got1.Properties.(map[string]any)["title"])

		got2, err := helper.GetObjectAuth(t, class, id1, user2Key)
		require.NoError(t, err)
		assert.Equal(t, class, got2.Class)
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
		// target_collection: namespaced caller sees the short class name.
		assert.Equal(t, class, resp1.Results[0].Properties.TargetCollection)
		assert.Equal(t, class, resp1.Results[1].Properties.TargetCollection)

		resp2, err := req(user2Key)
		require.NoError(t, err)
		require.Len(t, resp2.Results, 2)
		titles2 := []string{
			resp2.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue(),
			resp2.Results[1].Properties.NonRefProps.Fields["title"].GetTextValue(),
		}
		assert.ElementsMatch(t, []string{"ns2-Memento", "ns2-Tenet"}, titles2)
		assert.Equal(t, class, resp2.Results[0].Properties.TargetCollection)
		assert.Equal(t, class, resp2.Results[1].Properties.TargetCollection)
	})

	t.Run("Search by filter is namespace-scoped", func(t *testing.T) {
		// Filter parser sanity check on the gRPC Search path: each
		// namespace's row carries its own ns-prefixed title, so an exact
		// match on "ns1-Inception" only hits user1's namespace and vice
		// versa. Proves the qualified-class filter path round-trips
		// through Search the same way it does through BatchDelete.
		searchWithFilter := func(key, title string) (*pb.SearchReply, error) {
			r := searchReq(class, 10)
			r.Properties = &pb.PropertiesRequest{NonRefProperties: []string{"title"}}
			r.Filters = &pb.Filters{
				Operator:  pb.Filters_OPERATOR_EQUAL,
				TestValue: &pb.Filters_ValueText{ValueText: title},
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
			}
			return grpcClient.Search(authCtx(key), r)
		}

		// user1 matches their own row; user2 sees nothing for the same filter.
		resp1, err := searchWithFilter(user1Key, "ns1-Inception")
		require.NoError(t, err)
		require.Len(t, resp1.Results, 1)
		assert.Equal(t, "ns1-Inception", resp1.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())

		resp2Miss, err := searchWithFilter(user2Key, "ns1-Inception")
		require.NoError(t, err)
		assert.Empty(t, resp2Miss.Results)

		// And the symmetric case: user2's row is only visible to user2.
		resp2, err := searchWithFilter(user2Key, "ns2-Tenet")
		require.NoError(t, err)
		require.Len(t, resp2.Results, 1)
		assert.Equal(t, "ns2-Tenet", resp2.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue())

		resp1Miss, err := searchWithFilter(user1Key, "ns2-Tenet")
		require.NoError(t, err)
		assert.Empty(t, resp1Miss.Results)
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

	// Namespace isolation for Aggregate GroupBy: each namespace sees only
	// its own seeded buckets, and admin can still hit the qualified class.
	t.Run("Aggregate GroupBy on a string property succeeds on NS cluster", func(t *testing.T) {
		req := &pb.AggregateRequest{
			Collection:   class,
			ObjectsCount: true,
			GroupBy:      &pb.AggregateRequest_GroupBy{Collection: class, Property: "title"},
		}
		bucketsFor := func(t *testing.T, key string) []string {
			t.Helper()
			resp, err := grpcClient.Aggregate(authCtx(key), req)
			require.NoError(t, err)
			groups := resp.GetGroupedResults().GetGroups()
			require.NotEmpty(t, groups, "expected groupedResults for caller %q", key)
			out := make([]string, 0, len(groups))
			for _, g := range groups {
				out = append(out, g.GroupedBy.GetText())
			}
			return out
		}

		buckets1 := bucketsFor(t, user1Key)
		buckets2 := bucketsFor(t, user2Key)
		assert.NotEqual(t, buckets1, buckets2,
			"buckets should differ between namespaces; got %v on both", buckets1)

		respAdmin, err := grpcClient.Aggregate(authCtx(adminKey), &pb.AggregateRequest{
			Collection:   "customer1:" + class,
			ObjectsCount: true,
			GroupBy:      &pb.AggregateRequest_GroupBy{Collection: "customer1:" + class, Property: "title"},
		})
		require.NoError(t, err)
		require.NotEmpty(t, respAdmin.GetGroupedResults().GetGroups(),
			"admin's aggregate over the qualified class must return the same shape")
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
			// target_collection is the resolved class name, stripped to the
			// caller's namespace — never the alias the caller typed.
			assert.Equal(t, class, searchResp.Results[0].Properties.TargetCollection)
			assert.Equal(t, class, searchResp.Results[1].Properties.TargetCollection)

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
		// Admin sees the raw qualified class name in target_collection.
		assert.Equal(t, "customer1:"+class, searchResp.Results[0].Properties.TargetCollection)
		assert.Equal(t, "customer1:"+class, searchResp.Results[1].Properties.TargetCollection)

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

	// runBatchStream opens a BatchStream with key, sends one Data message
	// containing objs, then Stop+CloseSend. Returns (successes, errors)
	// aggregated from all Results frames so callers can assert what they
	// expect (no errors for happy paths, per-object errors for short-name
	// admin sends, etc).
	runBatchStream := func(t *testing.T, key string, objs []*pb.BatchObject) (int, []*pb.BatchStreamReply_Results_Error) {
		t.Helper()
		stream, err := grpcClient.BatchStream(authCtx(key))
		require.NoError(t, err)

		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}},
		}))
		started, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, started.GetStarted())

		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Data_{Data: &pb.BatchStreamRequest_Data{
				Objects: &pb.BatchStreamRequest_Data_Objects{Values: objs},
			}},
		}))
		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
		}))
		require.NoError(t, stream.CloseSend())

		var successes int
		var errs []*pb.BatchStreamReply_Results_Error
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			if r := msg.GetResults(); r != nil {
				successes += len(r.GetSuccesses())
				errs = append(errs, r.GetErrors()...)
			}
		}
		return successes, errs
	}

	t.Run("BatchStream is namespace-scoped", func(t *testing.T) {
		// Cover the streaming path end-to-end for namespaced principals.
		// The stream receiver does a pre-flight schema lookup per incoming
		// data message to decide whether to fan out for vectoriser-backed
		// classes; with namespaces enabled that lookup must use the
		// qualified class name, otherwise the hint silently misses (and a
		// future change could surface the miss as a fatal stream error).
		// Driving real traffic through the stream also locks in that the
		// principal captured at Handle() is the one used per data message.
		const streamClass = "MoviesGRPCStream"
		setupClassInBothNamespaces(t, streamClass, user1Key, user2Key)

		streamID1 := strfmt.UUID("99999999-1111-1111-1111-111111111111")
		streamID2 := strfmt.UUID("99999999-2222-2222-2222-222222222222")

		// user1 inserts under the short class name; the stream receiver must
		// qualify to customer1:MoviesGRPCStream for the schema pre-flight,
		// and the worker must qualify again for the actual write.
		successes, errs := runBatchStream(t, user1Key, []*pb.BatchObject{
			{Uuid: streamID1.String(), Collection: streamClass, Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
					"title": structpb.NewStringValue("ns1-stream"),
				}},
			}},
		})
		require.Empty(t, errs)
		assert.Equal(t, 1, successes)

		// user2 reuses the same UUID under the same short class name. Without
		// namespace fan-out this would collide; with it, the two rows land
		// on separate shards.
		successes, errs = runBatchStream(t, user2Key, []*pb.BatchObject{
			{Uuid: streamID1.String(), Collection: streamClass, Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
					"title": structpb.NewStringValue("ns2-stream"),
				}},
			}},
			{Uuid: streamID2.String(), Collection: streamClass, Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
					"title": structpb.NewStringValue("ns2-stream-2"),
				}},
			}},
		})
		require.Empty(t, errs)
		assert.Equal(t, 2, successes)

		// Verify the rows landed under their respective qualified class names.
		got1, err := helper.GetObjectAuth(t, streamClass, streamID1, user1Key)
		require.NoError(t, err)
		assert.Equal(t, streamClass, got1.Class)
		assert.Equal(t, "ns1-stream", got1.Properties.(map[string]any)["title"])

		got2, err := helper.GetObjectAuth(t, streamClass, streamID1, user2Key)
		require.NoError(t, err)
		assert.Equal(t, streamClass, got2.Class)
		assert.Equal(t, "ns2-stream", got2.Properties.(map[string]any)["title"])

		// user2's second UUID exists in ns2 only.
		_, err = helper.GetObjectAuth(t, streamClass, streamID2, user1Key)
		require.Error(t, err)
		got3, err := helper.GetObjectAuth(t, streamClass, streamID2, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "ns2-stream-2", got3.Properties.(map[string]any)["title"])
	})

	t.Run("BatchStream via namespace-local alias resolves per namespace", func(t *testing.T) {
		// Each namespace registers the same short alias name for its own
		// copy of the class. namespacing.Resolve in the receiver qualifies
		// the alias with the principal's namespace before alias→target
		// lookup, so user1's stream must land on customer1's class and
		// user2's stream on customer2's.
		const (
			streamClass = "MoviesGRPCStreamAlias"
			aliasName   = "StreamAlias"
		)
		setupClassInBothNamespaces(t, streamClass, user1Key, user2Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: streamClass}, user1Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: streamClass}, user2Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+aliasName, helper.CreateAuth(adminKey))
			helper.DeleteAliasWithAuthz(t, "customer2:"+aliasName, helper.CreateAuth(adminKey))
		})

		id1 := strfmt.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		id2 := strfmt.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

		objFor := func(id strfmt.UUID, title string) *pb.BatchObject {
			return &pb.BatchObject{
				Uuid:       id.String(),
				Collection: aliasName,
				Properties: &pb.BatchObject_Properties{NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{"title": structpb.NewStringValue(title)},
				}},
			}
		}

		// retryOnAliasLag absorbs the brief leader→follower replication gap
		// after CreateAliasAuth. Each per-namespace alias replicates
		// independently, so both streams need their own retry.
		runStream := func(key string, obj *pb.BatchObject) {
			var successes int
			var errs []*pb.BatchStreamReply_Results_Error
			retryOnAliasLag(t, func() error {
				successes, errs = runBatchStream(t, key, []*pb.BatchObject{obj})
				if len(errs) > 0 {
					return fmt.Errorf("per-object errors: %v", errs[0].GetError())
				}
				return nil
			})
			assert.Equal(t, 1, successes)
		}
		runStream(user1Key, objFor(id1, "ns1-alias-stream"))
		runStream(user2Key, objFor(id2, "ns2-alias-stream"))

		// The alias resolved to each namespace's own copy of the class.
		got1, err := helper.GetObjectAuth(t, streamClass, id1, user1Key)
		require.NoError(t, err)
		assert.Equal(t, streamClass, got1.Class)
		assert.Equal(t, "ns1-alias-stream", got1.Properties.(map[string]any)["title"])

		got2, err := helper.GetObjectAuth(t, streamClass, id2, user2Key)
		require.NoError(t, err)
		assert.Equal(t, streamClass, got2.Class)
		assert.Equal(t, "ns2-alias-stream", got2.Properties.(map[string]any)["title"])

		// And cross-namespace lookups miss — user2 cannot see user1's row.
		_, err = helper.GetObjectAuth(t, streamClass, id1, user2Key)
		require.Error(t, err)
	})

	t.Run("BatchStream as global admin needs qualified class name", func(t *testing.T) {
		// Admin has no namespace, so namespacing.Resolve is a no-op on the
		// class portion. Short names don't exist on disk; qualified names
		// flow through to the namespaced shard.
		const streamClass = "MoviesGRPCStreamAdmin"
		setupClassInNs1(t, streamClass, user1Key)

		adminShortID := strfmt.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
		adminQualifiedID := strfmt.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")

		mkObj := func(id strfmt.UUID, collection, title string) *pb.BatchObject {
			return &pb.BatchObject{
				Uuid:       id.String(),
				Collection: collection,
				Properties: &pb.BatchObject_Properties{NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{"title": structpb.NewStringValue(title)},
				}},
			}
		}

		// Short name: stream itself accepts and acks the message (the
		// receiver's schema pre-flight is best-effort and does not fail
		// closed on miss), but the worker errors per-object because the
		// unqualified class does not exist.
		successes, errs := runBatchStream(t, adminKey, []*pb.BatchObject{
			mkObj(adminShortID, streamClass, "admin-short"),
		})
		assert.Zero(t, successes)
		require.Len(t, errs, 1)
		assert.Equal(t, adminShortID.String(), errs[0].GetUuid())

		// Qualified name: write lands on customer1's shard and is visible
		// via REST to admin and to user1.
		successes, errs = runBatchStream(t, adminKey, []*pb.BatchObject{
			mkObj(adminQualifiedID, "customer1:"+streamClass, "admin-qualified"),
		})
		require.Empty(t, errs)
		assert.Equal(t, 1, successes)

		got, err := helper.GetObjectAuth(t, "customer1:"+streamClass, adminQualifiedID, adminKey)
		require.NoError(t, err)
		assert.Equal(t, "customer1:"+streamClass, got.Class)
		assert.Equal(t, "admin-qualified", got.Properties.(map[string]any)["title"])

		// The short-name row was never persisted under any class.
		_, err = helper.GetObjectAuth(t, streamClass, adminShortID, user1Key)
		require.Error(t, err)
	})
}

// TestNamespaces_GRPC_QueryProfile checks that a namespaced caller never sees
// its own "<namespace>:" prefix in the per-shard query_profile. The shard ID
// (shard.ID()) embeds the qualified class name, so the raw profile Name is
// "customer1:moviesgrpcprofile_<shard>"; the replier strips the prefix via
// namespacing.StripOwnNamespace, the same helper used for target_collection.
func TestNamespaces_GRPC_QueryProfile(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "MoviesGRPCProfile"
	setupClassInBothNamespaces(t, class, user1Key, user2Key)

	// Seed one row in ns1 so a shard is actually searched and profiled.
	_, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
		Objects: []*pb.BatchObject{{
			Uuid:       strfmt.UUID("11111111-aaaa-aaaa-aaaa-111111111111").String(),
			Collection: class,
			Properties: &pb.BatchObject_Properties{NonRefProperties: &structpb.Struct{
				Fields: map[string]*structpb.Value{"title": structpb.NewStringValue("matrix")},
			}},
		}},
	})
	require.NoError(t, err)

	// BM25 keyword search exercises a profiled shard-search path; query_profile
	// asks for the per-shard breakdown in the reply.
	profReq := func(collection string) *pb.SearchRequest {
		r := searchReq(collection, 10)
		r.Bm25Search = &pb.BM25{Query: "matrix", Properties: []string{"title"}}
		r.Metadata = &pb.MetadataRequest{QueryProfile: true}
		return r
	}

	t.Run("namespaced caller must not see its namespace prefix in the profile", func(t *testing.T) {
		resp, err := grpcClient.Search(authCtx(user1Key), profReq(class))
		require.NoError(t, err)
		require.NotNil(t, resp.QueryProfile, "expected query_profile to be populated")
		require.NotEmpty(t, resp.QueryProfile.Shards)
		// Scan every string field of the profile, not just shard Name: the
		// prefix must not surface anywhere a future detail could embed a
		// qualified name (search-type keys, detail keys, detail values).
		notLeak := func(field, val string) {
			assert.NotContainsf(t, val, "customer1:",
				"query_profile leaks the caller's namespace prefix via %s: %q", field, val)
		}
		for _, sh := range resp.QueryProfile.Shards {
			notLeak("shard name", sh.Name)
			notLeak("shard node", sh.Node)
			for searchType, sp := range sh.Searches {
				notLeak("search type", searchType)
				for k, v := range sp.Details {
					notLeak("detail key", k)
					notLeak("detail value", v)
				}
			}
		}
	})

	t.Run("global admin keeps the raw profile (raw mode)", func(t *testing.T) {
		// Admin has no namespace, so it keeps the raw internal names: at least
		// one shard name must still carry the qualified "customer1:" prefix.
		// This is the positive control for the stripping above — without it a
		// regression that strips global names too would go unnoticed.
		resp, err := grpcClient.Search(authCtx(adminKey), profReq("customer1:"+class))
		require.NoError(t, err)
		require.NotNil(t, resp.QueryProfile)
		require.NotEmpty(t, resp.QueryProfile.Shards)
		hasPrefix := false
		for _, sh := range resp.QueryProfile.Shards {
			if strings.Contains(sh.Name, "customer1:") {
				hasPrefix = true
			}
		}
		assert.True(t, hasPrefix,
			"admin should see the raw qualified shard name with the customer1: prefix, got %v",
			resp.QueryProfile.Shards)
	})
}

// TestNamespaces_GRPC_RemoteShardAggregate exercises the count(*) fan-out
// hop when the namespace's home_node differs from the gRPC entry node.
// The namespace is pinned to a node that is provably not the gRPC entry
// (GetWeaviate() returns weaviate-0) so the hop is exercised every run.
func TestNamespaces_GRPC_RemoteShardAggregate(t *testing.T) {
	const (
		ns       = "remoteshardns"
		homeNode = "weaviate-2"
	)
	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	user1Key := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "MoviesGRPCRemoteShard"
	helper.CreateClassAuth(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
	}, user1Key)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+class, adminKey) })

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

// TestNamespaces_GRPC_BatchAutoSchema guards the gRPC batch + auto-schema path
// for a namespaced principal: the class does not exist yet, so the handler must
// qualify the object's collection so the auto-created collection and the write
// agree on the qualified name.
func TestNamespaces_GRPC_BatchAutoSchema(t *testing.T) {
	user1Key, _ := twoNamespaces(t)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "GrpcAutoSchema"
	t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

	id := strfmt.UUID("99999999-aaaa-bbbb-cccc-999999999999")
	resp, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
		Objects: []*pb.BatchObject{{
			Uuid:       id.String(),
			Collection: class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"title": structpb.NewStringValue("Inception"),
					},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Errors)

	// Read-back by short name proves the object landed under the qualified collection.
	got, err := helper.GetObjectAuth(t, class, id, user1Key)
	require.NoError(t, err)
	assert.Equal(t, class, got.Class)
	assert.Equal(t, "Inception", got.Properties.(map[string]any)["title"])

	// Admin's raw view confirms auto-schema created a single-qualified class.
	gotClass := helper.GetClassAuth(t, "customer1:"+class, adminKey)
	assert.Equal(t, "customer1:"+class, gotClass.Class)
}

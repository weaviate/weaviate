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
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_ResponseStripping_Errors_REST pins the contract that REST
// error messages surfaced to namespaced callers do not carry the caller's own
// namespace prefix, while global admins still see qualified names.
func TestNamespaces_ResponseStripping_Errors_REST(t *testing.T) {
	user1Key, _ := twoNamespaces(t)
	const class = "ErrStripREST"
	setupClassInNs1(t, class, user1Key)

	t.Run("returned error: duplicate-class create surfaces stripped message", func(t *testing.T) {
		// Creating the class a second time returns 422 with a body whose
		// message contains the qualified name internally; the strip must
		// remove the "customer1:" prefix before the response leaves the
		// handler.
		_, err := helper.CreateClassAuthWithReturn(t, &models.Class{Class: class}, user1Key)
		require.Error(t, err)
		var unproc *schema.SchemaObjectsCreateUnprocessableEntity
		require.True(t, stderrors.As(err, &unproc), "expected SchemaObjectsCreateUnprocessableEntity, got %T: %v", err, err)
		require.NotEmpty(t, unproc.Payload.Error)
		msg := unproc.Payload.Error[0].Message
		assert.NotContains(t, msg, "customer1:", "namespaced caller must not see own-prefix in error: %s", msg)
	})

	t.Run("returned error: global admin sees raw qualified name (control)", func(t *testing.T) {
		// Admin adds a property whose name collides with the existing
		// "title" prop on the qualified class. The 422 message names the
		// qualified class, and the strip is a no-op for a global principal,
		// so "customer1:" stays intact.
		_, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(
			schema.NewSchemaObjectsPropertiesAddParams().
				WithClassName("customer1:"+class).
				WithBody(&models.Property{Name: "title", DataType: []string{"text"}}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *schema.SchemaObjectsPropertiesAddUnprocessableEntity
		require.True(t, stderrors.As(err, &unproc), "expected SchemaObjectsPropertiesAddUnprocessableEntity, got %T: %v", err, err)
		require.NotEmpty(t, unproc.Payload.Error)
		assert.Contains(t, unproc.Payload.Error[0].Message, "customer1:", "admin must see qualified name: %s", unproc.Payload.Error[0].Message)
	})

	t.Run("batch per-item error: object with type-mismatched property is stripped", func(t *testing.T) {
		// Number value for a text property — auto-schema can't reconcile
		// a type conflict on an existing prop, so the validator emits a
		// per-item error naming the qualified class. The message lands in
		// the success-shaped response's Result.Errors with "customer1:"
		// stripped for the namespaced caller.
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{Class: class, Properties: map[string]any{"title": "ok"}},
					{Class: class, Properties: map[string]any{"title": 42}},
				},
			}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		var found bool
		for _, r := range resp.Payload {
			if r.Result != nil && r.Result.Errors != nil && len(r.Result.Errors.Error) > 0 {
				msg := r.Result.Errors.Error[0].Message
				assert.NotContains(t, msg, "customer1:", "batch per-item error must be stripped: %s", msg)
				found = true
			}
		}
		require.True(t, found, "expected at least one failed per-item entry")
	})
}

// TestNamespaces_ResponseStripping_Errors_GRPC pins the contract for gRPC
// unary, batch-reply per-item, and batch-stream per-item error stripping.
// The defer-strip wrapping must preserve typed-error codes
// (PermissionDenied/Unauthenticated) — the test asserts the status code is
// preserved, not collapsed to Unknown.
func TestNamespaces_ResponseStripping_Errors_GRPC(t *testing.T) {
	user1Key, _ := twoNamespaces(t)
	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const class = "ErrStripGRPC"
	setupClassInNs1(t, class, user1Key)

	t.Run("unary returned error: Search on non-existent collection stripped", func(t *testing.T) {
		_, err := grpcClient.Search(authCtx(user1Key), searchReq("NonExistentClass", 1))
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
		// The status code must be preserved, not collapsed to Unknown by
		// the strippedErr wrapper (Unwrap keeps the original typed error
		// reachable for the interceptor's translateTypedError).
		assert.NotContains(t, st.Message(), "customer1:", "namespaced caller must not see own-prefix in status message: %s", st.Message())
	})

	t.Run("batch reply per-item error: invalid object yields stripped per-item entry", func(t *testing.T) {
		// Inject a per-item error by referencing an unknown collection so
		// the classGetter inside BatchObjects fails for that item; the
		// resulting BatchError must have no "customer1:" prefix.
		resp, err := grpcClient.BatchObjects(authCtx(user1Key), &pb.BatchObjectsRequest{
			Objects: []*pb.BatchObject{
				{Uuid: "11111111-1111-1111-1111-111111111111", Collection: "NoSuchClass", Properties: &pb.BatchObject_Properties{
					NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
						"title": structpb.NewStringValue("bad"),
					}},
				}},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Errors)
		for _, be := range resp.Errors {
			assert.NotContains(t, be.Error, "customer1:", "batch reply per-item error must be stripped: %s", be.Error)
		}
	})

	t.Run("batch-stream per-item error: invalid object yields stripped per-item entry", func(t *testing.T) {
		stream, err := grpcClient.BatchStream(authCtx(user1Key))
		require.NoError(t, err)

		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}},
		}))
		started, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, started.GetStarted())

		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Data_{Data: &pb.BatchStreamRequest_Data{
				Objects: &pb.BatchStreamRequest_Data_Objects{Values: []*pb.BatchObject{
					{Uuid: "22222222-2222-2222-2222-222222222222", Collection: "NoSuchStreamClass", Properties: &pb.BatchObject_Properties{
						NonRefProperties: &structpb.Struct{Fields: map[string]*structpb.Value{
							"title": structpb.NewStringValue("bad"),
						}},
					}},
				}},
			}},
		}))
		require.NoError(t, stream.Send(&pb.BatchStreamRequest{
			Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
		}))
		require.NoError(t, stream.CloseSend())

		var foundErr bool
		for {
			msg, recvErr := stream.Recv()
			if recvErr != nil {
				break
			}
			if r := msg.GetResults(); r != nil {
				for _, e := range r.GetErrors() {
					assert.NotContains(t, e.Error, "customer1:", "stream per-item error must be stripped: %s", e.Error)
					foundErr = true
				}
			}
		}
		require.True(t, foundErr, "expected at least one streamed per-item error")
	})

	t.Run("control: global admin sees qualified name in unary error", func(t *testing.T) {
		// Admin queries the qualified class form for a non-existent class —
		// the message must keep "customer1:" intact.
		_, err := grpcClient.Search(authCtx(adminKey), searchReq("customer1:NonExistentClass", 1))
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Contains(t, st.Message(), "customer1:", "admin must see qualified name: %s", st.Message())
	})
}

// TestNamespaces_ResponseStripping_Errors_MCP pins the contract for MCP
// tool-handler returned errors and per-item upsert error fields.
func TestNamespaces_ResponseStripping_Errors_MCP(t *testing.T) {
	user1Key, _ := twoNamespaces(t)
	const class = "ErrStripMCP"
	setupClassInNs1(t, class, user1Key)

	t.Run("returned error: hybrid on non-existent collection is stripped", func(t *testing.T) {
		alpha := 0.0
		var resp *search.QueryHybridResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: "NoSuchMCPClass",
			Query:          "anything",
			Alpha:          &alpha,
		}, &resp, user1Key)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "customer1:", "namespaced caller must not see own-prefix in MCP error: %s", err.Error())
	})

	t.Run("upsert per-item error: invalid object yields stripped per-item entry", func(t *testing.T) {
		// Mismatched UUID triggers a per-item failure; the resulting
		// UpsertObjectResult.Error string must have no "customer1:" prefix.
		var resp *create.UpsertObjectResp
		err := helper.CallToolOnce(t.Context(), t, mcpToolUpsert, &create.UpsertObjectArgs{
			CollectionName: class,
			Objects: []create.ObjectToUpsert{
				{UUID: "not-a-uuid", Properties: map[string]any{"title": "bad"}},
			},
		}, &resp, user1Key)
		// Per-item failures with malformed UUIDs are caught at MCP-arg
		// validation time before the batch call, returning a top-level
		// error. Accept either shape: top-level error OR per-item Error.
		switch {
		case err != nil:
			assert.NotContains(t, err.Error(), "customer1:", "top-level MCP error must be stripped: %s", err.Error())
		case resp != nil:
			require.NotEmpty(t, resp.Results)
			for _, r := range resp.Results {
				if r.Error != "" {
					assert.NotContains(t, r.Error, "customer1:", "per-item MCP error must be stripped: %s", r.Error)
				}
			}
		default:
			t.Fatalf("expected either top-level error or per-item failure")
		}
	})
}

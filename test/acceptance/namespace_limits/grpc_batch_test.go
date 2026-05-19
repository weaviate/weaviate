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

package namespace_limits

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/usagelimits"
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

// TestGRPCBatchObjects_AtQuota sends a forwarded gRPC batch into a
// namespace at quota and expects codes.ResourceExhausted with
// USAGE_LIMIT_EXCEEDED in the details. Covers the ErrorList wire format
// keeping Limit/Value and unanimousLimitExceeded lifting the per-row
// rejection to a top-level gRPC error.
func TestGRPCBatchObjects_AtQuota(t *testing.T) {
	const (
		ns       = "grpcbatch"
		homeNode = "weaviate-1"
		class    = "Items"
	)

	// gRPC client enters on GetWeaviate() (weaviate-0); home_node must
	// differ so the batch is forwarded.
	require.NotEqual(t, homeNode, sharedCompose.GetWeaviate().Name(),
		"gRPC client must enter on a node other than home_node")

	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: class}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+class, adminKey) })

	// Fill the cap via REST so the next write is rejected.
	fillUntilQuota(t, class, userKey)

	grpcClient, conn := newGrpcClient(t)
	defer conn.Close()

	const batchSize = 5
	objs := make([]*pb.BatchObject, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		objs = append(objs, &pb.BatchObject{
			Uuid:       strfmt.UUID(uuid.NewString()).String(),
			Collection: class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"i": structpb.NewNumberValue(float64(i)),
					},
				},
			},
		})
	}

	_, err := grpcClient.BatchObjects(authCtx(userKey), &pb.BatchObjectsRequest{Objects: objs})

	require.Error(t, err, "forwarded gRPC batch at quota must be rejected")
	st, ok := status.FromError(err)
	require.True(t, ok, "expected a gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.ResourceExhausted, st.Code(),
		"expected codes.ResourceExhausted, got %s: %s", st.Code(), st.Message())

	// USAGE_LIMIT_EXCEEDED must appear in the status details (see
	// limit_exceeded_grpc_test.go). Match on bytes to avoid pinning to
	// google.rpc.ErrorInfo's proto registration.
	foundMarker := false
	for _, d := range st.Proto().GetDetails() {
		if d == nil {
			continue
		}
		if assert.Contains(t, d.String(), usagelimits.ErrorCode) {
			foundMarker = true
			break
		}
	}
	assert.True(t, foundMarker, "expected %s in error details, got: %v", usagelimits.ErrorCode, st.Proto().GetDetails())
}

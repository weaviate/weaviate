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

package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	batchMocks "github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// Drives BatchObjects with a request carrying no objects, which returns before
// the batch manager is touched. The namespace is recorded before class
// resolution and authorization, so a rejected batch is still attributed.
// docs/metrics.md lists the residuals.
func TestHandler_BatchObjects(t *testing.T) {
	t.Run("records the caller's namespace in the slot", func(t *testing.T) {
		tests := []struct {
			name      string
			principal *models.Principal
			want      string
		}{
			{name: "nil principal yields empty label", principal: nil, want: ""},
			{
				name:      "global operator yields empty label",
				principal: &models.Principal{Username: "admin", Namespace: "ns_a", IsGlobalOperator: true},
				want:      "",
			},
			{
				name:      "namespace-less principal yields empty label",
				principal: &models.Principal{Username: "legacy"},
				want:      "",
			},
			{
				name:      "namespaced user yields its namespace",
				principal: &models.Principal{Username: "ns_a:alice", Namespace: "ns_a"},
				want:      "ns_a",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				authenticator := auth.NewHandler(false, func(string, []string) (*models.Principal, error) {
					return tc.principal, nil
				})
				h := NewHandler(nil, nil, logger, authenticator, nil, true)

				ctx, slot := restCtx.WithBatchNamespaceSlot(context.Background())
				reply, err := h.BatchObjects(ctx, &pb.BatchObjectsRequest{})

				require.NoError(t, err)
				require.NotNil(t, reply)
				assert.Equal(t, tc.want, slot.Namespace)
			})
		}
	})

	t.Run("authorizes each class and tenant pair", func(t *testing.T) {
		const badUUID = "not-a-uuid"
		_, errBadUUID := uuid.Parse(badUUID)
		errDenied := errors.New("denied")
		authzCall := func(verb, class, tenant string) authMocks.AuthZReq {
			return authMocks.AuthZReq{
				Verb:      verb,
				Resources: authorization.ShardsData(class, tenant),
			}
		}

		tests := []struct {
			name         string
			objects      []*pb.BatchObject
			allowedCalls int
			authzErr     error
			wantErrs     []error
			wantCalls    []authMocks.AuthZReq
		}{
			{
				name: "reuses authorization for the same pair",
				objects: []*pb.BatchObject{
					{Collection: "Foo", Tenant: "tenantA"},
					{Collection: "Foo", Tenant: "tenantA"},
				},
				wantErrs: []error{errBadUUID, errBadUUID},
				wantCalls: []authMocks.AuthZReq{
					authzCall(authorization.UPDATE, "Foo", "tenantA"),
					authzCall(authorization.CREATE, "Foo", "tenantA"),
				},
			},
			{
				name: "authorizes colliding pairs separately",
				objects: []*pb.BatchObject{
					{Collection: "Foo", Tenant: "tenantA#Victim"},
					{Collection: "Foo#tenantA", Tenant: "Victim"},
				},
				allowedCalls: 2,
				authzErr:     errDenied,
				wantErrs:     []error{errBadUUID, errDenied},
				wantCalls: []authMocks.AuthZReq{
					authzCall(authorization.UPDATE, "Foo", "tenantA#Victim"),
					authzCall(authorization.CREATE, "Foo", "tenantA#Victim"),
					authzCall(authorization.UPDATE, "Foo#tenantA", "Victim"),
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				authorizer := authMocks.NewMockAuthorizer()
				authorizer.SetErrAfter(tt.allowedCalls, tt.authzErr)
				schemaManager := batchMocks.NewMockschemaManager(t)
				schemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Twice()
				schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, "Foo").
					Return(map[string]versioned.Class{"Foo": {Class: &models.Class{Class: "Foo"}}}, nil).
					Once()
				handler := newHandler(authorizer, nil, logger, auth.NewHandler(true, nil), schemaManager, false)

				wantErrors := make([]*pb.BatchObjectsReply_BatchError, len(tt.objects))
				for i, obj := range tt.objects {
					obj.Uuid = badUUID
					wantErrors[i] = &pb.BatchObjectsReply_BatchError{Index: int32(i), Error: tt.wantErrs[i].Error()}
				}

				reply, err := handler.BatchObjects(context.Background(), &pb.BatchObjectsRequest{Objects: tt.objects})

				require.NoError(t, err)
				require.ElementsMatch(t, wantErrors, reply.Errors)
				require.Equal(t, tt.wantCalls, authorizer.Calls())
			})
		}
	})

	t.Run("failed authentication leaves the slot empty", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		authenticator := auth.NewHandler(false, func(string, []string) (*models.Principal, error) {
			return nil, errors.New("invalid token")
		})
		h := NewHandler(nil, nil, logger, authenticator, nil, true)

		ctx, slot := restCtx.WithBatchNamespaceSlot(context.Background())
		_, err := h.BatchObjects(ctx, &pb.BatchObjectsRequest{})

		require.Error(t, err)
		assert.Empty(t, slot.Namespace)
	})
}

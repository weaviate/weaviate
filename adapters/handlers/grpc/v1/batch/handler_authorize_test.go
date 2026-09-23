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

package batch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
	wlnamespaces "github.com/weaviate/weaviate/wl/namespaces"
)

// TestBatchObjectsAuthorizesEachClassTenant pins which authorization BatchObjects
// runs per object. The anonymous caller's nil principal leaves the qualified
// name and the error text unchanged.
func TestBatchObjectsAuthorizesEachClassTenant(t *testing.T) {
	const (
		gatedClass = "alpha:Movies"
		// gatedAlias resolves to gatedClass, so a call handed the alias unresolved
		// records a different Class.
		gatedAlias    = "alpha:MoviesAlias"
		droppedClass  = "alpha:Dropped"
		danglingAlias = "alpha:DroppedAlias"
		brokenClass   = "alpha:Broken"
		// badUUID fails parsing right after the class getter, so an admitted
		// object stops before the nil batch manager.
		badUUID = "not-a-uuid"
	)
	_, errBadUUID := uuid.Parse(badUUID)
	errDenied := errors.New("denied")
	errRead := errors.New("schema read failed")
	errDangling := errors.New(`alias "alpha:DroppedAlias" points to collection "alpha:Dropped" which does not exist`)
	updateCheck := func(class, tenant string) mocks.AuthZReq {
		return mocks.AuthZReq{
			Verb:      authorization.UPDATE,
			Resources: authorization.ShardsData(class, tenant),
			Method:    mocks.MethodAuthorize,
		}
	}
	createGate := func(class, tenant string) mocks.AuthZReq {
		return mocks.AuthZReq{
			Verb:      authorization.CREATE,
			Resources: authorization.ShardsData(class, tenant),
			Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
			Class:     class,
		}
	}

	tests := []struct {
		name         string
		objects      []*pb.BatchObject
		allowedCalls int
		authzErr     error
		wantErrs     []error
		wantCalls    []mocks.AuthZReq
	}{
		{
			name:      "an active namespace passes the update check and the create gate",
			objects:   []*pb.BatchObject{{Collection: gatedAlias}},
			wantErrs:  []error{errBadUUID},
			wantCalls: []mocks.AuthZReq{updateCheck(gatedClass, ""), createGate(gatedClass, "")},
		},
		{
			name:         "a suspended namespace is refused at the create gate",
			objects:      []*pb.BatchObject{{Collection: gatedAlias}},
			allowedCalls: 1,
			authzErr:     usecasesNamespaces.ErrNamespaceSuspended,
			wantErrs:     []error{usecasesNamespaces.ErrNamespaceSuspended},
			wantCalls:    []mocks.AuthZReq{updateCheck(gatedClass, ""), createGate(gatedClass, "")},
		},
		{
			name:      "a failed schema read refuses the pair's next object without reading again",
			objects:   []*pb.BatchObject{{Collection: brokenClass}, {Collection: brokenClass}},
			wantErrs:  []error{errRead, errRead},
			wantCalls: []mocks.AuthZReq{updateCheck(brokenClass, ""), createGate(brokenClass, "")},
		},
		{
			// Both pairs join to "alpha:Movies#tenantA#victim". The second is refused
			// only if it reaches the authorizer instead of reusing the first's admission.
			name: "two pairs whose names join to the same string are authorized separately",
			objects: []*pb.BatchObject{
				{Collection: gatedClass, Tenant: "tenantA#victim"},
				{Collection: gatedClass + "#tenantA", Tenant: "victim"},
			},
			allowedCalls: 2,
			authzErr:     errDenied,
			wantErrs:     []error{errBadUUID, errDenied},
			wantCalls: []mocks.AuthZReq{
				updateCheck(gatedClass, "tenantA#victim"),
				createGate(gatedClass, "tenantA#victim"),
				updateCheck(gatedClass+"#tenantA", "victim"),
			},
		},
		{
			name:      "an admitted missing class does not admit a dangling alias to it",
			objects:   []*pb.BatchObject{{Collection: droppedClass}, {Collection: danglingAlias}},
			wantErrs:  []error{errBadUUID, errDangling},
			wantCalls: []mocks.AuthZReq{updateCheck(droppedClass, ""), createGate(droppedClass, "")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			authorizer := mocks.NewMockAuthorizer()
			authorizer.SetErrAfter(tt.allowedCalls, tt.authzErr)
			schemaManager := objects.NewMockClassResolver(t)
			schemaManager.EXPECT().ResolveAlias(gatedAlias).Return(gatedClass).Maybe()
			schemaManager.EXPECT().ResolveAlias(danglingAlias).Return(droppedClass).Maybe()
			schemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
			schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, gatedClass).
				Return(map[string]versioned.Class{gatedClass: {Class: &models.Class{Class: gatedClass}}}, nil).Maybe()
			schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, droppedClass).
				Return(map[string]versioned.Class{}, nil).Maybe()
			schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, brokenClass).
				Return(nil, errRead).Maybe()
			handler := batch.NewHandler(authorizer, nil, logger, auth.NewHandler(true, nil), schemaManager, wlnamespaces.NewPrefixing())

			wantErrors := make([]*pb.BatchObjectsReply_BatchError, len(tt.objects))
			for i, obj := range tt.objects {
				obj.Uuid = badUUID
				wantErrors[i] = &pb.BatchObjectsReply_BatchError{Index: int32(i), Error: tt.wantErrs[i].Error()}
			}

			reply, err := handler.BatchObjects(context.Background(), &pb.BatchObjectsRequest{Objects: tt.objects})

			require.NoError(t, err)
			// BatchObjectsFromProto collects the errors in a map, so their order varies.
			require.ElementsMatch(t, wantErrors, reply.Errors)
			require.Equal(t, tt.wantCalls, authorizer.Calls())
		})
	}
}

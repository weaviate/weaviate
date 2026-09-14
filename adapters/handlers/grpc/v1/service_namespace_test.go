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

package v1

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema"
)

const (
	gatedNamespace = "alpha"
	gatedClass     = "alpha:Movies"
	gateRootUser   = "root"
)

func gateRootPrincipal() *models.Principal {
	return &models.Principal{Username: gateRootUser, UserType: models.UserTypeInputDb}
}

// namespaceIn returns a controller holding gatedNamespace after the given
// transitions. The states are reached through ChangeState rather than written
// directly, so a row naming a transition the control plane refuses fails here
// rather than testing an unreachable state.
func namespaceIn(t *testing.T, transitions []cmd.NamespaceState) *usecasesNamespaces.Controller {
	t.Helper()
	logger, _ := test.NewNullLogger()
	controller := usecasesNamespaces.NewController(logger)
	require.NoError(t, controller.Create(cmd.Namespace{Name: gatedNamespace, HomeNodes: []string{"node1"}}, 1))
	for i, target := range transitions {
		require.NoError(t, controller.ChangeState(gatedNamespace, target,
			usecasesNamespaces.StateChange{AppliedIndex: uint64(i) + 2}))
	}
	return controller
}

// namespaceAwareRBAC builds an RBAC manager with namespaces on and gateRootUser
// as a root, so every row's Authorize passes and only the namespace state decides.
func namespaceAwareRBAC(t *testing.T, lister rbac.NamespaceLister) *rbac.Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	manager, err := rbac.New(
		filepath.Join(t.TempDir(), "policy.csv"),
		rbacconf.Config{Enabled: true, RootUsers: []string{gateRootUser}},
		config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{gateRootUser}}},
		true, lister, logger)
	require.NoError(t, err)
	return manager
}

// TestClassGetterWithAuthzFuncRequiresActiveNamespace drives the real RBAC manager
// against the real namespace controller, so a getter that admits every class, or
// that never reaches the gate, fails here. One refused state is enough for that:
// usecases/namespaces.RequireActive owns the state-to-sentinel mapping.
func TestClassGetterWithAuthzFuncRequiresActiveNamespace(t *testing.T) {
	tests := []struct {
		name        string
		transitions []cmd.NamespaceState
		wantErr     error
	}{
		{
			name: "an active namespace is served",
		},
		{
			name:        "a suspended namespace is refused",
			transitions: []cmd.NamespaceState{cmd.NamespaceStateSuspended},
			wantErr:     usecasesNamespaces.ErrNamespaceSuspended,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := schema.NewMockSchemaReader(t)
			reader.On("ReadOnlyClass", gatedClass).Return(&models.Class{Class: gatedClass}).Maybe()
			s := &Service{
				schemaManager: &schema.Manager{SchemaReader: reader},
				authorizer:    namespaceAwareRBAC(t, namespaceIn(t, tt.transitions)),
			}

			getter := s.classGetterWithAuthzFunc(context.Background(), gateRootPrincipal(), "")
			class, err := getter(gatedClass)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, gatedClass, class.Class)
		})
	}
}

// TestGRPCHandlersReturnTheNamespaceRefusal pins that the refusal survives the
// whole gRPC stack. It is the one stack that rewrites the error on its way out,
// since aggregate wraps it and both entry points run it through
// StripErrForPrincipal. A %v in either place loses the sentinel. One sentinel
// covers all of them: nothing on the way out reads which state refused.
func TestGRPCHandlersReturnTheNamespaceRefusal(t *testing.T) {
	sentinel := usecasesNamespaces.ErrNamespaceSuspended
	entryPoints := map[string]func(*Service, context.Context) error{
		"search": func(s *Service, ctx context.Context) error {
			_, err := s.search(ctx, &pb.SearchRequest{Collection: gatedClass, Limit: 10})
			return err
		},
		"aggregate": func(s *Service, ctx context.Context) error {
			_, err := s.aggregate(ctx, &pb.AggregateRequest{Collection: gatedClass, ObjectsCount: true})
			return err
		},
	}

	for entryPoint, run := range entryPoints {
		t.Run(entryPoint, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			authorizer := mocks.NewMockAuthorizer()
			authorizer.SetErr(sentinel)
			reader := schema.NewMockSchemaReader(t)
			reader.On("ResolveAlias", gatedClass).Return("").Maybe()
			s := &Service{
				authenticator: auth.NewHandler(true, nil),
				schemaManager: &schema.Manager{SchemaReader: reader},
				config:        &config.Config{Namespaces: config.Namespaces{Enabled: true}},
				authorizer:    authorizer,
				logger:        logger,
			}

			err := run(s, context.Background())

			require.ErrorIs(t, err, sentinel)
		})
	}
}

// TestBatchDeleteRequiresActiveNamespaceBeforeParsing pins that batchDelete's
// DELETE authorization is the gate, so a suspended namespace is refused before
// the params getter runs its own READ check.
func TestBatchDeleteRequiresActiveNamespaceBeforeParsing(t *testing.T) {
	// gatedAlias resolves to gatedClass, so a gate handed the name as sent
	// records a different Class.
	const gatedAlias = "alpha:MoviesAlias"
	errReadDenied := errors.New("read denied")
	deleteGate := mocks.AuthZReq{
		Verb:      authorization.DELETE,
		Resources: authorization.ShardsData(gatedClass, ""),
		Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
		Class:     gatedClass,
	}

	tests := []struct {
		name      string
		configure func(*mocks.FakeAuthorizer)
		wantErr   error
		wantCalls []mocks.AuthZReq
	}{
		{
			// The getter's READ check refusing keeps this row off ReadOnlyClass, which
			// the mock reader does not expect. It runs only once the DELETE gate passes.
			name:      "an active namespace passes the gate to the params getter",
			configure: func(a *mocks.FakeAuthorizer) { a.SetErrAfter(1, errReadDenied) },
			wantErr:   errReadDenied,
			wantCalls: []mocks.AuthZReq{deleteGate, {
				Verb:      authorization.READ,
				Resources: authorization.CollectionsData(gatedClass),
				Method:    mocks.MethodAuthorizeAndRequireActiveNamespace,
				Class:     gatedClass,
			}},
		},
		{
			name:      "a suspended namespace is refused at the DELETE gate",
			configure: func(a *mocks.FakeAuthorizer) { a.SetErr(usecasesNamespaces.ErrNamespaceSuspended) },
			wantErr:   usecasesNamespaces.ErrNamespaceSuspended,
			wantCalls: []mocks.AuthZReq{deleteGate},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			tt.configure(authorizer)
			reader := schema.NewMockSchemaReader(t)
			reader.On("ResolveAlias", gatedAlias).Return(gatedClass)
			s := &Service{
				authenticator: auth.NewHandler(true, nil),
				schemaManager: &schema.Manager{SchemaReader: reader},
				config:        &config.Config{Namespaces: config.Namespaces{Enabled: true}},
				authorizer:    authorizer,
			}

			_, err := s.batchDelete(context.Background(), &pb.BatchDeleteRequest{Collection: gatedAlias})

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantCalls, authorizer.Calls())
		})
	}
}

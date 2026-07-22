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

package namespaces

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// req is the shared request context used to satisfy params.HTTPRequest. The
// handlers only read params.HTTPRequest.Context() from it; method and path
// are irrelevant for unit tests.
var req, _ = http.NewRequest("POST", "/namespaces/test", nil)

// mockRaft is a hand-rolled testify mock for NamespaceRaftGetter. We don't
// use mockery here because the interface only has a handful of methods and
// lives in this package — a generated mock would be more churn than value.
type mockRaft struct{ mock.Mock }

func (m *mockRaft) AddNamespace(ctx context.Context, ns cmd.Namespace) (cmd.Namespace, uint64, error) {
	ret := m.Called(ctx, ns)
	var out cmd.Namespace
	if v := ret.Get(0); v != nil {
		out = v.(cmd.Namespace)
	}
	return out, uint64(ret.Int(1)), ret.Error(2)
}

func (m *mockRaft) UpdateNamespace(ctx context.Context, ns cmd.Namespace) (uint64, error) {
	args := m.Called(ctx, ns)
	return uint64(args.Int(0)), args.Error(1)
}

func (m *mockRaft) ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	args := m.Called(ctx, name, target)
	return uint64(args.Int(0)), args.Error(1)
}

func (m *mockRaft) ChangeNamespaceStateIfUnchanged(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error) {
	args := m.Called(ctx, name, target)
	return uint64(args.Int(0)), args.Error(1)
}

func (m *mockRaft) GetNamespaces(names ...string) ([]cmd.Namespace, error) {
	// Convert variadic to []interface{} for Called.
	args := make([]interface{}, len(names))
	for i, n := range names {
		args[i] = n
	}
	ret := m.Called(args...)
	if v := ret.Get(0); v != nil {
		return v.([]cmd.Namespace), ret.Error(1)
	}
	return nil, ret.Error(1)
}

func (m *mockRaft) StorageCandidates() []string {
	return m.Called().Get(0).([]string)
}

func newHandler(t *testing.T) (*namespaceHandler, *authorization.MockAuthorizer, *mockRaft) {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	raft := &mockRaft{}
	t.Cleanup(func() { raft.AssertExpectations(t) })
	return &namespaceHandler{
		enabled:    true,
		authorizer: authorizer,
		raft:       raft,
	}, authorizer, raft
}

// -----------------------------------------------------------------------------
// Cross-endpoint shared behaviors
// -----------------------------------------------------------------------------

// TestHandlers_Forbidden verifies that every endpoint that gates on
// authz.Authorize returns the typed Forbidden response for that endpoint
// when the authorizer denies the request.
func TestHandlers_Forbidden(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		action    string
		invoke    func(h *namespaceHandler) middleware.Responder
		wantTyped any
	}{
		{
			name:   "create",
			action: authorization.CREATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.CreateNamespaceForbidden{},
		},
		{
			name:   "update",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				hn := "node-1"
				return h.updateNamespace(nsops.UpdateNamespaceParams{
					NamespaceID: "customer1", HTTPRequest: req,
					Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
				}, principal)
			},
			wantTyped: &nsops.UpdateNamespaceForbidden{},
		},
		{
			name:   "get",
			action: authorization.READ,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.GetNamespaceForbidden{},
		},
		{
			name:   "delete",
			action: authorization.DELETE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.DeleteNamespaceForbidden{},
		},
		{
			name:   "suspend",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.SuspendNamespaceForbidden{},
		},
		{
			name:   "resume",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.resumeNamespace(nsops.ResumeNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.ResumeNamespaceForbidden{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, _ := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, tc.action, authorization.Namespaces("customer1")[0]).
				Return(authzerrors.NewForbidden(principal, tc.action, authorization.Namespaces("customer1")...))

			assert.IsType(t, tc.wantTyped, tc.invoke(h))
		})
	}
}

// TestHandlers_InvalidNameRejected verifies that endpoints rejecting an
// invalid namespace name return a typed 422 for that endpoint. The full
// name-validation matrix lives on the create handler under
// TestCreateNamespace_UnprocessableEntity; this only ensures each endpoint
// surfaces the rejection consistently.
func TestHandlers_InvalidNameRejected(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		action    string
		invoke    func(h *namespaceHandler) middleware.Responder
		wantTyped any
	}{
		{
			name:   "update",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				hn := "node-1"
				return h.updateNamespace(nsops.UpdateNamespaceParams{
					NamespaceID: "BadName", HTTPRequest: req,
					Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
				}, principal)
			},
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			name:   "get",
			action: authorization.READ,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.GetNamespaceUnprocessableEntity{},
		},
		{
			name:   "delete",
			action: authorization.DELETE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.DeleteNamespaceUnprocessableEntity{},
		},
		{
			name:   "suspend",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.SuspendNamespaceUnprocessableEntity{},
		},
		{
			name:   "resume",
			action: authorization.UPDATE,
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.resumeNamespace(nsops.ResumeNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
			},
			wantTyped: &nsops.ResumeNamespaceUnprocessableEntity{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, _ := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, tc.action, authorization.Namespaces("BadName")[0]).Return(nil)

			assert.IsType(t, tc.wantTyped, tc.invoke(h))
		})
	}
}

// -----------------------------------------------------------------------------
// createNamespace
// -----------------------------------------------------------------------------

func TestCreateNamespace_UnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name  string
		input string
	}{
		{"too short", "ab"},
		{"too long", strings.Repeat("a", 37)},
		{"uppercase", "Customer1"},
		{"underscore", "customer_1"},
		{"reserved admin", "admin"},
		{"reserved weaviate", "weaviate"},
		{"reserved default", "default"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, _ := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces(tc.input)[0]).Return(nil)

			res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: tc.input, HTTPRequest: req}, principal)
			_, ok := res.(*nsops.CreateNamespaceUnprocessableEntity)
			assert.True(t, ok, "expected 422, got %T", res)
		})
	}
}

// TestCreateNamespace_Conflict checks the 409 paths: an existing name and a
// namespace mid-deletion both return Conflict with a distinguishing message.
func TestCreateNamespace_Conflict(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name       string
		raftErr    error
		wantSubstr string
	}{
		{
			name:       "already exists",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrAlreadyExists, "customer1"),
			wantSubstr: "already exists",
		},
		{
			name:       "being deleted",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceDeleting, "customer1"),
			wantSubstr: "being deleted",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("AddNamespace", mock.Anything, cmd.Namespace{Name: "customer1"}).Return(nil, 0, tc.raftErr)

			res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			parsed, ok := res.(*nsops.CreateNamespaceConflict)
			require.True(t, ok, "expected 409, got %T", res)
			require.NotNil(t, parsed.Payload)
			require.Len(t, parsed.Payload.Error, 1)
			assert.Contains(t, parsed.Payload.Error[0].Message, tc.wantSubstr)
		})
	}
}

// TestCreateNamespace_LifecycleErrorsReturn422 checks that the namespace
// lifecycle sentinels render 422 with a message that distinguishes them.
func TestCreateNamespace_LifecycleErrorsReturn422(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name       string
		raftErr    error
		wantSubstr string
	}{
		{
			name:       "namespace suspended",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceSuspended, "customer1"),
			wantSubstr: "suspended",
		},
		{
			name:       "collection suspended",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrCollectionSuspended, "customer1"),
			wantSubstr: "suspended",
		},
		{
			name:       "namespace still has resources",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceNotEmpty, "customer1"),
			wantSubstr: "owned resources",
		},
		{
			name:       "namespace in invalid state",
			raftErr:    fmt.Errorf("%w: %q", usecasesNamespaces.ErrInvalidState, "customer1"),
			wantSubstr: "invalid state",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("AddNamespace", mock.Anything, cmd.Namespace{Name: "customer1"}).Return(nil, 0, tc.raftErr)

			res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			parsed, ok := res.(*nsops.CreateNamespaceUnprocessableEntity)
			require.True(t, ok, "expected 422, got %T", res)
			require.NotNil(t, parsed.Payload)
			require.Len(t, parsed.Payload.Error, 1)
			assert.Contains(t, parsed.Payload.Error[0].Message, tc.wantSubstr)
			// This API is excluded from namespaces.PublicMessage: its callers
			// hold manage_namespaces, so the body keeps the name. The neutral
			// copy also contains tc.wantSubstr, so only this assertion catches
			// a cleanup that wires PublicMessage in here.
			assert.Contains(t, parsed.Payload.Error[0].Message, "customer1")
		})
	}
}

// TestCreateNamespace_RaftErrorMapping covers how AddNamespace errors map to
// HTTP status codes for the non-conflict cases: ErrBadRequest → 422 (defense
// in depth when an invalid name slips past handler validation) and untyped
// errors → 500.
func TestCreateNamespace_RaftErrorMapping(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		raftErr   error
		wantTyped any
	}{
		{
			name:      "ErrBadRequest → 422",
			raftErr:   fmt.Errorf("%w: bad payload", usecasesNamespaces.ErrBadRequest),
			wantTyped: &nsops.CreateNamespaceUnprocessableEntity{},
		},
		{
			// Picked up through the trailing HTTPStatusForNamespaceErr
			// fall-through, which admits exactly status 422.
			name:      "ErrInvalidStateTransition → 422",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrInvalidStateTransition, "customer1"),
			wantTyped: &nsops.CreateNamespaceUnprocessableEntity{},
		},
		{
			name:      "untyped error → 500",
			raftErr:   errors.New("raft boom"),
			wantTyped: &nsops.CreateNamespaceInternalServerError{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("AddNamespace", mock.Anything, cmd.Namespace{Name: "customer1"}).Return(nil, 0, tc.raftErr)

			res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			assert.IsType(t, tc.wantTyped, res)
		})
	}
}

func TestCreateNamespace_Created(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
	// AddNamespace returns the persisted namespace with home_node filled.
	raft.On("AddNamespace", mock.Anything, cmd.Namespace{Name: "customer1"}).
		Return(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, 0, nil)

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.CreateNamespaceCreated)
	require.True(t, ok, "expected 201, got %T", res)
	require.NotNil(t, parsed.Payload)
	assert.Equal(t, "customer1", parsed.Payload.Name)
	assert.Equal(t, string(cmd.NamespaceStateActive), parsed.Payload.State)
	assert.Equal(t, "node-1", parsed.Payload.HomeNode)
}

// TestCreateNamespace_BodyHomeNode covers operator-supplied home_node: a
// known storage candidate is accepted and forwarded; an unknown node is
// rejected with 422 before reaching RAFT.
func TestCreateNamespace_BodyHomeNode(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		homeNode  string
		setupRaft func(r *mockRaft)
		wantTyped any
	}{
		{
			name:     "known storage candidate accepted",
			homeNode: "node-2",
			setupRaft: func(r *mockRaft) {
				r.On("StorageCandidates").Return([]string{"node-1", "node-2"})
				r.On("AddNamespace", mock.Anything, cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}).
					Return(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}, 0, nil)
			},
			wantTyped: &nsops.CreateNamespaceCreated{},
		},
		{
			name:     "unknown node rejected with 422",
			homeNode: "node-99",
			setupRaft: func(r *mockRaft) {
				r.On("StorageCandidates").Return([]string{"node-1", "node-2"})
			},
			wantTyped: &nsops.CreateNamespaceUnprocessableEntity{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
			tc.setupRaft(raft)

			res := h.createNamespace(nsops.CreateNamespaceParams{
				NamespaceID: "customer1",
				HTTPRequest: req,
				Body:        &models.NamespaceCreateRequest{HomeNode: tc.homeNode},
			}, principal)
			assert.IsType(t, tc.wantTyped, res)
		})
	}
}

// -----------------------------------------------------------------------------
// updateNamespace
// -----------------------------------------------------------------------------

// TestUpdateNamespace_HomeNodeValidation covers the 422-before-RAFT paths
// for home_node validation: missing field and unknown node.
func TestUpdateNamespace_HomeNodeValidation(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		body      *models.NamespaceUpdateRequest
		setupRaft func(r *mockRaft)
	}{
		{
			name: "missing body",
			body: nil,
		},
		{
			name: "missing home_node",
			body: &models.NamespaceUpdateRequest{},
		},
		{
			name: "unknown home_node",
			body: func() *models.NamespaceUpdateRequest {
				hn := "node-99"
				return &models.NamespaceUpdateRequest{HomeNode: &hn}
			}(),
			setupRaft: func(r *mockRaft) {
				r.On("StorageCandidates").Return([]string{"node-1"})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).Return(nil)
			if tc.setupRaft != nil {
				tc.setupRaft(raft)
			}

			res := h.updateNamespace(nsops.UpdateNamespaceParams{
				NamespaceID: "customer1", HTTPRequest: req, Body: tc.body,
			}, principal)
			_, ok := res.(*nsops.UpdateNamespaceUnprocessableEntity)
			assert.True(t, ok, "expected 422, got %T", res)
		})
	}
}

// TestUpdateNamespace_RaftErrorMapping covers how UpdateNamespace errors map
// to HTTP status codes.
func TestUpdateNamespace_RaftErrorMapping(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		raftErr   error
		wantTyped any
	}{
		{
			name:      "ErrNotFound → 404",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrNotFound, "customer1"),
			wantTyped: &nsops.UpdateNamespaceNotFound{},
		},
		{
			// Picked up through the trailing HTTPStatusForNamespaceErr
			// fall-through, which admits exactly status 422.
			name:      "ErrInvalidStateTransition → 422",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrInvalidStateTransition, "customer1"),
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			// No retry can succeed: cleanup removes the namespace, so a
			// later update 404s.
			name:      "ErrNamespaceDeleting → 422",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceDeleting, "customer1"),
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			name:      "ErrNamespaceSuspended → 422",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceSuspended, "customer1"),
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			name:      "ErrCollectionSuspended → 422",
			raftErr:   fmt.Errorf("%w: %q", usecasesNamespaces.ErrCollectionSuspended, "customer1"),
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			name:      "ErrBadRequest → 422",
			raftErr:   fmt.Errorf("%w: bad payload", usecasesNamespaces.ErrBadRequest),
			wantTyped: &nsops.UpdateNamespaceUnprocessableEntity{},
		},
		{
			name:      "untyped error → 500",
			raftErr:   errors.New("raft boom"),
			wantTyped: &nsops.UpdateNamespaceInternalServerError{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("StorageCandidates").Return([]string{"node-1"})
			raft.On("UpdateNamespace", mock.Anything, cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}).Return(0, tc.raftErr)

			hn := "node-1"
			res := h.updateNamespace(nsops.UpdateNamespaceParams{
				NamespaceID: "customer1", HTTPRequest: req,
				Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
			}, principal)
			assert.IsType(t, tc.wantTyped, res)
		})
	}
}

func TestUpdateNamespace_OK(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("StorageCandidates").Return([]string{"node-1", "node-2"})
	raft.On("UpdateNamespace", mock.Anything, cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}).Return(0, nil)
	// Handler reads back State from the controller after a successful
	// update rather than hardcoding active.
	raft.On("GetNamespaces", "customer1").Return(
		[]cmd.Namespace{{Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive}}, nil)

	hn := "node-2"
	res := h.updateNamespace(nsops.UpdateNamespaceParams{
		NamespaceID: "customer1", HTTPRequest: req,
		Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
	}, principal)
	parsed, ok := res.(*nsops.UpdateNamespaceOK)
	require.True(t, ok, "expected 200, got %T", res)
	require.NotNil(t, parsed.Payload)
	assert.Equal(t, "customer1", parsed.Payload.Name)
	assert.Equal(t, "node-2", parsed.Payload.HomeNode)
	assert.Equal(t, string(cmd.NamespaceStateActive), parsed.Payload.State)
}

// TestUpdateNamespace_OK_StateReadFailureLeavesStateEmpty asserts the
// read-back failure path: when GetNamespaces errors or returns nothing
// after a successful update, the handler still returns 200 with the
// caller-supplied HomeNode and an empty State (rather than guessing).
func TestUpdateNamespace_OK_StateReadFailureLeavesStateEmpty(t *testing.T) {
	cases := []struct {
		name      string
		getResult []cmd.Namespace
		getErr    error
	}{
		{name: "GetNamespaces error", getErr: errors.New("read boom")},
		{name: "GetNamespaces returns nothing (raced with delete)", getResult: []cmd.Namespace{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			principal := &models.Principal{}
			authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("StorageCandidates").Return([]string{"node-1", "node-2"})
			raft.On("UpdateNamespace", mock.Anything, cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-2"}}).Return(0, nil)
			raft.On("GetNamespaces", "customer1").Return(tc.getResult, tc.getErr)

			hn := "node-2"
			res := h.updateNamespace(nsops.UpdateNamespaceParams{
				NamespaceID: "customer1", HTTPRequest: req,
				Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
			}, principal)
			parsed, ok := res.(*nsops.UpdateNamespaceOK)
			require.True(t, ok, "expected 200, got %T", res)
			require.NotNil(t, parsed.Payload)
			assert.Equal(t, "node-2", parsed.Payload.HomeNode)
			assert.Empty(t, parsed.Payload.State)
		})
	}
}

// -----------------------------------------------------------------------------
// getNamespace
// -----------------------------------------------------------------------------

// TestGetNamespace_RaftErrorMapping covers how GetNamespaces results map to
// HTTP status codes for the GET handler.
func TestGetNamespace_RaftErrorMapping(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		raftRet   []cmd.Namespace
		raftErr   error
		wantTyped any
	}{
		{
			name:      "missing → 404",
			raftRet:   []cmd.Namespace{},
			wantTyped: &nsops.GetNamespaceNotFound{},
		},
		{
			name:      "untyped error → 500",
			raftErr:   errors.New("raft boom"),
			wantTyped: &nsops.GetNamespaceInternalServerError{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("GetNamespaces", "customer1").Return(tc.raftRet, tc.raftErr)

			res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			assert.IsType(t, tc.wantTyped, res)
		})
	}
}

func TestGetNamespace_OK(t *testing.T) {
	cases := []struct {
		name      string
		nsState   cmd.NamespaceState
		wantState string
	}{
		{name: "active is surfaced", nsState: cmd.NamespaceStateActive, wantState: string(cmd.NamespaceStateActive)},
		{name: "deleting is surfaced", nsState: cmd.NamespaceStateDeleting, wantState: string(cmd.NamespaceStateDeleting)},
		{name: "suspended is surfaced", nsState: cmd.NamespaceStateSuspended, wantState: string(cmd.NamespaceStateSuspended)},
		{name: "resuming is surfaced", nsState: cmd.NamespaceStateResuming, wantState: string(cmd.NamespaceStateResuming)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			principal := &models.Principal{}
			authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("GetNamespaces", "customer1").Return([]cmd.Namespace{{Name: "customer1", State: tc.nsState}}, nil)

			res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			parsed, ok := res.(*nsops.GetNamespaceOK)
			require.True(t, ok, "expected 200, got %T", res)
			require.NotNil(t, parsed.Payload)
			assert.Equal(t, "customer1", parsed.Payload.Name)
			assert.Equal(t, tc.wantState, parsed.Payload.State)
		})
	}
}

// -----------------------------------------------------------------------------
// deleteNamespace
// -----------------------------------------------------------------------------

// TestDeleteNamespace flips the namespace to deleting and asserts the typed
// HTTP response. The auth guard invalidates the namespace's keys, so the
// handler issues no user drain; the cleanup tick reclaims the rows.
func TestDeleteNamespace(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name           string
		changeStateErr error
		wantTyped      any
	}{
		{
			name:           "ChangeNamespaceState returns ErrNotFound → 404",
			changeStateErr: fmt.Errorf("%w: %q", usecasesNamespaces.ErrNotFound, "customer1"),
			wantTyped:      &nsops.DeleteNamespaceNotFound{},
		},
		{
			name:           "ChangeNamespaceState untyped error → 500",
			changeStateErr: errors.New("raft boom"),
			wantTyped:      &nsops.DeleteNamespaceInternalServerError{},
		},
		{
			name:      "happy path returns 202",
			wantTyped: &nsops.DeleteNamespaceAccepted{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("customer1")[0]).Return(nil)
			raft.On("ChangeNamespaceState", mock.Anything, "customer1", cmd.NamespaceStateDeleting).Return(0, tc.changeStateErr)

			res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			assert.IsType(t, tc.wantTyped, res)
			raft.AssertNotCalled(t, "DeleteUsersInNamespace")
		})
	}
}

// -----------------------------------------------------------------------------
// listNamespaces
// -----------------------------------------------------------------------------

func TestListNamespaces_Empty(t *testing.T) {
	h, _, raft := newHandler(t)
	principal := &models.Principal{}
	raft.On("GetNamespaces").Return([]cmd.Namespace{}, nil)

	res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.ListNamespacesOK)
	require.True(t, ok, "expected 200, got %T", res)
	assert.Empty(t, parsed.Payload)
}

func TestListNamespaces_FilteredByAuthz(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	all := []cmd.Namespace{
		{Name: "customer1", HomeNodes: []string{"node-1"}, State: cmd.NamespaceStateActive},
		{Name: "customer2", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateDeleting},
		{Name: "customer3", HomeNodes: []string{"node-3"}, State: cmd.NamespaceStateActive},
	}
	raft.On("GetNamespaces").Return(all, nil)

	// Caller only has permission on customer2.
	authz.On("FilterAuthorizedResources",
		mock.Anything, principal, authorization.READ,
		authorization.Namespaces("customer1")[0],
		authorization.Namespaces("customer2")[0],
		authorization.Namespaces("customer3")[0],
	).Return([]string{authorization.Namespaces("customer2")[0]}, nil)

	res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.ListNamespacesOK)
	require.True(t, ok, "expected 200, got %T", res)
	require.Len(t, parsed.Payload, 1)
	assert.Equal(t, "customer2", parsed.Payload[0].Name)
	assert.Equal(t, string(cmd.NamespaceStateDeleting), parsed.Payload[0].State)
	// HomeNode must round-trip through List, matching Create/Get/Update.
	assert.Equal(t, "node-2", parsed.Payload[0].HomeNode)
}

func TestListNamespaces_NoPermissionsReturnsEmpty(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	all := []cmd.Namespace{{Name: "customer1"}, {Name: "customer2"}}
	raft.On("GetNamespaces").Return(all, nil)
	authz.On("FilterAuthorizedResources",
		mock.Anything, principal, authorization.READ,
		authorization.Namespaces("customer1")[0],
		authorization.Namespaces("customer2")[0],
	).Return([]string{}, nil)

	res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.ListNamespacesOK)
	require.True(t, ok, "expected 200, got %T", res)
	assert.Empty(t, parsed.Payload)
}

// TestListNamespaces_ErrorMapping covers the two paths that return 500: a
// raft GetNamespaces failure and a downstream FilterAuthorizedResources
// failure on a non-empty list.
func TestListNamespaces_ErrorMapping(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name      string
		setupRaft func(r *mockRaft, a *authorization.MockAuthorizer)
	}{
		{
			name: "GetNamespaces returns error",
			setupRaft: func(r *mockRaft, _ *authorization.MockAuthorizer) {
				r.On("GetNamespaces").Return(nil, errors.New("boom"))
			},
		},
		{
			name: "FilterAuthorizedResources returns error",
			setupRaft: func(r *mockRaft, a *authorization.MockAuthorizer) {
				r.On("GetNamespaces").Return([]cmd.Namespace{{Name: "customer1"}}, nil)
				a.On("FilterAuthorizedResources",
					mock.Anything, principal, authorization.READ,
					authorization.Namespaces("customer1")[0],
				).Return(nil, errors.New("filter boom"))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, authz, raft := newHandler(t)
			tc.setupRaft(raft, authz)

			res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
			_, ok := res.(*nsops.ListNamespacesInternalServerError)
			assert.True(t, ok, "expected 500, got %T", res)
		})
	}
}

// -----------------------------------------------------------------------------
// namespaces feature flag
// -----------------------------------------------------------------------------

// TestHandlers_Disabled verifies that every endpoint short-circuits with 404
// when the namespaces feature flag is off, without calling authz or RAFT.
// The handlers return a raw middleware.ResponderFunc (not a typed operations
// response), so we drive it through a httptest recorder and check the status.
func TestHandlers_Disabled(t *testing.T) {
	principal := &models.Principal{}
	hn := "node-1"
	cases := []struct {
		name   string
		invoke func(h *namespaceHandler) middleware.Responder
	}{
		{
			name: "create",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
		},
		{
			name: "update",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.updateNamespace(nsops.UpdateNamespaceParams{
					NamespaceID: "customer1", HTTPRequest: req,
					Body: &models.NamespaceUpdateRequest{HomeNode: &hn},
				}, principal)
			},
		},
		{
			name: "get",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
		},
		{
			name: "delete",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
		},
		{
			name: "list",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
			},
		},
		{
			name: "suspend",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
		},
		{
			name: "resume",
			invoke: func(h *namespaceHandler) middleware.Responder {
				return h.resumeNamespace(nsops.ResumeNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _ := newHandler(t)
			h.enabled = false
			res := tc.invoke(h)

			rec := httptest.NewRecorder()
			res.WriteResponse(rec, runtime.JSONProducer())
			assert.Equal(t, http.StatusNotFound, rec.Code, "expected 404")

			var body models.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Error, 1)
			assert.Contains(t, body.Error[0].Message, "namespaces are not enabled")
		})
	}
}

// -----------------------------------------------------------------------------
// suspendNamespace / resumeNamespace
// -----------------------------------------------------------------------------

func TestStatusForChangeStateErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "not found", err: usecasesNamespaces.ErrNotFound, want: http.StatusNotFound},
		{name: "lost the race", err: usecasesNamespaces.ErrStateChangedConcurrently, want: http.StatusConflict},
		{name: "illegal transition", err: usecasesNamespaces.ErrInvalidStateTransition, want: http.StatusUnprocessableEntity},
		{name: "forwarded not-leader", err: types.ErrNotLeader, want: http.StatusServiceUnavailable},
		{name: "forwarded leader-not-found", err: types.ErrLeaderNotFound, want: http.StatusServiceUnavailable},
		{name: "leader-local not-leader", err: raft.ErrNotLeader, want: http.StatusServiceUnavailable},
		{name: "leader-local leadership-lost", err: raft.ErrLeadershipLost, want: http.StatusServiceUnavailable},
		{name: "unclassified", err: errors.New("disk on fire"), want: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, statusForChangeStateErr(tc.err))
			// The apply path wraps sentinels with the namespace name.
			assert.Equal(t, tc.want, statusForChangeStateErr(fmt.Errorf("apply: %w", tc.err)),
				"a wrapped sentinel must classify the same")
		})
	}
}

// TestSuspendResume_ApplyOutcomes pins the status and body each apply outcome
// renders, on both endpoints. The namespace's *current* state is not modelled
// here — the mock returns whatever the row asks for — so the from-state
// matrix (idempotent re-suspend, aborting an in-flight resume, deleting being
// terminal) lives at the controller, in TestController_ChangeState.
func TestSuspendResume_ApplyOutcomes(t *testing.T) {
	principal := &models.Principal{Username: "ops-1"}
	noLeaderErr := fmt.Errorf("%w, can not resolve nodes [node-7:8300]", types.ErrLeaderNotFound)
	cases := []struct {
		name           string
		changeStateErr error
		wantCode       int
		wantInBody     string
	}{
		{name: "applied", wantCode: http.StatusAccepted},
		{
			name:           "illegal transition",
			changeStateErr: usecasesNamespaces.ErrInvalidStateTransition,
			wantCode:       http.StatusUnprocessableEntity,
			wantInBody:     "invalid namespace state transition",
		},
		{
			name:           "missing namespace",
			changeStateErr: usecasesNamespaces.ErrNotFound,
			wantCode:       http.StatusNotFound,
			wantInBody:     `namespace \"customer1\" not found`,
		},
		{
			name:           "lost a concurrent flip",
			changeStateErr: usecasesNamespaces.ErrStateChangedConcurrently,
			wantCode:       http.StatusConflict,
			wantInBody:     "namespace state changed concurrently",
		},
		{
			name:           "no leader",
			changeStateErr: noLeaderErr,
			wantCode:       http.StatusServiceUnavailable,
			wantInBody:     "changing namespace state",
		},
		{
			name:           "unclassified failure",
			changeStateErr: errors.New("boom"),
			wantCode:       http.StatusInternalServerError,
			wantInBody:     "changing namespace state",
		},
	}
	for _, tc := range cases {
		for _, endpoint := range []string{"suspend", "resume"} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				h, authz, raftMock := newHandler(t)
				authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).Return(nil)

				wantTarget := cmd.NamespaceStateActive
				if endpoint == "suspend" {
					wantTarget = cmd.NamespaceStateSuspended
				}
				raftMock.On("ChangeNamespaceStateIfUnchanged", mock.Anything, "customer1", wantTarget).
					Return(0, tc.changeStateErr)

				var res middleware.Responder
				if endpoint == "suspend" {
					res = h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
				} else {
					res = h.resumeNamespace(nsops.ResumeNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
				}

				rec := httptest.NewRecorder()
				res.WriteResponse(rec, runtime.JSONProducer())
				assert.Equal(t, tc.wantCode, rec.Code)

				if tc.wantCode == http.StatusAccepted {
					assert.Empty(t, rec.Body.String(), "an accepted flip returns no body")
					return
				}
				assert.Contains(t, rec.Body.String(), tc.wantInBody)
			})
		}
	}
}

// A denied or invalid request must not reach RAFT: the mock has no
// ChangeNamespaceStateIfUnchanged expectation, so any apply call fails the test.
func TestSuspendResume_NoApplyBeforeValidation(t *testing.T) {
	principal := &models.Principal{Username: "ops-1"}

	t.Run("forbidden", func(t *testing.T) {
		h, authz, _ := newHandler(t)
		authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("customer1")[0]).
			Return(authzerrors.NewForbidden(principal, authorization.UPDATE, authorization.Namespaces("customer1")...))

		res := h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
		rec := httptest.NewRecorder()
		res.WriteResponse(rec, runtime.JSONProducer())
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("invalid name", func(t *testing.T) {
		h, authz, _ := newHandler(t)
		authz.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Namespaces("BadName")[0]).Return(nil)

		res := h.suspendNamespace(nsops.SuspendNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
		rec := httptest.NewRecorder()
		res.WriteResponse(rec, runtime.JSONProducer())
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	})
}

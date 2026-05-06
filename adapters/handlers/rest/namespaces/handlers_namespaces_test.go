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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
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
// use mockery here because the interface has only three methods and lives
// in this package — a generated mock would be more churn than value.
type mockRaft struct{ mock.Mock }

func (m *mockRaft) AddNamespace(ns cmd.Namespace) error {
	return m.Called(ns).Error(0)
}

func (m *mockRaft) DeleteNamespace(name string) error {
	return m.Called(name).Error(0)
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

func newHandler(t *testing.T) (*namespaceHandler, *authorization.MockAuthorizer, *mockRaft) {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	raft := &mockRaft{}
	t.Cleanup(func() { raft.AssertExpectations(t) })
	return &namespaceHandler{
		enabled:    true,
		authorizer: authorizer,
		raft:       raft,
		logger:     logrus.New(),
	}, authorizer, raft
}

// -----------------------------------------------------------------------------
// createNamespace
// -----------------------------------------------------------------------------

func TestCreateNamespace_Forbidden(t *testing.T) {
	h, authz, _ := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).
		Return(authzerrors.NewForbidden(principal, authorization.CREATE, authorization.Namespaces("customer1")...))

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.CreateNamespaceForbidden)
	assert.True(t, ok, "expected 403, got %T", res)
}

func TestCreateNamespace_UnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	cases := []struct {
		name  string
		input string
	}{
		{"too short", "ab"},
		{"too long", strings.Repeat("a", 37)},
		{"uppercase", "Customer1"},
		{"digit prefix", "1customer"},
		{"dash in name", "customer-1"},
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

// TestCreateNamespace_ConflictOnRaftAlreadyExists covers the TOCTOU race:
// two concurrent creates can both pass validation; the loser must get 409,
// not 500. The handler translates usecasesNamespaces.ErrAlreadyExists → 409.
func TestCreateNamespace_ConflictOnRaftAlreadyExists(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("AddNamespace", cmd.Namespace{Name: "customer1"}).
		Return(fmt.Errorf("%w: %q", usecasesNamespaces.ErrAlreadyExists, "customer1"))

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.CreateNamespaceConflict)
	assert.True(t, ok, "expected 409, got %T", res)
}

// TestCreateNamespace_UnprocessableOnRaftBadRequest covers the defense-in-depth
// case where an invalid name slips past the handler check and the FSM rejects
// it. Surface as 422, not 500.
func TestCreateNamespace_UnprocessableOnRaftBadRequest(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("AddNamespace", cmd.Namespace{Name: "customer1"}).
		Return(fmt.Errorf("%w: bad payload", usecasesNamespaces.ErrBadRequest))

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.CreateNamespaceUnprocessableEntity)
	assert.True(t, ok, "expected 422, got %T", res)
}

func TestCreateNamespace_RaftAddError(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("AddNamespace", cmd.Namespace{Name: "customer1"}).Return(errors.New("raft boom"))

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.CreateNamespaceInternalServerError)
	assert.True(t, ok, "expected 500, got %T", res)
}

func TestCreateNamespace_Created(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("AddNamespace", cmd.Namespace{Name: "customer1"}).Return(nil)

	res := h.createNamespace(nsops.CreateNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.CreateNamespaceCreated)
	require.True(t, ok, "expected 201, got %T", res)
	require.NotNil(t, parsed.Payload)
	assert.Equal(t, "customer1", parsed.Payload.Name)
}

// -----------------------------------------------------------------------------
// getNamespace
// -----------------------------------------------------------------------------

func TestGetNamespace_Forbidden(t *testing.T) {
	h, authz, _ := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).
		Return(authzerrors.NewForbidden(principal, authorization.READ, authorization.Namespaces("customer1")...))

	res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.GetNamespaceForbidden)
	assert.True(t, ok, "expected 403, got %T", res)
}

func TestGetNamespace_InvalidNameRejected(t *testing.T) {
	h, authz, _ := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("BadName")[0]).Return(nil)

	res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.GetNamespaceUnprocessableEntity)
	assert.True(t, ok, "expected 422, got %T", res)
}

func TestGetNamespace_NotFound(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("GetNamespaces", "customer1").Return([]cmd.Namespace{}, nil)

	res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.GetNamespaceNotFound)
	assert.True(t, ok, "expected 404, got %T", res)
}

func TestGetNamespace_RaftError(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("GetNamespaces", "customer1").Return(nil, errors.New("boom"))

	res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.GetNamespaceInternalServerError)
	assert.True(t, ok, "expected 500, got %T", res)
}

func TestGetNamespace_OK(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("GetNamespaces", "customer1").Return([]cmd.Namespace{{Name: "customer1"}}, nil)

	res := h.getNamespace(nsops.GetNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	parsed, ok := res.(*nsops.GetNamespaceOK)
	require.True(t, ok, "expected 200, got %T", res)
	require.NotNil(t, parsed.Payload)
	assert.Equal(t, "customer1", parsed.Payload.Name)
}

// -----------------------------------------------------------------------------
// deleteNamespace
// -----------------------------------------------------------------------------

func TestDeleteNamespace_Forbidden(t *testing.T) {
	h, authz, _ := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("customer1")[0]).
		Return(authzerrors.NewForbidden(principal, authorization.DELETE, authorization.Namespaces("customer1")...))

	res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.DeleteNamespaceForbidden)
	assert.True(t, ok, "expected 403, got %T", res)
}

func TestDeleteNamespace_InvalidNameRejected(t *testing.T) {
	h, authz, _ := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("BadName")[0]).Return(nil)

	res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "BadName", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.DeleteNamespaceUnprocessableEntity)
	assert.True(t, ok, "expected 422, got %T", res)
}

func TestDeleteNamespace_NotFoundOnRaftErrNotFound(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("DeleteNamespace", "customer1").
		Return(fmt.Errorf("%w: %q", usecasesNamespaces.ErrNotFound, "customer1"))

	res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.DeleteNamespaceNotFound)
	assert.True(t, ok, "expected 404, got %T", res)
}

func TestDeleteNamespace_RaftDeleteError(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("DeleteNamespace", "customer1").Return(errors.New("raft boom"))

	res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.DeleteNamespaceInternalServerError)
	assert.True(t, ok, "expected 500, got %T", res)
}

func TestDeleteNamespace_NoContent(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	authz.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Namespaces("customer1")[0]).Return(nil)
	raft.On("DeleteNamespace", "customer1").Return(nil)

	res := h.deleteNamespace(nsops.DeleteNamespaceParams{NamespaceID: "customer1", HTTPRequest: req}, principal)
	_, ok := res.(*nsops.DeleteNamespaceNoContent)
	assert.True(t, ok, "expected 204, got %T", res)
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

func TestListNamespaces_RaftError(t *testing.T) {
	h, _, raft := newHandler(t)
	principal := &models.Principal{}
	raft.On("GetNamespaces").Return(nil, errors.New("boom"))

	res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
	_, ok := res.(*nsops.ListNamespacesInternalServerError)
	assert.True(t, ok, "expected 500, got %T", res)
}

func TestListNamespaces_FilteredByAuthz(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	all := []cmd.Namespace{{Name: "customer1"}, {Name: "customer2"}, {Name: "customer3"}}
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

func TestListNamespaces_FilterError(t *testing.T) {
	h, authz, raft := newHandler(t)
	principal := &models.Principal{}
	all := []cmd.Namespace{{Name: "customer1"}}
	raft.On("GetNamespaces").Return(all, nil)
	authz.On("FilterAuthorizedResources",
		mock.Anything, principal, authorization.READ,
		authorization.Namespaces("customer1")[0],
	).Return(nil, errors.New("filter boom"))

	res := h.listNamespaces(nsops.ListNamespacesParams{HTTPRequest: req}, principal)
	_, ok := res.(*nsops.ListNamespacesInternalServerError)
	assert.True(t, ok, "expected 500, got %T", res)
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

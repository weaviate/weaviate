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

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// NamespaceRaftGetter is the subset of cluster.Raft the handlers use. Keeping
// it narrow makes the unit tests easy to mock.
type NamespaceRaftGetter interface {
	AddNamespace(ns cmd.Namespace) error
	DeleteNamespace(name string) error
	GetNamespaces(names ...string) ([]cmd.Namespace, error)
}

type namespaceHandler struct {
	enabled    bool
	authorizer authorization.Authorizer
	raft       NamespaceRaftGetter
	logger     logrus.FieldLogger
}

// errNamespacesDisabled is returned by every handler when the namespaces
// feature flag is off, so clients see a consistent message regardless of
// which endpoint they hit.
var errNamespacesDisabled = fmt.Errorf("namespaces are not enabled")

// SetupHandlers wires the namespace handler methods into the generated REST
// API surface. Called from adapters/handlers/rest/configure_api.go next to the
// other SetupHandlers invocations.
func SetupHandlers(
	enabled bool,
	api *operations.WeaviateAPI,
	raft NamespaceRaftGetter,
	authorizer authorization.Authorizer,
	logger logrus.FieldLogger,
) {
	h := &namespaceHandler{
		enabled:    enabled,
		authorizer: authorizer,
		raft:       raft,
		logger:     logger,
	}

	api.NamespacesCreateNamespaceHandler = nsops.CreateNamespaceHandlerFunc(h.createNamespace)
	api.NamespacesDeleteNamespaceHandler = nsops.DeleteNamespaceHandlerFunc(h.deleteNamespace)
	api.NamespacesGetNamespaceHandler = nsops.GetNamespaceHandlerFunc(h.getNamespace)
	api.NamespacesListNamespacesHandler = nsops.ListNamespacesHandlerFunc(h.listNamespaces)
}

// disabledResponder returns a 404 with an ErrorResponse body when the
// namespaces feature flag is off. We use a raw ResponderFunc because the
// generated go-swagger response types for these endpoints do not include a
// 404 variant for create/list — and a 404 is the correct semantic for "this
// endpoint is not available on this cluster".
func disabledResponder() middleware.Responder {
	return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(cerrors.ErrPayloadFromSingleErr(errNamespacesDisabled))
	})
}

func (h *namespaceHandler) createNamespace(params nsops.CreateNamespaceParams, principal *models.Principal) middleware.Responder {
	if !h.enabled {
		return disabledResponder()
	}

	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Namespaces(name)...); err != nil {
		return nsops.NewCreateNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	// No pre-check for existence: the RAFT apply layer is the single source
	// of truth for uniqueness, so we translate its error sentinels directly.
	// This avoids a TOCTOU where two concurrent creates both pass a pre-check
	// and the loser would surface a misleading 500.
	if err := h.raft.AddNamespace(cmd.Namespace{Name: name}); err != nil {
		switch {
		case errors.Is(err, usecasesNamespaces.ErrAlreadyExists):
			return nsops.NewCreateNamespaceConflict().WithPayload(
				cerrors.ErrPayloadFromSingleErr(fmt.Errorf("namespace %q already exists", name)))
		case errors.Is(err, usecasesNamespaces.ErrBadRequest):
			return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		default:
			return nsops.NewCreateNamespaceInternalServerError().WithPayload(
				cerrors.ErrPayloadFromSingleErr(fmt.Errorf("creating namespace: %w", err)))
		}
	}

	return nsops.NewCreateNamespaceCreated().WithPayload(&models.Namespace{Name: name})
}

func (h *namespaceHandler) getNamespace(params nsops.GetNamespaceParams, principal *models.Principal) middleware.Responder {
	if !h.enabled {
		return disabledResponder()
	}

	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Namespaces(name)...); err != nil {
		return nsops.NewGetNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewGetNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	got, err := h.raft.GetNamespaces(name)
	if err != nil {
		return nsops.NewGetNamespaceInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(fmt.Errorf("getting namespace: %w", err)))
	}
	if len(got) == 0 {
		return nsops.NewGetNamespaceNotFound().WithPayload(
			cerrors.ErrPayloadFromSingleErr(fmt.Errorf("namespace %q not found", name)))
	}

	return nsops.NewGetNamespaceOK().WithPayload(&models.Namespace{Name: got[0].Name})
}

func (h *namespaceHandler) deleteNamespace(params nsops.DeleteNamespaceParams, principal *models.Principal) middleware.Responder {
	if !h.enabled {
		return disabledResponder()
	}

	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Namespaces(name)...); err != nil {
		return nsops.NewDeleteNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewDeleteNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	// No pre-check for existence: the RAFT apply layer returns ErrNotFound
	// for missing entries, so we translate directly. Avoids a TOCTOU where
	// two concurrent deletes would both see the entry and only one succeeds.
	if err := h.raft.DeleteNamespace(name); err != nil {
		switch {
		case errors.Is(err, usecasesNamespaces.ErrNotFound):
			return nsops.NewDeleteNamespaceNotFound().WithPayload(
				cerrors.ErrPayloadFromSingleErr(fmt.Errorf("namespace %q not found", name)))
		case errors.Is(err, usecasesNamespaces.ErrBadRequest):
			return nsops.NewDeleteNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		default:
			return nsops.NewDeleteNamespaceInternalServerError().WithPayload(
				cerrors.ErrPayloadFromSingleErr(fmt.Errorf("deleting namespace: %w", err)))
		}
	}

	return nsops.NewDeleteNamespaceNoContent()
}

// listNamespaces never returns 403. Callers without any applicable
// manage_namespaces permission see an empty list, matching the listRoles
// convention so RBAC UIs can render a consistent empty state.
func (h *namespaceHandler) listNamespaces(params nsops.ListNamespacesParams, principal *models.Principal) middleware.Responder {
	if !h.enabled {
		return disabledResponder()
	}

	ctx := params.HTTPRequest.Context()

	all, err := h.raft.GetNamespaces()
	if err != nil {
		return nsops.NewListNamespacesInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(fmt.Errorf("listing namespaces: %w", err)))
	}
	if len(all) == 0 {
		return nsops.NewListNamespacesOK().WithPayload([]*models.Namespace{})
	}

	resources := make([]string, len(all))
	for i, ns := range all {
		resources[i] = authorization.Namespaces(ns.Name)[0]
	}

	allowed, err := h.authorizer.FilterAuthorizedResources(ctx, principal, authorization.READ, resources...)
	if err != nil {
		return nsops.NewListNamespacesInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(fmt.Errorf("filtering authorized namespaces: %w", err)))
	}

	allowedSet := make(map[string]struct{}, len(allowed))
	for _, r := range allowed {
		allowedSet[r] = struct{}{}
	}

	out := make([]*models.Namespace, 0, len(allowed))
	for _, ns := range all {
		if _, ok := allowedSet[authorization.Namespaces(ns.Name)[0]]; ok {
			out = append(out, &models.Namespace{Name: ns.Name})
		}
	}
	return nsops.NewListNamespacesOK().WithPayload(out)
}

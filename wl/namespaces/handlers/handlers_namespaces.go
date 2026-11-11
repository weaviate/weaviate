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

package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/cluster"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// NamespaceRaftGetter is the subset of cluster.Raft the handlers use. Keeping
// it narrow makes the unit tests easy to mock.
type NamespaceRaftGetter interface {
	AddNamespace(ctx context.Context, ns cmd.Namespace) (cmd.Namespace, uint64, error)
	UpdateNamespace(ctx context.Context, ns cmd.Namespace) (uint64, error)
	ChangeNamespaceState(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error)
	ChangeNamespaceStateIfUnchanged(ctx context.Context, name string, target cmd.NamespaceState) (uint64, error)
	GetNamespaces(names ...string) ([]cmd.Namespace, error)
	cluster.StorageCandidateLister
}

type namespaceHandler struct {
	authorizer authorization.Authorizer
	raft       NamespaceRaftGetter
}

// SetupHandlers registers the seven namespace operations on api. The handlers
// check neither NAMESPACES_ENABLED nor the license key, so call it only when
// the namespaces feature's license.Mode is FeatureLicensed.
func SetupHandlers(
	api *operations.WeaviateAPI,
	raft NamespaceRaftGetter,
	authorizer authorization.Authorizer,
) {
	h := &namespaceHandler{
		authorizer: authorizer,
		raft:       raft,
	}

	api.NamespacesCreateNamespaceHandler = nsops.CreateNamespaceHandlerFunc(h.createNamespace)
	api.NamespacesUpdateNamespaceHandler = nsops.UpdateNamespaceHandlerFunc(h.updateNamespace)
	api.NamespacesDeleteNamespaceHandler = nsops.DeleteNamespaceHandlerFunc(h.deleteNamespace)
	api.NamespacesGetNamespaceHandler = nsops.GetNamespaceHandlerFunc(h.getNamespace)
	api.NamespacesListNamespacesHandler = nsops.ListNamespacesHandlerFunc(h.listNamespaces)
	api.NamespacesSuspendNamespaceHandler = nsops.SuspendNamespaceHandlerFunc(h.suspendNamespace)
	api.NamespacesResumeNamespaceHandler = nsops.ResumeNamespaceHandlerFunc(h.resumeNamespace)
}

func (h *namespaceHandler) createNamespace(params nsops.CreateNamespaceParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Namespaces(name)...); err != nil {
		return nsops.NewCreateNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	var body models.NamespaceCreateRequest
	if params.Body != nil {
		body = *params.Body
	}
	homeNodes, err := requestHomeNodes(body.HomeNode, body.HomeNodes) //nolint:staticcheck // deprecated on the wire but still read and filled for old clients
	if err != nil {
		return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	// A request with neither field leaves homeNodes nil, and AddNamespace then
	// auto-picks MaximumFactor nodes, or refuses when no maximum is set.
	if len(homeNodes) > 0 {
		if err := h.checkHomeNodes(homeNodes); err != nil {
			return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	// No pre-check for existence — the RAFT apply layer owns uniqueness, so
	// we translate its sentinels directly. A pre-check would TOCTOU under
	// concurrent creates and surface a misleading 500 on the loser.
	created, _, err := h.raft.AddNamespace(ctx, cmd.Namespace{Name: name, HomeNodes: homeNodes})
	if err != nil {
		switch {
		case errors.Is(err, usecasesNamespaces.ErrAlreadyExists):
			return nsops.NewCreateNamespaceConflict().WithPayload(
				cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q already exists", name)))
		case errors.Is(err, usecasesNamespaces.ErrNamespaceDeleting):
			// Deleting is a transient state conflict — 409 to retry after
			// cleanup, not the terminal-family 422.
			return nsops.NewCreateNamespaceConflict().WithPayload(
				cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q is being deleted; retry after cleanup completes", name)))
		case errors.Is(err, usecasesNamespaces.ErrBadRequest):
			return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		if status, ok := cerrors.HTTPStatusForNamespaceErr(err); ok && status == http.StatusUnprocessableEntity {
			return nsops.NewCreateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		return nsops.NewCreateNamespaceInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("creating namespace: %w", err)))
	}

	created.State = cmd.NamespaceStateActive
	return nsops.NewCreateNamespaceCreated().WithPayload(namespaceModel(created))
}

func (h *namespaceHandler) updateNamespace(params nsops.UpdateNamespaceParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Namespaces(name)...); err != nil {
		return nsops.NewUpdateNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	var body models.NamespaceUpdateRequest
	if params.Body != nil {
		body = *params.Body
	}
	var homeNode string
	if hn := body.HomeNode; hn != nil { //nolint:staticcheck // deprecated on the wire but still read and filled for old clients
		homeNode = *hn
	}
	homeNodes, err := requestHomeNodes(homeNode, body.HomeNodes)
	if err != nil {
		return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if len(homeNodes) == 0 {
		return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, errors.New("home_nodes is required")))
	}
	if err := h.checkHomeNodes(homeNodes); err != nil {
		return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if _, err := h.raft.UpdateNamespace(ctx, cmd.Namespace{Name: name, HomeNodes: homeNodes}); err != nil {
		switch {
		case errors.Is(err, usecasesNamespaces.ErrNotFound):
			return nsops.NewUpdateNamespaceNotFound().WithPayload(
				cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q not found", name)))
		case errors.Is(err, usecasesNamespaces.ErrNamespaceDeleting):
			// Not a retryable conflict: the namespace never leaves deleting, so
			// no later retry of this update can succeed. Cased explicitly for
			// the tailored message; the status matches the family's 422.
			return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(
				cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q is being deleted; home_nodes cannot be updated", name)))
		case errors.Is(err, usecasesNamespaces.ErrBadRequest):
			return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		if status, ok := cerrors.HTTPStatusForNamespaceErr(err); ok && status == http.StatusUnprocessableEntity {
			return nsops.NewUpdateNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		return nsops.NewUpdateNamespaceInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("updating namespace: %w", err)))
	}

	// Read State from the controller rather than hardcoding active so the
	// response stays correct if Update later returns into other states.
	// Empty on read failure / concurrent delete.
	updated := cmd.Namespace{Name: name, HomeNodes: homeNodes}
	if got, err := h.raft.GetNamespaces(name); err == nil && len(got) == 1 {
		updated.State = got[0].State
	}
	return nsops.NewUpdateNamespaceOK().WithPayload(namespaceModel(updated))
}

// requestHomeNodes resolves a create or update body's home-node list. An empty
// homeNode or homeNodes counts as unset, and a set homeNode is a one-entry list.
func requestHomeNodes(homeNode string, homeNodes []string) ([]string, error) {
	switch {
	case homeNode != "" && len(homeNodes) > 0:
		return nil, errors.New("home_node and home_nodes are both set; send only home_nodes")
	case homeNode != "":
		return []string{homeNode}, nil
	case len(homeNodes) == 0:
		return nil, nil
	default:
		return homeNodes, nil
	}
}

// checkHomeNodes refuses a list with an empty or repeated entry, or with an
// entry that is not a current storage candidate.
func (h *namespaceHandler) checkHomeNodes(homeNodes []string) error {
	if err := usecasesNamespaces.ValidateHomeNodes(homeNodes); err != nil {
		return err
	}
	ns := cmd.Namespace{HomeNodes: homeNodes}
	if missing := ns.MissingHomeNodes(h.raft.StorageCandidates()); len(missing) > 0 {
		return fmt.Errorf("home_nodes entries %q are not current storage candidates", missing)
	}
	return nil
}

// namespaceModel builds the REST view of ns. home_node carries the first
// HomeNodes entry for clients that predate home_nodes.
func namespaceModel(ns cmd.Namespace) *models.Namespace {
	out := &models.Namespace{Name: ns.Name, HomeNodes: ns.HomeNodes, State: string(ns.State)}
	if len(ns.HomeNodes) > 0 {
		out.HomeNode = ns.HomeNodes[0] //nolint:staticcheck // deprecated on the wire but still read and filled for old clients
	}
	return out
}

func (h *namespaceHandler) getNamespace(params nsops.GetNamespaceParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Namespaces(name)...); err != nil {
		return nsops.NewGetNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewGetNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	got, err := h.raft.GetNamespaces(name)
	if err != nil {
		return nsops.NewGetNamespaceInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("getting namespace: %w", err)))
	}
	if len(got) == 0 {
		return nsops.NewGetNamespaceNotFound().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q not found", name)))
	}

	return nsops.NewGetNamespaceOK().WithPayload(namespaceModel(got[0]))
}

func (h *namespaceHandler) deleteNamespace(params nsops.DeleteNamespaceParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	name := params.NamespaceID

	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Namespaces(name)...); err != nil {
		return nsops.NewDeleteNamespaceForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return nsops.NewDeleteNamespaceUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// Flip to deleting; the apply handler is idempotent on already-deleting
	// (returns nil). Keys in the namespace stop authenticating as soon as the
	// flip applies, and the rows are reclaimed by the cleanup tick.
	if _, err := h.raft.ChangeNamespaceState(ctx, name, cmd.NamespaceStateDeleting); err != nil {
		if errors.Is(err, usecasesNamespaces.ErrNotFound) {
			return nsops.NewDeleteNamespaceNotFound().WithPayload(
				cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("namespace %q not found", name)))
		}
		return nsops.NewDeleteNamespaceInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("marking namespace for deletion: %w", err)))
	}

	return nsops.NewDeleteNamespaceAccepted()
}

func (h *namespaceHandler) suspendNamespace(params nsops.SuspendNamespaceParams, principal *models.Principal) middleware.Responder {
	status, err := h.changeState(params.HTTPRequest.Context(), principal, params.NamespaceID, cmd.NamespaceStateSuspended)
	body := cerrors.ErrPayloadFromSingleErr(principal, err)
	switch status {
	case http.StatusAccepted:
		return nsops.NewSuspendNamespaceAccepted()
	case http.StatusForbidden:
		return nsops.NewSuspendNamespaceForbidden().WithPayload(body)
	case http.StatusNotFound:
		return nsops.NewSuspendNamespaceNotFound().WithPayload(body)
	case http.StatusConflict:
		return nsops.NewSuspendNamespaceConflict().WithPayload(body)
	case http.StatusUnprocessableEntity:
		return nsops.NewSuspendNamespaceUnprocessableEntity().WithPayload(body)
	case http.StatusServiceUnavailable:
		return nsops.NewSuspendNamespaceServiceUnavailable().WithPayload(body)
	default:
		return nsops.NewSuspendNamespaceInternalServerError().WithPayload(body)
	}
}

func (h *namespaceHandler) resumeNamespace(params nsops.ResumeNamespaceParams, principal *models.Principal) middleware.Responder {
	status, err := h.changeState(params.HTTPRequest.Context(), principal, params.NamespaceID, cmd.NamespaceStateActive)
	body := cerrors.ErrPayloadFromSingleErr(principal, err)
	switch status {
	case http.StatusAccepted:
		return nsops.NewResumeNamespaceAccepted()
	case http.StatusForbidden:
		return nsops.NewResumeNamespaceForbidden().WithPayload(body)
	case http.StatusNotFound:
		return nsops.NewResumeNamespaceNotFound().WithPayload(body)
	case http.StatusConflict:
		return nsops.NewResumeNamespaceConflict().WithPayload(body)
	case http.StatusUnprocessableEntity:
		return nsops.NewResumeNamespaceUnprocessableEntity().WithPayload(body)
	case http.StatusServiceUnavailable:
		return nsops.NewResumeNamespaceServiceUnavailable().WithPayload(body)
	default:
		return nsops.NewResumeNamespaceInternalServerError().WithPayload(body)
	}
}

// changeState authorizes, validates and applies a namespace state change,
// returning the HTTP status its caller should render and the error to put in
// the body. Both endpoints are the same flow with a different target state.
func (h *namespaceHandler) changeState(
	ctx context.Context, principal *models.Principal, name string, target cmd.NamespaceState,
) (int, error) {
	// resumeNamespace calls changeState too, so AuthorizeAndRequireActiveNamespace
	// here would refuse to resume a suspended namespace.
	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Namespaces(name)...); err != nil {
		return http.StatusForbidden, err
	}
	if err := usecasesNamespaces.ValidateName(name); err != nil {
		return http.StatusUnprocessableEntity, err
	}

	// Conditional because suspend and resume undo each other: an Execute
	// retry must not revert a flip another operator was already told applied.
	if _, err := h.raft.ChangeNamespaceStateIfUnchanged(ctx, name, target); err != nil {
		status := statusForChangeStateErr(err)
		if status == http.StatusNotFound {
			return status, fmt.Errorf("namespace %q not found", name)
		}
		return status, fmt.Errorf("changing namespace state: %w", err)
	}

	return http.StatusAccepted, nil
}

// statusForChangeStateErr maps an apply failure to its REST status. The 404,
// 409 and leadership 503 are classified here rather than through the shared
// namespace-status helper, which does not cover them.
func statusForChangeStateErr(err error) int {
	switch {
	case errors.Is(err, usecasesNamespaces.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, usecasesNamespaces.ErrStateChangedConcurrently):
		return http.StatusConflict
	case errors.Is(err, usecasesNamespaces.ErrInvalidStateTransition):
		return http.StatusUnprocessableEntity
	case types.IsNoLeader(err):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// listNamespaces never returns 403. Callers without any applicable
// manage_namespaces permission see an empty list, matching the listRoles
// convention so RBAC UIs can render a consistent empty state.
func (h *namespaceHandler) listNamespaces(params nsops.ListNamespacesParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	all, err := h.raft.GetNamespaces()
	if err != nil {
		return nsops.NewListNamespacesInternalServerError().WithPayload(
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("listing namespaces: %w", err)))
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
			cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("filtering authorized namespaces: %w", err)))
	}

	allowedSet := make(map[string]struct{}, len(allowed))
	for _, r := range allowed {
		allowedSet[r] = struct{}{}
	}

	out := make([]*models.Namespace, 0, len(allowed))
	for _, ns := range all {
		if _, ok := allowedSet[authorization.Namespaces(ns.Name)[0]]; ok {
			out = append(out, namespaceModel(ns))
		}
	}
	return nsops.NewListNamespacesOK().WithPayload(out)
}

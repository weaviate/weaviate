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

package rest

import (
	"fmt"
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/license"
)

// errNamespacesDisabled is what every namespace operation answers with on a
// node with NAMESPACES_ENABLED off.
var errNamespacesDisabled = fmt.Errorf("namespaces are not enabled")

// setupNamespaceHandlers registers the seven namespace operations for mode.
// setupWL runs only in FeatureLicensed, and any mode but FeatureOff and
// FeatureLicensed gets the license refusal.
func setupNamespaceHandlers(api *operations.WeaviateAPI, mode license.Mode, setupWL func(*operations.WeaviateAPI)) {
	switch mode {
	case license.FeatureOff:
		setupNamespacesDisabledHandlers(api)
		return
	case license.FeatureLicensed:
		setupWL(api)
		return
	case license.FeatureUnlicensed:
	}
	setupNamespacesUnlicensedHandlers(api)
}

func setupNamespacesDisabledHandlers(api *operations.WeaviateAPI) {
	r := disabledResponder()
	api.NamespacesCreateNamespaceHandler = nsops.CreateNamespaceHandlerFunc(respondWith[nsops.CreateNamespaceParams](r))
	api.NamespacesUpdateNamespaceHandler = nsops.UpdateNamespaceHandlerFunc(respondWith[nsops.UpdateNamespaceParams](r))
	api.NamespacesDeleteNamespaceHandler = nsops.DeleteNamespaceHandlerFunc(respondWith[nsops.DeleteNamespaceParams](r))
	api.NamespacesGetNamespaceHandler = nsops.GetNamespaceHandlerFunc(respondWith[nsops.GetNamespaceParams](r))
	api.NamespacesListNamespacesHandler = nsops.ListNamespacesHandlerFunc(respondWith[nsops.ListNamespacesParams](r))
	api.NamespacesSuspendNamespaceHandler = nsops.SuspendNamespaceHandlerFunc(respondWith[nsops.SuspendNamespaceParams](r))
	api.NamespacesResumeNamespaceHandler = nsops.ResumeNamespaceHandlerFunc(respondWith[nsops.ResumeNamespaceParams](r))
}

// setupNamespacesUnlicensedHandlers answers every namespace operation with 403
// before any authorization check, so every caller gets the license refusal
// whatever their permissions.
func setupNamespacesUnlicensedHandlers(api *operations.WeaviateAPI) {
	body := cerrors.ErrPayloadFromSingleErr(nil, license.Required(namespacesFeature))
	api.NamespacesCreateNamespaceHandler = nsops.CreateNamespaceHandlerFunc(
		respondWith[nsops.CreateNamespaceParams](nsops.NewCreateNamespaceForbidden().WithPayload(body)))
	api.NamespacesUpdateNamespaceHandler = nsops.UpdateNamespaceHandlerFunc(
		respondWith[nsops.UpdateNamespaceParams](nsops.NewUpdateNamespaceForbidden().WithPayload(body)))
	api.NamespacesDeleteNamespaceHandler = nsops.DeleteNamespaceHandlerFunc(
		respondWith[nsops.DeleteNamespaceParams](nsops.NewDeleteNamespaceForbidden().WithPayload(body)))
	api.NamespacesGetNamespaceHandler = nsops.GetNamespaceHandlerFunc(
		respondWith[nsops.GetNamespaceParams](nsops.NewGetNamespaceForbidden().WithPayload(body)))
	api.NamespacesListNamespacesHandler = nsops.ListNamespacesHandlerFunc(
		respondWith[nsops.ListNamespacesParams](nsops.NewListNamespacesForbidden().WithPayload(body)))
	api.NamespacesSuspendNamespaceHandler = nsops.SuspendNamespaceHandlerFunc(
		respondWith[nsops.SuspendNamespaceParams](nsops.NewSuspendNamespaceForbidden().WithPayload(body)))
	api.NamespacesResumeNamespaceHandler = nsops.ResumeNamespaceHandlerFunc(
		respondWith[nsops.ResumeNamespaceParams](nsops.NewResumeNamespaceForbidden().WithPayload(body)))
}

// respondWith returns a handler that answers every request with r and reads
// nothing from it.
func respondWith[P any](r middleware.Responder) func(P, *models.Principal) middleware.Responder {
	return func(P, *models.Principal) middleware.Responder { return r }
}

// disabledResponder answers 404 with errNamespacesDisabled. It is untyped, so
// one responder serves all seven operations.
func disabledResponder() middleware.Responder {
	return jsonResponder(http.StatusNotFound, cerrors.ErrPayloadFromSingleErr(nil, errNamespacesDisabled))
}

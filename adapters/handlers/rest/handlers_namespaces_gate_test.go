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
	"net/http"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	nsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/namespaces"
	"github.com/weaviate/weaviate/usecases/license"
)

// namespaceOperations calls each namespace operation through the handler
// registered on api. An unregistered operation answers 501.
var namespaceOperations = []struct {
	name string
	call func(api *operations.WeaviateAPI) middleware.Responder
}{
	{name: "create", call: callCreateNamespace},
	{name: "update", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesUpdateNamespaceHandler.Handle(nsops.UpdateNamespaceParams{NamespaceID: "customer1"}, nil)
	}},
	{name: "get", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesGetNamespaceHandler.Handle(nsops.GetNamespaceParams{NamespaceID: "customer1"}, nil)
	}},
	{name: "list", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesListNamespacesHandler.Handle(nsops.ListNamespacesParams{}, nil)
	}},
	{name: "delete", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesDeleteNamespaceHandler.Handle(nsops.DeleteNamespaceParams{NamespaceID: "customer1"}, nil)
	}},
	{name: "suspend", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesSuspendNamespaceHandler.Handle(nsops.SuspendNamespaceParams{NamespaceID: "customer1"}, nil)
	}},
	{name: "resume", call: func(api *operations.WeaviateAPI) middleware.Responder {
		return api.NamespacesResumeNamespaceHandler.Handle(nsops.ResumeNamespaceParams{NamespaceID: "customer1"}, nil)
	}},
}

func callCreateNamespace(api *operations.WeaviateAPI) middleware.Responder {
	return api.NamespacesCreateNamespaceHandler.Handle(nsops.CreateNamespaceParams{NamespaceID: "customer1"}, nil)
}

var licenseRefusal = license.Required(namespacesFeature).Error()

func requireAnswer(t *testing.T, resp middleware.Responder, wantCode int, wantMsg string) {
	t.Helper()
	code, body := statusOf(t, resp)
	require.Equal(t, wantCode, code)
	if wantMsg == "" {
		return
	}
	require.Len(t, body.Error, 1)
	require.Contains(t, body.Error[0].Message, wantMsg)
}

func TestNamespacesDisabledHandlers(t *testing.T) {
	api := operations.NewWeaviateAPI(nil)
	setupNamespacesDisabledHandlers(api)

	for _, op := range namespaceOperations {
		t.Run(op.name, func(t *testing.T) {
			requireAnswer(t, op.call(api), http.StatusNotFound, "namespaces are not enabled")
		})
	}
}

func TestNamespacesUnlicensedHandlers(t *testing.T) {
	api := operations.NewWeaviateAPI(nil)
	setupNamespacesUnlicensedHandlers(api)

	for _, op := range namespaceOperations {
		t.Run(op.name, func(t *testing.T) {
			requireAnswer(t, op.call(api), http.StatusForbidden, licenseRefusal)
		})
	}
}

func TestSetupNamespaceHandlers(t *testing.T) {
	cases := []struct {
		name        string
		mode        license.Mode
		wantWLCalls int
		wantCode    int
		wantMsg     string
	}{
		{name: "off answers the disabled 404", mode: license.FeatureOff, wantCode: http.StatusNotFound, wantMsg: "namespaces are not enabled"},
		{name: "unlicensed answers the license 403", mode: license.FeatureUnlicensed, wantCode: http.StatusForbidden, wantMsg: licenseRefusal},
		{name: "licensed registers the wl handlers", mode: license.FeatureLicensed, wantWLCalls: 1, wantCode: http.StatusCreated},
		{name: "a mode outside the three answers the license 403", mode: license.Mode(99), wantCode: http.StatusForbidden, wantMsg: licenseRefusal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := operations.NewWeaviateAPI(nil)
			wlCalls := 0
			setupWL := func(api *operations.WeaviateAPI) {
				wlCalls++
				api.NamespacesCreateNamespaceHandler = nsops.CreateNamespaceHandlerFunc(
					respondWith[nsops.CreateNamespaceParams](nsops.NewCreateNamespaceCreated()))
			}

			setupNamespaceHandlers(api, tc.mode, setupWL)

			require.Equal(t, tc.wantWLCalls, wlCalls)
			requireAnswer(t, callCreateNamespace(api), tc.wantCode, tc.wantMsg)
		})
	}
}

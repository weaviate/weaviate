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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	nodesops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/nodes"
	objectsops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	schemaops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// TestResolverErrorStatus covers the handlers that answer a class-name
// resolver error with a status of their own. A Forbidden answers 403 there,
// and any other error keeps the handler's status.
func TestResolverErrorStatus(t *testing.T) {
	logger, _ := test.NewNullLogger()
	principal := &models.Principal{Username: "admin"}
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	sites := []struct {
		name        string
		otherStatus int
		respond     func(err error) middleware.Responder
	}{
		{
			name: "tokenize", otherStatus: http.StatusUnprocessableEntity,
			respond: func(err error) middleware.Responder {
				return propertyTokenize(schemaops.SchemaObjectsPropertiesTokenizeParams{
					HTTPRequest: req, ClassName: "Movies", PropertyName: "title",
				}, principal, nil, namespacing.Refusing(err), logger)
			},
		},
		{
			name: "node status by class", otherStatus: http.StatusUnprocessableEntity,
			respond: func(err error) middleware.Responder {
				h := &nodesHandlers{qualifier: namespacing.Refusing(err)}
				return h.getNodesStatusByClass(nodesops.NodesGetClassParams{HTTPRequest: req, ClassName: "Movies"}, principal)
			},
		},
		{
			name: "property index delete", otherStatus: http.StatusUnprocessableEntity,
			respond: func(err error) middleware.Responder {
				h := &schemaHandlers{
					qualifier:           namespacing.Refusing(err),
					metricRequestsTotal: newSchemaRequestsTotal(nil, logger),
				}
				return h.deleteClassPropertyIndex(schemaops.SchemaObjectsPropertiesDeleteParams{
					HTTPRequest: req, ClassName: "Movies", PropertyName: "title", IndexName: "searchable",
				}, principal)
			},
		},
		{
			name: "index upsert", otherStatus: http.StatusNotFound,
			respond: func(err error) middleware.Responder {
				h := &indexesHandlers{appState: &state.State{NamespaceQualifier: namespacing.Refusing(err)}}
				return h.upsertIndex(schemaops.SchemaObjectsIndexUpsertParams{
					HTTPRequest: req, ClassName: "Movies", PropertyName: "title", IndexName: "searchable",
				}, principal)
			},
		},
		{
			name: "index rebuild", otherStatus: http.StatusNotFound,
			respond: func(err error) middleware.Responder {
				h := &indexesHandlers{appState: &state.State{NamespaceQualifier: namespacing.Refusing(err)}}
				return h.rebuildIndex(schemaops.SchemaObjectsIndexRebuildParams{
					HTTPRequest: req, ClassName: "Movies", PropertyName: "title", IndexName: "searchable",
				}, principal)
			},
		},
		{
			name: "index cancel", otherStatus: http.StatusNotFound,
			respond: func(err error) middleware.Responder {
				h := &indexesHandlers{appState: &state.State{NamespaceQualifier: namespacing.Refusing(err)}}
				return h.cancelIndex(schemaops.SchemaObjectsIndexCancelParams{
					HTTPRequest: req, ClassName: "Movies", PropertyName: "title", IndexName: "searchable",
				}, principal)
			},
		},
		{
			name: "getIndexes", otherStatus: http.StatusUnprocessableEntity,
			respond: func(err error) middleware.Responder {
				h := &indexesHandlers{appState: &state.State{NamespaceQualifier: namespacing.Refusing(err)}}
				return h.getIndexes(schemaops.SchemaObjectsIndexesGetParams{HTTPRequest: req, ClassName: "Movies"}, principal)
			},
		},
		{
			name: "search", otherStatus: http.StatusBadRequest,
			respond: func(err error) middleware.Responder {
				h := restsearch.NewHandler(restsearch.HandlerConfig{Qualifier: namespacing.Refusing(err), Logger: logger})
				_, apiErr := h.Bm25(context.Background(), principal, "Movies", &models.SearchBm25Request{})
				return searchBm25ErrResponder(apiErr)
			},
		},
		{
			name: "aggregate", otherStatus: http.StatusBadRequest,
			respond: func(err error) middleware.Responder {
				h := restsearch.NewHandler(restsearch.HandlerConfig{Qualifier: namespacing.Refusing(err), Logger: logger})
				_, apiErr := h.Aggregate(context.Background(), principal, "Movies", &models.AggregateRequest{})
				return aggregateErrResponder(apiErr)
			},
		},
		{
			// GetObjectClassFromName fails on the resolver and on its own
			// authorization, so the fake returns err directly.
			name: "GET object with include", otherStatus: http.StatusBadRequest,
			respond: func(err error) middleware.Responder {
				h := &objectHandlers{
					manager:             &fakeManager{getObjectClassFromNameErr: err},
					metricRequestsTotal: &fakeMetricRequestsTotal{},
				}
				include := "vector"
				return h.getObject(objectsops.ObjectsClassGetParams{
					HTTPRequest: req, ClassName: "Movies", ID: "5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc", Include: &include,
				}, principal)
			},
		},
		{
			name: "deprecated GET object with include", otherStatus: http.StatusBadRequest,
			respond: func(err error) middleware.Responder {
				h := &objectHandlers{
					manager:             &fakeManager{getObjectsClassErr: err},
					metricRequestsTotal: &fakeMetricRequestsTotal{},
				}
				include := "vector"
				return h.getObject(objectsops.ObjectsClassGetParams{
					HTTPRequest: req, ID: "5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc", Include: &include,
				}, principal)
			},
		},
	}

	errs := []struct {
		name      string
		err       error
		forbidden bool
	}{
		{name: "Forbidden", err: authzerrors.NewForbidden(principal, "read", "collections/Movies"), forbidden: true},
		{name: "wrapped Forbidden", err: fmt.Errorf("rbac: %w", authzerrors.NewForbidden(principal, "read", "collections/Movies")), forbidden: true},
		{name: "other error", err: errors.New("x")},
	}

	for _, site := range sites {
		for _, e := range errs {
			t.Run(site.name+"/"+e.name, func(t *testing.T) {
				want := site.otherStatus
				if e.forbidden {
					want = http.StatusForbidden
				}
				code, body := statusOf(t, site.respond(e.err))
				require.Equal(t, want, code)
				require.Len(t, body.Error, 1)
				require.Contains(t, body.Error[0].Message, e.err.Error())
			})
		}
	}
}

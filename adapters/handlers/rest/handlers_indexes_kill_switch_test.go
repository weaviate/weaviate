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
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestSubmitRefusedWhileRuntimeReindexDisabled pins the wire response for
// every submit entry point while the feature is off. SchemaManager and
// ClusterService are left nil on purpose: both would panic/503 if reached,
// so a clean 400 proves the refusal happens before any of that work. The
// accepted-when-on case is covered by the reindex acceptance suites, which
// submit and expect 202.
func TestSubmitRefusedWhileRuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name string
		call func(h *indexesHandlers) middleware.Responder
	}{
		{
			name: "upsert with a migration body",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.upsertIndex(schema.SchemaObjectsIndexUpsertParams{
					HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
					ClassName:    "Books",
					IndexName:    "searchable",
					PropertyName: "title",
					Body:         &models.IndexUpsertRequest{Algorithm: "blockmax"},
				}, nil)
			},
		},
		{
			name: "upsert with an empty body",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.upsertIndex(schema.SchemaObjectsIndexUpsertParams{
					HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
					ClassName:    "Books",
					IndexName:    "searchable",
					PropertyName: "title",
					Body:         &models.IndexUpsertRequest{},
				}, nil)
			},
		},
		{
			name: "upsert with no body at all",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.upsertIndex(schema.SchemaObjectsIndexUpsertParams{
					HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
					ClassName:    "Books",
					IndexName:    "searchable",
					PropertyName: "title",
				}, nil)
			},
		},
		{
			name: "rebuild searchable",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
					HTTPRequest:  httptest.NewRequest("POST", "/", nil),
					ClassName:    "Books",
					IndexName:    "searchable",
					PropertyName: "title",
				}, nil)
			},
		},
		{
			name: "rebuild filterable",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
					HTTPRequest:  httptest.NewRequest("POST", "/", nil),
					ClassName:    "Books",
					IndexName:    "filterable",
					PropertyName: "title",
				}, nil)
			},
		},
		{
			name: "rebuild rangeable",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
					HTTPRequest:  httptest.NewRequest("POST", "/", nil),
					ClassName:    "Books",
					IndexName:    "rangeable",
					PropertyName: "title",
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &indexesHandlers{appState: &state.State{
				Authorizer:         &authorization.DummyAuthorizer{},
				ServerConfig:       &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: false}},
				ReindexSubmitLocks: state.NewReindexSubmitLocks(),
			}}

			rec := httptest.NewRecorder()
			tt.call(h).WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
			require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
				"the refusal must name the knob that turns the feature on")
		})
	}
}

// TestSubmitNotRefusedWhileRuntimeReindexEnabled pins that the kill switch
// only fires when the knob is off: with it on, the handler proceeds past the
// gate into the class read.
func TestSubmitNotRefusedWhileRuntimeReindexEnabled(t *testing.T) {
	h := &indexesHandlers{appState: &state.State{
		ServerConfig: &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: true}},
	}}

	require.Nil(t, h.refuseIfReindexDisabled(nil))
}

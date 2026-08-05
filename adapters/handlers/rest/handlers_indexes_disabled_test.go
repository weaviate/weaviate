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
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

// disabledIndexesHandlers builds the reindex handlers with runtime reindex
// off. Nothing beyond the config is populated: a handler that reaches past
// the disabled check panics, which is itself the assertion that the check
// comes first.
func disabledIndexesHandlers() *indexesHandlers {
	return &indexesHandlers{appState: &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{RuntimeReindexEnabled: false},
		},
	}}
}

// recordResponse renders a responder so the test can assert on the status
// code and body the client actually receives.
func recordResponse(t *testing.T, resp middleware.Responder) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	return rec
}

func TestUpdateIndex_RuntimeReindexDisabled(t *testing.T) {
	h := disabledIndexesHandlers()

	resp := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		ClassName:    "Books",
		PropertyName: "title",
		Body:         &models.IndexUpdateRequest{},
	}, nil)

	rec := recordResponse(t, resp)
	require.Equal(t, 400, rec.Code, "submit must be refused while runtime reindex is off")
	require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
	require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
		"the refusal must name the knob that turns the feature on")
}

func TestGetIndexes_RuntimeReindexDisabled(t *testing.T) {
	h := disabledIndexesHandlers()

	resp := h.getIndexes(schema.SchemaObjectsIndexesGetParams{ClassName: "Books"}, nil)

	rec := recordResponse(t, resp)
	require.Equal(t, 403, rec.Code, "status must be refused while runtime reindex is off")
	require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
	require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED")
}

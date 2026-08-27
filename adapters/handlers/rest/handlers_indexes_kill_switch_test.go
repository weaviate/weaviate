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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestUpdateIndex_RuntimeReindexDisabled pins the wire response for a
// submit refused while the feature is off. The accepted-when-on case is
// covered by the reindex acceptance suites, which submit and expect 202.
func TestUpdateIndex_RuntimeReindexDisabled(t *testing.T) {
	h := &indexesHandlers{appState: &state.State{
		Authorizer:   &authorization.DummyAuthorizer{},
		ServerConfig: &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: false}},
	}}

	resp := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    "Books",
		PropertyName: "title",
		Body:         &models.IndexUpdateRequest{Searchable: &models.IndexUpdateSearchable{Enabled: true}},
	}, nil)

	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())

	require.Equal(t, 400, rec.Code)
	require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
	require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
		"the refusal must name the knob that turns the feature on")
}

// TestRequestsCancel pins which bodies the refusal exempts: cancel must
// survive the flag being off so a task already running stays stoppable.
func TestRequestsCancel(t *testing.T) {
	tests := []struct {
		name string
		body *models.IndexUpdateRequest
		want bool
	}{
		{name: "nil body"},
		{name: "submit", body: &models.IndexUpdateRequest{Searchable: &models.IndexUpdateSearchable{Enabled: true}}},
		{name: "searchable cancel", body: &models.IndexUpdateRequest{Searchable: &models.IndexUpdateSearchable{Cancel: true}}, want: true},
		{name: "filterable cancel", body: &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Cancel: true}}, want: true},
		{name: "rangeable cancel", body: &models.IndexUpdateRequest{Rangeable: &models.IndexUpdateRangeable{Cancel: true}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, requestsCancel(tt.body))
		})
	}
}

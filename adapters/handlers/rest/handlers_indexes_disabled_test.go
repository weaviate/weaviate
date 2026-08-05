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

// TestReindexEndpoints_RuntimeReindexDisabled pins that every reindex
// endpoint refuses with a 4xx naming the knob while the flag is off.
func TestReindexEndpoints_RuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name     string
		call     func(*indexesHandlers) middleware.Responder
		wantCode int
	}{
		{
			name: "submit and cancel",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
					ClassName:    "Books",
					PropertyName: "title",
					Body:         &models.IndexUpdateRequest{},
				}, nil)
			},
			wantCode: 400,
		},
		{
			name: "status",
			call: func(h *indexesHandlers) middleware.Responder {
				return h.getIndexes(schema.SchemaObjectsIndexesGetParams{
					ClassName: "Books",
				}, nil)
			},
			wantCode: 403,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.call(disabledIndexesHandlers()).WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, tt.wantCode, rec.Code,
				"endpoint must be refused while runtime reindex is off")
			require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
			require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
				"the refusal must name the knob that turns the feature on")
		})
	}
}

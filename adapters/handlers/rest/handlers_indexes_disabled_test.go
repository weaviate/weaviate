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
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

// forbiddenAuthorizer denies every request, standing in for a principal
// with no permission on the collection.
type forbiddenAuthorizer struct{ authorization.DummyAuthorizer }

func (*forbiddenAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, _ ...string) error {
	return authzerrors.NewForbidden(&models.Principal{Username: "nobody"}, "update", "Books")
}

// indexesHandlersWithFlag builds the reindex handlers with runtime reindex
// in the given state. authorizer nil means "allow everything".
func indexesHandlersWithFlag(enabled bool, authorizer authorization.Authorizer) *indexesHandlers {
	if authorizer == nil {
		authorizer = &authorization.DummyAuthorizer{}
	}
	return &indexesHandlers{appState: &state.State{
		Authorizer: authorizer,
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{RuntimeReindexEnabled: enabled},
		},
	}}
}

// updateIndexParams builds a PUT request for the "Books.title" property.
func updateIndexParams(body *models.IndexUpdateRequest) schema.SchemaObjectsIndexesUpdateParams {
	return schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    "Books",
		PropertyName: "title",
		Body:         body,
	}
}

// enableSearchableBody is a submit — the request shape the flag refuses.
func enableSearchableBody() *models.IndexUpdateRequest {
	return &models.IndexUpdateRequest{
		Searchable: &models.IndexUpdateSearchable{Enabled: true},
	}
}

// TestRefuseAsRuntimeReindexDisabled pins which requests the flag refuses.
// Submits are refused; cancel is not, because a task left STARTED in RAFT
// by an earlier flag-on run keeps blocking property mutations and cancel
// is the only way to clear it.
func TestRefuseAsRuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name        string
		flagEnabled bool
		body        *models.IndexUpdateRequest
		wantRefused bool
	}{
		{
			name: "off refuses enable-searchable submit",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Enabled: true},
			},
			wantRefused: true,
		},
		{
			name: "off refuses rebuild submit",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Rebuild: true},
			},
			wantRefused: true,
		},
		{
			name:        "off refuses an empty body",
			body:        &models.IndexUpdateRequest{},
			wantRefused: true,
		},
		{
			name:        "off refuses a nil body",
			body:        nil,
			wantRefused: true,
		},
		{
			name: "off allows searchable cancel",
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Cancel: true},
			},
			wantRefused: false,
		},
		{
			name: "off allows filterable cancel",
			body: &models.IndexUpdateRequest{
				Filterable: &models.IndexUpdateFilterable{Cancel: true},
			},
			wantRefused: false,
		},
		{
			name: "off allows rangeable cancel",
			body: &models.IndexUpdateRequest{
				Rangeable: &models.IndexUpdateRangeable{Cancel: true},
			},
			wantRefused: false,
		},
		{
			name:        "on allows submit",
			flagEnabled: true,
			body: &models.IndexUpdateRequest{
				Searchable: &models.IndexUpdateSearchable{Enabled: true},
			},
			wantRefused: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := indexesHandlersWithFlag(tt.flagEnabled, nil)
			require.Equal(t, tt.wantRefused, h.refuseAsRuntimeReindexDisabled(tt.body))
		})
	}
}

// TestUpdateIndex_RuntimeReindexDisabled pins the response a refused
// submit actually puts on the wire, for a caller that IS authorized.
func TestUpdateIndex_RuntimeReindexDisabled(t *testing.T) {
	resp := indexesHandlersWithFlag(false, nil).
		updateIndex(updateIndexParams(enableSearchableBody()), nil)

	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())

	require.Equal(t, 400, rec.Code, "submit must be refused while runtime reindex is off")
	require.Contains(t, rec.Body.String(), "runtime reindex is disabled")
	require.Contains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
		"the refusal must name the knob that turns the feature on")
}

// TestUpdateIndex_AuthzSpeaksBeforeTheFlag pins that authorization is
// answered first: an ungranted caller gets the same 403 whether the flag
// is on or off, so feature state never leaks to someone who may not read
// it. Without this ordering the flag-off build answers 400 and tells an
// unauthorized caller the feature exists and is disabled.
func TestUpdateIndex_AuthzSpeaksBeforeTheFlag(t *testing.T) {
	for _, flagEnabled := range []bool{false, true} {
		name := "flag off"
		if flagEnabled {
			name = "flag on"
		}
		t.Run(name, func(t *testing.T) {
			resp := indexesHandlersWithFlag(flagEnabled, &forbiddenAuthorizer{}).
				updateIndex(updateIndexParams(enableSearchableBody()), nil)

			rec := httptest.NewRecorder()
			resp.WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, 403, rec.Code,
				"an ungranted caller must get 403 regardless of the flag")
			require.NotContains(t, rec.Body.String(), "RUNTIME_REINDEX_ENABLED",
				"the 403 must not leak feature state")
		})
	}
}

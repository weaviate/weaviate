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

// confirmingCleanupProber answers every owner "my gate is closed" on the first
// ask, so the cancel confirms without spending its polling budget. What the
// owners answer is not what these tests are about.
type confirmingCleanupProber struct{}

func (confirmingCleanupProber) CleanupInProgress(context.Context, string, string) (bool, error) {
	return true, nil
}

// Pins: the kill switch must not block cancel of an already-running
// migration, or an operator loses the only way to stop it.
func TestUpdateIndex_RuntimeReindexDisabledStillCancels(t *testing.T) {
	const (
		collection = "Movies"
		property   = "title"
	)

	cancelBody := func(indexType string) *models.IndexUpdateRequest {
		switch indexType {
		case "searchable":
			return &models.IndexUpdateRequest{Searchable: &models.IndexUpdateSearchable{Cancel: true}}
		case "rangeable":
			return &models.IndexUpdateRequest{Rangeable: &models.IndexUpdateRangeable{Cancel: true}}
		default:
			return &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Cancel: true}}
		}
	}

	tests := []struct {
		name string
		body *models.IndexUpdateRequest
		// wantCancelled is the live repair-filterable task the fixture holds;
		// only the matching index type can stop it.
		wantCancelled bool
		wantStatus    string
	}{
		{
			name:          "filterable cancel reaches the task",
			body:          cancelBody("filterable"),
			wantCancelled: true,
			wantStatus:    "CANCELLED",
		},
		{
			name:       "searchable cancel reaches the handler and finds nothing to stop",
			body:       cancelBody("searchable"),
			wantStatus: "NO_OP",
		},
		{
			name:       "rangeable cancel reaches the handler and finds nothing to stop",
			body:       cancelBody("rangeable"),
			wantStatus: "NO_OP",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, svc := cancelFixture(t, confirmingCleanupProber{})
			h.appState.ServerConfig.Config.RuntimeReindexEnabled = false

			responder := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
				HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
				ClassName:    collection,
				PropertyName: property,
				Body:         tc.body,
			}, &models.Principal{Username: "u1"})

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "a cancel must not be refused by the kill switch, got %T", responder)
			require.Equal(t, tc.wantStatus, accepted.Payload.Status)
			if tc.wantCancelled {
				require.Len(t, svc.cancelled, 1, "the live task must actually be cancelled")
				require.Empty(t, svc.startedTasks(),
					"the caller was told the migration stopped; none may remain STARTED")
			} else {
				require.Empty(t, svc.cancelled, "no task of this index type was running")
				require.Len(t, svc.startedTasks(), 1,
					"a cancel aimed at another index type must leave the running task alone")
			}
			require.Zero(t, svc.adds,
				"the carve-out exempts cancel from the refusal, not from the ban on new tasks")
		})
	}
}

// The carve-out is narrow: it lets the cancel through, it does not switch the
// feature back on for the duration. The gates the flag disables read node-local
// holds, and a cancel is exactly what closes those holds — so a flag-off cancel
// is the one moment a half-disabled gate would start refusing backups on a node
// whose operator has turned the whole feature off.
func TestUpdateIndex_RuntimeReindexDisabledCancelStartsNoMigration(t *testing.T) {
	h, svc := cancelFixture(t, confirmingCleanupProber{})
	h.appState.ServerConfig.Config.RuntimeReindexEnabled = false

	responder := h.updateIndex(schema.SchemaObjectsIndexesUpdateParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    "Movies",
		PropertyName: "title",
		Body:         &models.IndexUpdateRequest{Filterable: &models.IndexUpdateFilterable{Cancel: true}},
	}, &models.Principal{Username: "u1"})

	_, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "expected the cancel to be accepted, got %T", responder)
	require.Len(t, svc.cancelled, 1)

	require.False(t, h.appState.ReindexProvider.Load().AnyCleanupInProgress(),
		"the cancel must hand its cleanup gate back before answering, so no later backup is refused by it")
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

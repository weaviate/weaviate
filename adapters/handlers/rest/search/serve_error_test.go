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

package search

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	openapierrors "github.com/go-openapi/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestServeErrorReshapesBindErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/search/Movie/near-text", nil)
	rec := httptest.NewRecorder()

	ServeError(rec, req, openapierrors.New(http.StatusUnprocessableEntity, "query in body is required"))

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	var payload models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Error, 1)
	assert.Equal(t, "query in body is required", payload.Error[0].Message)
}

func TestServeErrorKeepsStatusAndHeaders(t *testing.T) {
	// 405 must keep the Allow header the default renderer computes
	req := httptest.NewRequest(http.MethodDelete, "/v1/search/Movie/near-text", nil)
	rec := httptest.NewRecorder()

	ServeError(rec, req, openapierrors.MethodNotAllowed(http.MethodDelete, []string{http.MethodPost}))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, http.MethodPost, rec.Header().Get("Allow"))
	var payload models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Error, 1)
}

func TestServeErrorSingleContentType(t *testing.T) {
	// the writer go-swagger hands us has usually already set a Content-Type
	// (here a stale/wrong one); the reshaped error must end with exactly one
	// Content-Type header, application/json, not a duplicate
	req := httptest.NewRequest(http.MethodDelete, "/v1/search/Movie/near-text", nil)
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "text/event-stream")

	ServeError(rec, req, openapierrors.MethodNotAllowed(http.MethodDelete, []string{http.MethodPost}))

	got := rec.Result().Header["Content-Type"]
	require.Len(t, got, 1, "exactly one Content-Type header")
	assert.Equal(t, "application/json", got[0])
}

// TestServeErrorRewritesBindMessages: bind errors name request fields
// instead of Go types, and a body cut off by the cap is a 413.
func TestServeErrorRewritesBindMessages(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		want       string
	}{
		{
			name:       "type mismatch names the field",
			err:        openapierrors.NewParseError("body", "body", "", errors.New(`json: cannot unmarshal string into Go struct field SearchCommon.tenant of type string`)),
			wantStatus: http.StatusBadRequest,
			want:       `invalid request body: field "tenant" must be string, got string`,
		},
		{
			name:       "nested field type mismatch",
			err:        openapierrors.NewParseError("body", "body", "", errors.New(`json: cannot unmarshal string into Go struct field .alpha of type float64`)),
			wantStatus: http.StatusBadRequest,
			want:       `invalid request body: field "alpha" must be float64, got string`,
		},
		{
			name:       "non-object body",
			err:        openapierrors.NewParseError("body", "body", "", errors.New(`json: cannot unmarshal array into Go value of type models.SearchBm25Request`)),
			wantStatus: http.StatusBadRequest,
			want:       "invalid request body: the body must be a JSON object, got array",
		},
		{
			name:       "oversize body is a 413",
			err:        openapierrors.NewParseError("body", "body", "", errors.New("http: request body too large")),
			wantStatus: http.StatusRequestEntityTooLarge,
			want:       fmt.Sprintf("request body exceeds the %d byte limit", MaxBodyBytes),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/search/Movie/bm25", nil)
			rec := httptest.NewRecorder()

			ServeError(rec, req, tt.err)

			assert.Equal(t, tt.wantStatus, rec.Code)
			var payload models.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
			require.Len(t, payload.Error, 1)
			assert.Equal(t, tt.want, payload.Error[0].Message)
		})
	}
}

func TestServeErrorCompositeValidation(t *testing.T) {
	// several bind failures arrive as a composite error; the message must
	// survive the reshaping
	req := httptest.NewRequest(http.MethodPost, "/v1/search/Movie/near-text", nil)
	rec := httptest.NewRecorder()

	composite := openapierrors.CompositeValidationError(
		openapierrors.New(http.StatusUnprocessableEntity, "query in body is required"),
		openapierrors.New(http.StatusUnprocessableEntity, "consistencyLevel in body should be one of [ONE QUORUM ALL]"),
	)
	ServeError(rec, req, composite)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	var payload models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Error, 1)
	assert.Contains(t, payload.Error[0].Message, "query in body is required")
}

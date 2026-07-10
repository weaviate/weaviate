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

func TestServeErrorCompositeValidation(t *testing.T) {
	// several bind failures arrive as a composite error; the message must
	// survive the reshaping
	req := httptest.NewRequest(http.MethodPost, "/v1/search/Movie/near-text", nil)
	rec := httptest.NewRecorder()

	composite := openapierrors.CompositeValidationError(
		openapierrors.New(http.StatusUnprocessableEntity, "query in body is required"),
		openapierrors.New(http.StatusUnprocessableEntity, "consistency_level in body should be one of [ONE QUORUM ALL]"),
	)
	ServeError(rec, req, composite)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	var payload models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Error, 1)
	assert.Contains(t, payload.Error[0].Message, "query in body is required")
}

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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestTooManyRequestsResponderWrites429 pins that the query-admission shed
// surfaces as an HTTP 429 with the standard error payload on the REST path.
func TestTooManyRequestsResponderWrites429(t *testing.T) {
	rec := httptest.NewRecorder()

	tooManyRequestsResponder(fmt.Errorf("query admission: node overloaded, request shed (429)")).
		WriteResponse(rec, runtime.JSONProducer())

	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	var body models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Error, 1)
	require.Contains(t, body.Error[0].Message, "429")
}

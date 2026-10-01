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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	uco "github.com/weaviate/weaviate/usecases/objects"

	"github.com/weaviate/weaviate/entities/models"
)

// TestTooManyRequestsResponderWrites429 pins that the query-admission shed
// surfaces as an HTTP 429 with the standard error payload on the REST path.
func TestTooManyRequestsResponderWrites429(t *testing.T) {
	rec := httptest.NewRecorder()

	tooManyRequestsResponder(nil, fmt.Errorf("query admission: node overloaded, request shed (429)")).
		WriteResponse(rec, runtime.JSONProducer())

	require.Equal(t, http.StatusTooManyRequests, rec.Code)

	var body models.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Error, 1)
	require.Contains(t, body.Error[0].Message, "429")
}

// A node still replaying the log is unavailable, not broken: the client and the replica path both
// need to see that rather than a 500.
func TestNotCaughtUpResponder(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{
			name:     "the schema wait timing out",
			err:      fmt.Errorf("repo.object: %w: version got=23  want=55", clusterTypes.ErrDeadlineExceeded),
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:     "wrapped through an objects error",
			err:      &uco.Error{Msg: "repo.object", Code: uco.StatusServiceUnavailable, Err: clusterTypes.ErrDeadlineExceeded},
			wantCode: http.StatusServiceUnavailable,
		},
		{name: "anything else falls through", err: errors.New("boom")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := notCaughtUpResponder(nil, test.err)
			if test.wantCode == 0 {
				assert.Nil(t, res, "an unrelated error must not be reported as unavailable")
				return
			}
			require.NotNil(t, res)

			rec := httptest.NewRecorder()
			res.WriteResponse(rec, runtime.JSONProducer())
			assert.Equal(t, test.wantCode, rec.Code)
		})
	}
}

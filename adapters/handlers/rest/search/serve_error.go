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
	"bytes"
	"encoding/json"
	"net/http"

	openapierrors "github.com/go-openapi/errors"

	"github.com/weaviate/weaviate/entities/models"
)

// ServeError renders go-swagger-layer errors (bind validation, security,
// routing) for the search routes in the standard ErrorResponse shape, so
// every search error body is {"error":[{"message":...}]}. Status codes and
// headers (e.g. Allow on 405) stay exactly as the default go-openapi
// renderer computes them: the default runs into a buffer and only a
// {"code","message"} body is re-shaped.
func ServeError(rw http.ResponseWriter, r *http.Request, err error) {
	rec := &responseRecorder{header: http.Header{}}
	openapierrors.ServeError(rec, r, err)

	var apiErr struct {
		Message string `json:"message"`
	}
	body := rec.body.Bytes()
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Message != "" {
		reshaped, marshalErr := json.Marshal(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: apiErr.Message}},
		})
		if marshalErr == nil {
			body = reshaped
		}
	}

	for key, values := range rec.header {
		if key == "Content-Length" {
			continue
		}
		for _, value := range values {
			rw.Header().Add(key, value)
		}
	}
	rw.WriteHeader(rec.status)
	rw.Write(body)
}

// responseRecorder captures the default error renderer's output.
type responseRecorder struct {
	status int
	header http.Header
	body   bytes.Buffer
}

func (r *responseRecorder) Header() http.Header {
	return r.header
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	return r.body.Write(p)
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
}

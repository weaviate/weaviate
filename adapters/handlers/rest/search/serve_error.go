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
	"fmt"
	"net/http"
	"regexp"
	"strings"

	openapierrors "github.com/go-openapi/errors"

	"github.com/weaviate/weaviate/entities/models"
)

// ServeError renders go-swagger-layer errors (bind validation, security,
// routing) on search routes in the standard ErrorResponse body. The default
// renderer runs into a buffer, so statuses and headers stay unchanged.
func ServeError(rw http.ResponseWriter, r *http.Request, err error) {
	rec := &responseRecorder{header: http.Header{}}
	openapierrors.ServeError(rec, r, err)

	var apiErr struct {
		Message string `json:"message"`
	}
	body := rec.body.Bytes()
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Message != "" {
		message := rewriteBindMessage(apiErr.Message)
		if strings.Contains(apiErr.Message, errBodyTooLarge) {
			rec.status = http.StatusRequestEntityTooLarge
			message = fmt.Sprintf("request body exceeds the %d byte limit", MaxBodyBytes)
		}
		reshaped, marshalErr := json.Marshal(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: message}},
		})
		if marshalErr == nil {
			body = reshaped
		}
	}

	for key, values := range rec.header {
		// skip both: Content-Length is recomputed, and Content-Type is forced
		// to application/json below (the writer may already hold a stale one)
		if key == "Content-Length" || key == "Content-Type" {
			continue
		}
		for _, value := range values {
			rw.Header().Add(key, value)
		}
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(rec.status)
	rw.Write(body)
}

// MaxBodyBytes caps a search or aggregate request body; a where filter
// with thousands of values fits comfortably, an unbounded body does not.
const MaxBodyBytes = 4 << 20

// errBodyTooLarge is the text of net/http's MaxBytesError.
const errBodyTooLarge = "http: request body too large"

var (
	// swagger: parsing body body from "" failed, because <reason>
	bindPrefix = regexp.MustCompile(`^parsing body body from "" failed, because `)
	// encoding/json: cannot unmarshal string into Go struct field SearchCommon.tenant of type string
	bindTypeMismatch = regexp.MustCompile(`json: cannot unmarshal (\S+) into Go struct field (?:[\w.]*\.)?(\w+) of type ([\w.\[\]]+)`)
	// encoding/json: cannot unmarshal array into Go value of type models.SearchBm25Request
	bindBodyMismatch = regexp.MustCompile(`json: cannot unmarshal (\S+) into Go value of type [\w.]+`)
)

// rewriteBindMessage turns the JSON decoder's messages into ones that name
// request fields instead of Go types.
func rewriteBindMessage(msg string) string {
	msg = bindPrefix.ReplaceAllString(msg, "invalid request body: ")
	msg = bindTypeMismatch.ReplaceAllString(msg, `field "$2" must be $3, got $1`)
	msg = bindBodyMismatch.ReplaceAllString(msg, "the body must be a JSON object, got $1")
	return msg
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

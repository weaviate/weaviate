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
	"net/http"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"

	"github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(messages ...string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// appends all error messages to the error
	for _, message := range messages {
		er.Error = append(er.Error, &models.ErrorResponseErrorItems0{
			Message: message,
		})
	}

	return er
}

func errPayloadFromSingleErr(principal *models.Principal, err error) *models.ErrorResponse {
	return cerrors.ErrPayloadFromSingleErr(principal, err)
}

// withHTTPStatus sets code on every error item to the HTTP status the
// handler is about to write. That status wins over a code inferred from
// the error type, so the body and the status line agree.
func withHTTPStatus(resp *models.ErrorResponse, status int) *models.ErrorResponse {
	if resp == nil || status == 0 {
		return resp
	}
	for _, item := range resp.Error {
		if item != nil {
			item.Code = int64(status)
		}
	}
	return resp
}

// tooManyRequestsResponder writes an HTTP 429 with the standard error
// payload; the generated go-swagger operations declare no 429 response.
func tooManyRequestsResponder(principal *models.Principal, err error) middleware.Responder {
	return middleware.ResponderFunc(func(rw http.ResponseWriter, producer runtime.Producer) {
		rw.WriteHeader(http.StatusTooManyRequests)
		payload := withHTTPStatus(errPayloadFromSingleErr(principal, err), http.StatusTooManyRequests)
		if perr := producer.Produce(rw, payload); perr != nil {
			panic(perr) // let the recovery middleware deal with this
		}
	})
}

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

	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// tooManyRequestsResponder writes an HTTP 429 with the standard error
// payload; the generated go-swagger operations declare no 429 response.
func tooManyRequestsResponder(principal *models.Principal, err error) middleware.Responder {
	return middleware.ResponderFunc(func(rw http.ResponseWriter, producer runtime.Producer) {
		rw.WriteHeader(http.StatusTooManyRequests)
		if perr := producer.Produce(rw, errPayloadFromSingleErr(principal, err)); perr != nil {
			panic(perr) // let the recovery middleware deal with this
		}
	})
}

// memoryShedResponder renders a memwatch rejection as HTTP 429, or returns nil to fall through
func memoryShedResponder(principal *models.Principal, err error) middleware.Responder {
	if !enterrors.IsMemoryPressure(err) {
		return nil
	}
	return tooManyRequestsResponder(principal, err)
}

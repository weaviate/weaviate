//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package batch

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// BatchReferencesCreateOKCode is the HTTP code returned for type BatchReferencesCreateOK
const BatchReferencesCreateOKCode int = 200

/*
BatchReferencesCreateOK Request Successful. Warning: A successful request does not guarantuee that every batched reference was successfully created. Inspect the response body to see which references succeeded and which failed.

swagger:response batchReferencesCreateOK
*/
type BatchReferencesCreateOK struct {
	/*
	  In: Body
	*/
	Payload []*models.BatchReferenceResponse `json:"body,omitempty"`
}

// NewBatchReferencesCreateOK creates BatchReferencesCreateOK with default headers values
func NewBatchReferencesCreateOK() *BatchReferencesCreateOK {
	return &BatchReferencesCreateOK{}
}

// WithPayload adds the payload to the batch references create o k response
func (o *BatchReferencesCreateOK) WithPayload(payload []*models.BatchReferenceResponse) *BatchReferencesCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the batch references create o k response
func (o *BatchReferencesCreateOK) SetPayload(payload []*models.BatchReferenceResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BatchReferencesCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.WriteHeader(200)
	payload := o.Payload
	if payload == nil {
		// return empty array
		payload = make([]*models.BatchReferenceResponse, 0, 50)
	}

	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

// BatchReferencesCreateUnauthorizedCode is the HTTP code returned for type BatchReferencesCreateUnauthorized
const BatchReferencesCreateUnauthorizedCode int = 401

/*
BatchReferencesCreateUnauthorized Unauthorized or invalid credentials.

swagger:response batchReferencesCreateUnauthorized
*/
type BatchReferencesCreateUnauthorized struct{}

// NewBatchReferencesCreateUnauthorized creates BatchReferencesCreateUnauthorized with default headers values
func NewBatchReferencesCreateUnauthorized() *BatchReferencesCreateUnauthorized {
	return &BatchReferencesCreateUnauthorized{}
}

// WriteResponse to the client
func (o *BatchReferencesCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.Header().Del(runtime.HeaderContentType) // Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// BatchReferencesCreateForbiddenCode is the HTTP code returned for type BatchReferencesCreateForbidden
const BatchReferencesCreateForbiddenCode int = 403

/*
BatchReferencesCreateForbidden Forbidden

swagger:response batchReferencesCreateForbidden
*/
type BatchReferencesCreateForbidden struct {
	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBatchReferencesCreateForbidden creates BatchReferencesCreateForbidden with default headers values
func NewBatchReferencesCreateForbidden() *BatchReferencesCreateForbidden {
	return &BatchReferencesCreateForbidden{}
}

// WithPayload adds the payload to the batch references create forbidden response
func (o *BatchReferencesCreateForbidden) WithPayload(payload *models.ErrorResponse) *BatchReferencesCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the batch references create forbidden response
func (o *BatchReferencesCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BatchReferencesCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BatchReferencesCreateUnprocessableEntityCode is the HTTP code returned for type BatchReferencesCreateUnprocessableEntity
const BatchReferencesCreateUnprocessableEntityCode int = 422

/*
BatchReferencesCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response batchReferencesCreateUnprocessableEntity
*/
type BatchReferencesCreateUnprocessableEntity struct {
	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBatchReferencesCreateUnprocessableEntity creates BatchReferencesCreateUnprocessableEntity with default headers values
func NewBatchReferencesCreateUnprocessableEntity() *BatchReferencesCreateUnprocessableEntity {
	return &BatchReferencesCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the batch references create unprocessable entity response
func (o *BatchReferencesCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *BatchReferencesCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the batch references create unprocessable entity response
func (o *BatchReferencesCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BatchReferencesCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BatchReferencesCreateInternalServerErrorCode is the HTTP code returned for type BatchReferencesCreateInternalServerError
const BatchReferencesCreateInternalServerErrorCode int = 500

/*
BatchReferencesCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response batchReferencesCreateInternalServerError
*/
type BatchReferencesCreateInternalServerError struct {
	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBatchReferencesCreateInternalServerError creates BatchReferencesCreateInternalServerError with default headers values
func NewBatchReferencesCreateInternalServerError() *BatchReferencesCreateInternalServerError {
	return &BatchReferencesCreateInternalServerError{}
}

// WithPayload adds the payload to the batch references create internal server error response
func (o *BatchReferencesCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *BatchReferencesCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the batch references create internal server error response
func (o *BatchReferencesCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BatchReferencesCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

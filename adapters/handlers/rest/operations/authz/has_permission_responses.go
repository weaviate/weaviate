//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/liutizhong/weaviate/entities/models"
)

// HasPermissionOKCode is the HTTP code returned for type HasPermissionOK
const HasPermissionOKCode int = 200

/*
HasPermissionOK Permission check was successful

swagger:response hasPermissionOK
*/
type HasPermissionOK struct {

	/*
	  In: Body
	*/
	Payload bool `json:"body,omitempty"`
}

// NewHasPermissionOK creates HasPermissionOK with default headers values
func NewHasPermissionOK() *HasPermissionOK {

	return &HasPermissionOK{}
}

// WithPayload adds the payload to the has permission o k response
func (o *HasPermissionOK) WithPayload(payload bool) *HasPermissionOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the has permission o k response
func (o *HasPermissionOK) SetPayload(payload bool) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *HasPermissionOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

// HasPermissionBadRequestCode is the HTTP code returned for type HasPermissionBadRequest
const HasPermissionBadRequestCode int = 400

/*
HasPermissionBadRequest Malformed request.

swagger:response hasPermissionBadRequest
*/
type HasPermissionBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewHasPermissionBadRequest creates HasPermissionBadRequest with default headers values
func NewHasPermissionBadRequest() *HasPermissionBadRequest {

	return &HasPermissionBadRequest{}
}

// WithPayload adds the payload to the has permission bad request response
func (o *HasPermissionBadRequest) WithPayload(payload *models.ErrorResponse) *HasPermissionBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the has permission bad request response
func (o *HasPermissionBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *HasPermissionBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// HasPermissionUnauthorizedCode is the HTTP code returned for type HasPermissionUnauthorized
const HasPermissionUnauthorizedCode int = 401

/*
HasPermissionUnauthorized Unauthorized or invalid credentials.

swagger:response hasPermissionUnauthorized
*/
type HasPermissionUnauthorized struct {
}

// NewHasPermissionUnauthorized creates HasPermissionUnauthorized with default headers values
func NewHasPermissionUnauthorized() *HasPermissionUnauthorized {

	return &HasPermissionUnauthorized{}
}

// WriteResponse to the client
func (o *HasPermissionUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// HasPermissionForbiddenCode is the HTTP code returned for type HasPermissionForbidden
const HasPermissionForbiddenCode int = 403

/*
HasPermissionForbidden Forbidden

swagger:response hasPermissionForbidden
*/
type HasPermissionForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewHasPermissionForbidden creates HasPermissionForbidden with default headers values
func NewHasPermissionForbidden() *HasPermissionForbidden {

	return &HasPermissionForbidden{}
}

// WithPayload adds the payload to the has permission forbidden response
func (o *HasPermissionForbidden) WithPayload(payload *models.ErrorResponse) *HasPermissionForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the has permission forbidden response
func (o *HasPermissionForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *HasPermissionForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// HasPermissionUnprocessableEntityCode is the HTTP code returned for type HasPermissionUnprocessableEntity
const HasPermissionUnprocessableEntityCode int = 422

/*
HasPermissionUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response hasPermissionUnprocessableEntity
*/
type HasPermissionUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewHasPermissionUnprocessableEntity creates HasPermissionUnprocessableEntity with default headers values
func NewHasPermissionUnprocessableEntity() *HasPermissionUnprocessableEntity {

	return &HasPermissionUnprocessableEntity{}
}

// WithPayload adds the payload to the has permission unprocessable entity response
func (o *HasPermissionUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *HasPermissionUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the has permission unprocessable entity response
func (o *HasPermissionUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *HasPermissionUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// HasPermissionInternalServerErrorCode is the HTTP code returned for type HasPermissionInternalServerError
const HasPermissionInternalServerErrorCode int = 500

/*
HasPermissionInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response hasPermissionInternalServerError
*/
type HasPermissionInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewHasPermissionInternalServerError creates HasPermissionInternalServerError with default headers values
func NewHasPermissionInternalServerError() *HasPermissionInternalServerError {

	return &HasPermissionInternalServerError{}
}

// WithPayload adds the payload to the has permission internal server error response
func (o *HasPermissionInternalServerError) WithPayload(payload *models.ErrorResponse) *HasPermissionInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the has permission internal server error response
func (o *HasPermissionInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *HasPermissionInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

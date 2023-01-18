//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// SchemaObjectsUpdateOKCode is the HTTP code returned for type SchemaObjectsUpdateOK
const SchemaObjectsUpdateOKCode int = 200

/*
SchemaObjectsUpdateOK Class was updated successfully

swagger:response schemaObjectsUpdateOK
*/
type SchemaObjectsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Class `json:"body,omitempty"`
}

// NewSchemaObjectsUpdateOK creates SchemaObjectsUpdateOK with default headers values
func NewSchemaObjectsUpdateOK() *SchemaObjectsUpdateOK {

	return &SchemaObjectsUpdateOK{}
}

// WithPayload adds the payload to the schema objects update o k response
func (o *SchemaObjectsUpdateOK) WithPayload(payload *models.Class) *SchemaObjectsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects update o k response
func (o *SchemaObjectsUpdateOK) SetPayload(payload *models.Class) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsUpdateUnauthorizedCode is the HTTP code returned for type SchemaObjectsUpdateUnauthorized
const SchemaObjectsUpdateUnauthorizedCode int = 401

/*
SchemaObjectsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response schemaObjectsUpdateUnauthorized
*/
type SchemaObjectsUpdateUnauthorized struct {
}

// NewSchemaObjectsUpdateUnauthorized creates SchemaObjectsUpdateUnauthorized with default headers values
func NewSchemaObjectsUpdateUnauthorized() *SchemaObjectsUpdateUnauthorized {

	return &SchemaObjectsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// SchemaObjectsUpdateForbiddenCode is the HTTP code returned for type SchemaObjectsUpdateForbidden
const SchemaObjectsUpdateForbiddenCode int = 403

/*
SchemaObjectsUpdateForbidden Forbidden

swagger:response schemaObjectsUpdateForbidden
*/
type SchemaObjectsUpdateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsUpdateForbidden creates SchemaObjectsUpdateForbidden with default headers values
func NewSchemaObjectsUpdateForbidden() *SchemaObjectsUpdateForbidden {

	return &SchemaObjectsUpdateForbidden{}
}

// WithPayload adds the payload to the schema objects update forbidden response
func (o *SchemaObjectsUpdateForbidden) WithPayload(payload *models.ErrorResponse) *SchemaObjectsUpdateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects update forbidden response
func (o *SchemaObjectsUpdateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsUpdateNotFoundCode is the HTTP code returned for type SchemaObjectsUpdateNotFound
const SchemaObjectsUpdateNotFoundCode int = 404

/*
SchemaObjectsUpdateNotFound Class to be updated does not exist

swagger:response schemaObjectsUpdateNotFound
*/
type SchemaObjectsUpdateNotFound struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsUpdateNotFound creates SchemaObjectsUpdateNotFound with default headers values
func NewSchemaObjectsUpdateNotFound() *SchemaObjectsUpdateNotFound {

	return &SchemaObjectsUpdateNotFound{}
}

// WithPayload adds the payload to the schema objects update not found response
func (o *SchemaObjectsUpdateNotFound) WithPayload(payload *models.ErrorResponse) *SchemaObjectsUpdateNotFound {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects update not found response
func (o *SchemaObjectsUpdateNotFound) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsUpdateUnprocessableEntityCode is the HTTP code returned for type SchemaObjectsUpdateUnprocessableEntity
const SchemaObjectsUpdateUnprocessableEntityCode int = 422

/*
SchemaObjectsUpdateUnprocessableEntity Invalid update attempt

swagger:response schemaObjectsUpdateUnprocessableEntity
*/
type SchemaObjectsUpdateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsUpdateUnprocessableEntity creates SchemaObjectsUpdateUnprocessableEntity with default headers values
func NewSchemaObjectsUpdateUnprocessableEntity() *SchemaObjectsUpdateUnprocessableEntity {

	return &SchemaObjectsUpdateUnprocessableEntity{}
}

// WithPayload adds the payload to the schema objects update unprocessable entity response
func (o *SchemaObjectsUpdateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *SchemaObjectsUpdateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects update unprocessable entity response
func (o *SchemaObjectsUpdateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsUpdateInternalServerErrorCode is the HTTP code returned for type SchemaObjectsUpdateInternalServerError
const SchemaObjectsUpdateInternalServerErrorCode int = 500

/*
SchemaObjectsUpdateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response schemaObjectsUpdateInternalServerError
*/
type SchemaObjectsUpdateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsUpdateInternalServerError creates SchemaObjectsUpdateInternalServerError with default headers values
func NewSchemaObjectsUpdateInternalServerError() *SchemaObjectsUpdateInternalServerError {

	return &SchemaObjectsUpdateInternalServerError{}
}

// WithPayload adds the payload to the schema objects update internal server error response
func (o *SchemaObjectsUpdateInternalServerError) WithPayload(payload *models.ErrorResponse) *SchemaObjectsUpdateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects update internal server error response
func (o *SchemaObjectsUpdateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsUpdateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

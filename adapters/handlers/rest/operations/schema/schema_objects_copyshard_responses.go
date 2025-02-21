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

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// SchemaObjectsCopyshardOKCode is the HTTP code returned for type SchemaObjectsCopyshardOK
const SchemaObjectsCopyshardOKCode int = 200

/*
SchemaObjectsCopyshardOK Shard was copied successfully

swagger:response schemaObjectsCopyshardOK
*/
type SchemaObjectsCopyshardOK struct {
}

// NewSchemaObjectsCopyshardOK creates SchemaObjectsCopyshardOK with default headers values
func NewSchemaObjectsCopyshardOK() *SchemaObjectsCopyshardOK {

	return &SchemaObjectsCopyshardOK{}
}

// WriteResponse to the client
func (o *SchemaObjectsCopyshardOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// SchemaObjectsCopyshardUnauthorizedCode is the HTTP code returned for type SchemaObjectsCopyshardUnauthorized
const SchemaObjectsCopyshardUnauthorizedCode int = 401

/*
SchemaObjectsCopyshardUnauthorized Unauthorized or invalid credentials.

swagger:response schemaObjectsCopyshardUnauthorized
*/
type SchemaObjectsCopyshardUnauthorized struct {
}

// NewSchemaObjectsCopyshardUnauthorized creates SchemaObjectsCopyshardUnauthorized with default headers values
func NewSchemaObjectsCopyshardUnauthorized() *SchemaObjectsCopyshardUnauthorized {

	return &SchemaObjectsCopyshardUnauthorized{}
}

// WriteResponse to the client
func (o *SchemaObjectsCopyshardUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// SchemaObjectsCopyshardForbiddenCode is the HTTP code returned for type SchemaObjectsCopyshardForbidden
const SchemaObjectsCopyshardForbiddenCode int = 403

/*
SchemaObjectsCopyshardForbidden Forbidden

swagger:response schemaObjectsCopyshardForbidden
*/
type SchemaObjectsCopyshardForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsCopyshardForbidden creates SchemaObjectsCopyshardForbidden with default headers values
func NewSchemaObjectsCopyshardForbidden() *SchemaObjectsCopyshardForbidden {

	return &SchemaObjectsCopyshardForbidden{}
}

// WithPayload adds the payload to the schema objects copyshard forbidden response
func (o *SchemaObjectsCopyshardForbidden) WithPayload(payload *models.ErrorResponse) *SchemaObjectsCopyshardForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects copyshard forbidden response
func (o *SchemaObjectsCopyshardForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsCopyshardForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsCopyshardUnprocessableEntityCode is the HTTP code returned for type SchemaObjectsCopyshardUnprocessableEntity
const SchemaObjectsCopyshardUnprocessableEntityCode int = 422

/*
SchemaObjectsCopyshardUnprocessableEntity Invalid request

swagger:response schemaObjectsCopyshardUnprocessableEntity
*/
type SchemaObjectsCopyshardUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsCopyshardUnprocessableEntity creates SchemaObjectsCopyshardUnprocessableEntity with default headers values
func NewSchemaObjectsCopyshardUnprocessableEntity() *SchemaObjectsCopyshardUnprocessableEntity {

	return &SchemaObjectsCopyshardUnprocessableEntity{}
}

// WithPayload adds the payload to the schema objects copyshard unprocessable entity response
func (o *SchemaObjectsCopyshardUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *SchemaObjectsCopyshardUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects copyshard unprocessable entity response
func (o *SchemaObjectsCopyshardUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsCopyshardUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaObjectsCopyshardInternalServerErrorCode is the HTTP code returned for type SchemaObjectsCopyshardInternalServerError
const SchemaObjectsCopyshardInternalServerErrorCode int = 500

/*
SchemaObjectsCopyshardInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response schemaObjectsCopyshardInternalServerError
*/
type SchemaObjectsCopyshardInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaObjectsCopyshardInternalServerError creates SchemaObjectsCopyshardInternalServerError with default headers values
func NewSchemaObjectsCopyshardInternalServerError() *SchemaObjectsCopyshardInternalServerError {

	return &SchemaObjectsCopyshardInternalServerError{}
}

// WithPayload adds the payload to the schema objects copyshard internal server error response
func (o *SchemaObjectsCopyshardInternalServerError) WithPayload(payload *models.ErrorResponse) *SchemaObjectsCopyshardInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema objects copyshard internal server error response
func (o *SchemaObjectsCopyshardInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaObjectsCopyshardInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

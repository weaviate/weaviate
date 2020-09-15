//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// SchemaActionsCreateOKCode is the HTTP code returned for type SchemaActionsCreateOK
const SchemaActionsCreateOKCode int = 200

/*SchemaActionsCreateOK Added the new Action class to the schema.

swagger:response schemaActionsCreateOK
*/
type SchemaActionsCreateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Class `json:"body,omitempty"`
}

// NewSchemaActionsCreateOK creates SchemaActionsCreateOK with default headers values
func NewSchemaActionsCreateOK() *SchemaActionsCreateOK {

	return &SchemaActionsCreateOK{}
}

// WithPayload adds the payload to the schema actions create o k response
func (o *SchemaActionsCreateOK) WithPayload(payload *models.Class) *SchemaActionsCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema actions create o k response
func (o *SchemaActionsCreateOK) SetPayload(payload *models.Class) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaActionsCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaActionsCreateUnauthorizedCode is the HTTP code returned for type SchemaActionsCreateUnauthorized
const SchemaActionsCreateUnauthorizedCode int = 401

/*SchemaActionsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response schemaActionsCreateUnauthorized
*/
type SchemaActionsCreateUnauthorized struct {
}

// NewSchemaActionsCreateUnauthorized creates SchemaActionsCreateUnauthorized with default headers values
func NewSchemaActionsCreateUnauthorized() *SchemaActionsCreateUnauthorized {

	return &SchemaActionsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *SchemaActionsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// SchemaActionsCreateForbiddenCode is the HTTP code returned for type SchemaActionsCreateForbidden
const SchemaActionsCreateForbiddenCode int = 403

/*SchemaActionsCreateForbidden Forbidden

swagger:response schemaActionsCreateForbidden
*/
type SchemaActionsCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaActionsCreateForbidden creates SchemaActionsCreateForbidden with default headers values
func NewSchemaActionsCreateForbidden() *SchemaActionsCreateForbidden {

	return &SchemaActionsCreateForbidden{}
}

// WithPayload adds the payload to the schema actions create forbidden response
func (o *SchemaActionsCreateForbidden) WithPayload(payload *models.ErrorResponse) *SchemaActionsCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema actions create forbidden response
func (o *SchemaActionsCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaActionsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaActionsCreateUnprocessableEntityCode is the HTTP code returned for type SchemaActionsCreateUnprocessableEntity
const SchemaActionsCreateUnprocessableEntityCode int = 422

/*SchemaActionsCreateUnprocessableEntity Invalid Action class

swagger:response schemaActionsCreateUnprocessableEntity
*/
type SchemaActionsCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaActionsCreateUnprocessableEntity creates SchemaActionsCreateUnprocessableEntity with default headers values
func NewSchemaActionsCreateUnprocessableEntity() *SchemaActionsCreateUnprocessableEntity {

	return &SchemaActionsCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the schema actions create unprocessable entity response
func (o *SchemaActionsCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *SchemaActionsCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema actions create unprocessable entity response
func (o *SchemaActionsCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaActionsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// SchemaActionsCreateInternalServerErrorCode is the HTTP code returned for type SchemaActionsCreateInternalServerError
const SchemaActionsCreateInternalServerErrorCode int = 500

/*SchemaActionsCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response schemaActionsCreateInternalServerError
*/
type SchemaActionsCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewSchemaActionsCreateInternalServerError creates SchemaActionsCreateInternalServerError with default headers values
func NewSchemaActionsCreateInternalServerError() *SchemaActionsCreateInternalServerError {

	return &SchemaActionsCreateInternalServerError{}
}

// WithPayload adds the payload to the schema actions create internal server error response
func (o *SchemaActionsCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *SchemaActionsCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the schema actions create internal server error response
func (o *SchemaActionsCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *SchemaActionsCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

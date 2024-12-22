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

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/liutizhong/weaviate/entities/models"
)

// ObjectsReferencesUpdateOKCode is the HTTP code returned for type ObjectsReferencesUpdateOK
const ObjectsReferencesUpdateOKCode int = 200

/*
ObjectsReferencesUpdateOK Successfully replaced all the references.

swagger:response objectsReferencesUpdateOK
*/
type ObjectsReferencesUpdateOK struct {
}

// NewObjectsReferencesUpdateOK creates ObjectsReferencesUpdateOK with default headers values
func NewObjectsReferencesUpdateOK() *ObjectsReferencesUpdateOK {

	return &ObjectsReferencesUpdateOK{}
}

// WriteResponse to the client
func (o *ObjectsReferencesUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// ObjectsReferencesUpdateUnauthorizedCode is the HTTP code returned for type ObjectsReferencesUpdateUnauthorized
const ObjectsReferencesUpdateUnauthorizedCode int = 401

/*
ObjectsReferencesUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response objectsReferencesUpdateUnauthorized
*/
type ObjectsReferencesUpdateUnauthorized struct {
}

// NewObjectsReferencesUpdateUnauthorized creates ObjectsReferencesUpdateUnauthorized with default headers values
func NewObjectsReferencesUpdateUnauthorized() *ObjectsReferencesUpdateUnauthorized {

	return &ObjectsReferencesUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsReferencesUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsReferencesUpdateForbiddenCode is the HTTP code returned for type ObjectsReferencesUpdateForbidden
const ObjectsReferencesUpdateForbiddenCode int = 403

/*
ObjectsReferencesUpdateForbidden Forbidden

swagger:response objectsReferencesUpdateForbidden
*/
type ObjectsReferencesUpdateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesUpdateForbidden creates ObjectsReferencesUpdateForbidden with default headers values
func NewObjectsReferencesUpdateForbidden() *ObjectsReferencesUpdateForbidden {

	return &ObjectsReferencesUpdateForbidden{}
}

// WithPayload adds the payload to the objects references update forbidden response
func (o *ObjectsReferencesUpdateForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesUpdateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references update forbidden response
func (o *ObjectsReferencesUpdateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsReferencesUpdateUnprocessableEntityCode is the HTTP code returned for type ObjectsReferencesUpdateUnprocessableEntity
const ObjectsReferencesUpdateUnprocessableEntityCode int = 422

/*
ObjectsReferencesUpdateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?

swagger:response objectsReferencesUpdateUnprocessableEntity
*/
type ObjectsReferencesUpdateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesUpdateUnprocessableEntity creates ObjectsReferencesUpdateUnprocessableEntity with default headers values
func NewObjectsReferencesUpdateUnprocessableEntity() *ObjectsReferencesUpdateUnprocessableEntity {

	return &ObjectsReferencesUpdateUnprocessableEntity{}
}

// WithPayload adds the payload to the objects references update unprocessable entity response
func (o *ObjectsReferencesUpdateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesUpdateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references update unprocessable entity response
func (o *ObjectsReferencesUpdateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsReferencesUpdateInternalServerErrorCode is the HTTP code returned for type ObjectsReferencesUpdateInternalServerError
const ObjectsReferencesUpdateInternalServerErrorCode int = 500

/*
ObjectsReferencesUpdateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsReferencesUpdateInternalServerError
*/
type ObjectsReferencesUpdateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesUpdateInternalServerError creates ObjectsReferencesUpdateInternalServerError with default headers values
func NewObjectsReferencesUpdateInternalServerError() *ObjectsReferencesUpdateInternalServerError {

	return &ObjectsReferencesUpdateInternalServerError{}
}

// WithPayload adds the payload to the objects references update internal server error response
func (o *ObjectsReferencesUpdateInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesUpdateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references update internal server error response
func (o *ObjectsReferencesUpdateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesUpdateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

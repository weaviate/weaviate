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

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsClassReferencesCreateOKCode is the HTTP code returned for type ObjectsClassReferencesCreateOK
const ObjectsClassReferencesCreateOKCode int = 200

/*
ObjectsClassReferencesCreateOK Successfully added the reference.

swagger:response objectsClassReferencesCreateOK
*/
type ObjectsClassReferencesCreateOK struct {
}

// NewObjectsClassReferencesCreateOK creates ObjectsClassReferencesCreateOK with default headers values
func NewObjectsClassReferencesCreateOK() *ObjectsClassReferencesCreateOK {

	return &ObjectsClassReferencesCreateOK{}
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// ObjectsClassReferencesCreateUnauthorizedCode is the HTTP code returned for type ObjectsClassReferencesCreateUnauthorized
const ObjectsClassReferencesCreateUnauthorizedCode int = 401

/*
ObjectsClassReferencesCreateUnauthorized Unauthorized or invalid credentials.

swagger:response objectsClassReferencesCreateUnauthorized
*/
type ObjectsClassReferencesCreateUnauthorized struct {
}

// NewObjectsClassReferencesCreateUnauthorized creates ObjectsClassReferencesCreateUnauthorized with default headers values
func NewObjectsClassReferencesCreateUnauthorized() *ObjectsClassReferencesCreateUnauthorized {

	return &ObjectsClassReferencesCreateUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsClassReferencesCreateForbiddenCode is the HTTP code returned for type ObjectsClassReferencesCreateForbidden
const ObjectsClassReferencesCreateForbiddenCode int = 403

/*
ObjectsClassReferencesCreateForbidden Forbidden

swagger:response objectsClassReferencesCreateForbidden
*/
type ObjectsClassReferencesCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsClassReferencesCreateForbidden creates ObjectsClassReferencesCreateForbidden with default headers values
func NewObjectsClassReferencesCreateForbidden() *ObjectsClassReferencesCreateForbidden {

	return &ObjectsClassReferencesCreateForbidden{}
}

// WithPayload adds the payload to the objects class references create forbidden response
func (o *ObjectsClassReferencesCreateForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsClassReferencesCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects class references create forbidden response
func (o *ObjectsClassReferencesCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsClassReferencesCreateNotFoundCode is the HTTP code returned for type ObjectsClassReferencesCreateNotFound
const ObjectsClassReferencesCreateNotFoundCode int = 404

/*
ObjectsClassReferencesCreateNotFound Source object doesn't exist.

swagger:response objectsClassReferencesCreateNotFound
*/
type ObjectsClassReferencesCreateNotFound struct {
}

// NewObjectsClassReferencesCreateNotFound creates ObjectsClassReferencesCreateNotFound with default headers values
func NewObjectsClassReferencesCreateNotFound() *ObjectsClassReferencesCreateNotFound {

	return &ObjectsClassReferencesCreateNotFound{}
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ObjectsClassReferencesCreateUnprocessableEntityCode is the HTTP code returned for type ObjectsClassReferencesCreateUnprocessableEntity
const ObjectsClassReferencesCreateUnprocessableEntityCode int = 422

/*
ObjectsClassReferencesCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?

swagger:response objectsClassReferencesCreateUnprocessableEntity
*/
type ObjectsClassReferencesCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsClassReferencesCreateUnprocessableEntity creates ObjectsClassReferencesCreateUnprocessableEntity with default headers values
func NewObjectsClassReferencesCreateUnprocessableEntity() *ObjectsClassReferencesCreateUnprocessableEntity {

	return &ObjectsClassReferencesCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the objects class references create unprocessable entity response
func (o *ObjectsClassReferencesCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ObjectsClassReferencesCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects class references create unprocessable entity response
func (o *ObjectsClassReferencesCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsClassReferencesCreateInternalServerErrorCode is the HTTP code returned for type ObjectsClassReferencesCreateInternalServerError
const ObjectsClassReferencesCreateInternalServerErrorCode int = 500

/*
ObjectsClassReferencesCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsClassReferencesCreateInternalServerError
*/
type ObjectsClassReferencesCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsClassReferencesCreateInternalServerError creates ObjectsClassReferencesCreateInternalServerError with default headers values
func NewObjectsClassReferencesCreateInternalServerError() *ObjectsClassReferencesCreateInternalServerError {

	return &ObjectsClassReferencesCreateInternalServerError{}
}

// WithPayload adds the payload to the objects class references create internal server error response
func (o *ObjectsClassReferencesCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsClassReferencesCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects class references create internal server error response
func (o *ObjectsClassReferencesCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsClassReferencesCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

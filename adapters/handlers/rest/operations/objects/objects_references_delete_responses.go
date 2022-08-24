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

	"github.com/semi-technologies/weaviate/entities/models"
)

// ObjectsReferencesDeleteNoContentCode is the HTTP code returned for type ObjectsReferencesDeleteNoContent
const ObjectsReferencesDeleteNoContentCode int = 204

/*
ObjectsReferencesDeleteNoContent Successfully deleted.

swagger:response objectsReferencesDeleteNoContent
*/
type ObjectsReferencesDeleteNoContent struct {
}

// NewObjectsReferencesDeleteNoContent creates ObjectsReferencesDeleteNoContent with default headers values
func NewObjectsReferencesDeleteNoContent() *ObjectsReferencesDeleteNoContent {

	return &ObjectsReferencesDeleteNoContent{}
}

// WriteResponse to the client
func (o *ObjectsReferencesDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(204)
}

// ObjectsReferencesDeleteUnauthorizedCode is the HTTP code returned for type ObjectsReferencesDeleteUnauthorized
const ObjectsReferencesDeleteUnauthorizedCode int = 401

/*
ObjectsReferencesDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response objectsReferencesDeleteUnauthorized
*/
type ObjectsReferencesDeleteUnauthorized struct {
}

// NewObjectsReferencesDeleteUnauthorized creates ObjectsReferencesDeleteUnauthorized with default headers values
func NewObjectsReferencesDeleteUnauthorized() *ObjectsReferencesDeleteUnauthorized {

	return &ObjectsReferencesDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsReferencesDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsReferencesDeleteForbiddenCode is the HTTP code returned for type ObjectsReferencesDeleteForbidden
const ObjectsReferencesDeleteForbiddenCode int = 403

/*
ObjectsReferencesDeleteForbidden Forbidden

swagger:response objectsReferencesDeleteForbidden
*/
type ObjectsReferencesDeleteForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesDeleteForbidden creates ObjectsReferencesDeleteForbidden with default headers values
func NewObjectsReferencesDeleteForbidden() *ObjectsReferencesDeleteForbidden {

	return &ObjectsReferencesDeleteForbidden{}
}

// WithPayload adds the payload to the objects references delete forbidden response
func (o *ObjectsReferencesDeleteForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesDeleteForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references delete forbidden response
func (o *ObjectsReferencesDeleteForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsReferencesDeleteNotFoundCode is the HTTP code returned for type ObjectsReferencesDeleteNotFound
const ObjectsReferencesDeleteNotFoundCode int = 404

/*
ObjectsReferencesDeleteNotFound Successful query result but no resource was found.

swagger:response objectsReferencesDeleteNotFound
*/
type ObjectsReferencesDeleteNotFound struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesDeleteNotFound creates ObjectsReferencesDeleteNotFound with default headers values
func NewObjectsReferencesDeleteNotFound() *ObjectsReferencesDeleteNotFound {

	return &ObjectsReferencesDeleteNotFound{}
}

// WithPayload adds the payload to the objects references delete not found response
func (o *ObjectsReferencesDeleteNotFound) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesDeleteNotFound {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references delete not found response
func (o *ObjectsReferencesDeleteNotFound) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsReferencesDeleteInternalServerErrorCode is the HTTP code returned for type ObjectsReferencesDeleteInternalServerError
const ObjectsReferencesDeleteInternalServerErrorCode int = 500

/*
ObjectsReferencesDeleteInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsReferencesDeleteInternalServerError
*/
type ObjectsReferencesDeleteInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsReferencesDeleteInternalServerError creates ObjectsReferencesDeleteInternalServerError with default headers values
func NewObjectsReferencesDeleteInternalServerError() *ObjectsReferencesDeleteInternalServerError {

	return &ObjectsReferencesDeleteInternalServerError{}
}

// WithPayload adds the payload to the objects references delete internal server error response
func (o *ObjectsReferencesDeleteInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsReferencesDeleteInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects references delete internal server error response
func (o *ObjectsReferencesDeleteInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsReferencesDeleteInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

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

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsHeadNoContentCode is the HTTP code returned for type ObjectsHeadNoContent
const ObjectsHeadNoContentCode int = 204

/*
ObjectsHeadNoContent Object exists.

swagger:response objectsHeadNoContent
*/
type ObjectsHeadNoContent struct {
}

// NewObjectsHeadNoContent creates ObjectsHeadNoContent with default headers values
func NewObjectsHeadNoContent() *ObjectsHeadNoContent {

	return &ObjectsHeadNoContent{}
}

// WriteResponse to the client
func (o *ObjectsHeadNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(204)
}

// ObjectsHeadUnauthorizedCode is the HTTP code returned for type ObjectsHeadUnauthorized
const ObjectsHeadUnauthorizedCode int = 401

/*
ObjectsHeadUnauthorized Unauthorized or invalid credentials.

swagger:response objectsHeadUnauthorized
*/
type ObjectsHeadUnauthorized struct {
}

// NewObjectsHeadUnauthorized creates ObjectsHeadUnauthorized with default headers values
func NewObjectsHeadUnauthorized() *ObjectsHeadUnauthorized {

	return &ObjectsHeadUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsHeadUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsHeadForbiddenCode is the HTTP code returned for type ObjectsHeadForbidden
const ObjectsHeadForbiddenCode int = 403

/*
ObjectsHeadForbidden Forbidden

swagger:response objectsHeadForbidden
*/
type ObjectsHeadForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsHeadForbidden creates ObjectsHeadForbidden with default headers values
func NewObjectsHeadForbidden() *ObjectsHeadForbidden {

	return &ObjectsHeadForbidden{}
}

// WithPayload adds the payload to the objects head forbidden response
func (o *ObjectsHeadForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsHeadForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects head forbidden response
func (o *ObjectsHeadForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsHeadForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsHeadNotFoundCode is the HTTP code returned for type ObjectsHeadNotFound
const ObjectsHeadNotFoundCode int = 404

/*
ObjectsHeadNotFound Object doesn't exist.

swagger:response objectsHeadNotFound
*/
type ObjectsHeadNotFound struct {
}

// NewObjectsHeadNotFound creates ObjectsHeadNotFound with default headers values
func NewObjectsHeadNotFound() *ObjectsHeadNotFound {

	return &ObjectsHeadNotFound{}
}

// WriteResponse to the client
func (o *ObjectsHeadNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ObjectsHeadInternalServerErrorCode is the HTTP code returned for type ObjectsHeadInternalServerError
const ObjectsHeadInternalServerErrorCode int = 500

/*
ObjectsHeadInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsHeadInternalServerError
*/
type ObjectsHeadInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsHeadInternalServerError creates ObjectsHeadInternalServerError with default headers values
func NewObjectsHeadInternalServerError() *ObjectsHeadInternalServerError {

	return &ObjectsHeadInternalServerError{}
}

// WithPayload adds the payload to the objects head internal server error response
func (o *ObjectsHeadInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsHeadInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects head internal server error response
func (o *ObjectsHeadInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsHeadInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

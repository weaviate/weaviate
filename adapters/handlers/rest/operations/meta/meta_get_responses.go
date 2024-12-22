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

package meta

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/liutizhong/weaviate/entities/models"
)

// MetaGetOKCode is the HTTP code returned for type MetaGetOK
const MetaGetOKCode int = 200

/*
MetaGetOK Successful response.

swagger:response metaGetOK
*/
type MetaGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Meta `json:"body,omitempty"`
}

// NewMetaGetOK creates MetaGetOK with default headers values
func NewMetaGetOK() *MetaGetOK {

	return &MetaGetOK{}
}

// WithPayload adds the payload to the meta get o k response
func (o *MetaGetOK) WithPayload(payload *models.Meta) *MetaGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the meta get o k response
func (o *MetaGetOK) SetPayload(payload *models.Meta) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *MetaGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// MetaGetUnauthorizedCode is the HTTP code returned for type MetaGetUnauthorized
const MetaGetUnauthorizedCode int = 401

/*
MetaGetUnauthorized Unauthorized or invalid credentials.

swagger:response metaGetUnauthorized
*/
type MetaGetUnauthorized struct {
}

// NewMetaGetUnauthorized creates MetaGetUnauthorized with default headers values
func NewMetaGetUnauthorized() *MetaGetUnauthorized {

	return &MetaGetUnauthorized{}
}

// WriteResponse to the client
func (o *MetaGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// MetaGetForbiddenCode is the HTTP code returned for type MetaGetForbidden
const MetaGetForbiddenCode int = 403

/*
MetaGetForbidden Forbidden

swagger:response metaGetForbidden
*/
type MetaGetForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewMetaGetForbidden creates MetaGetForbidden with default headers values
func NewMetaGetForbidden() *MetaGetForbidden {

	return &MetaGetForbidden{}
}

// WithPayload adds the payload to the meta get forbidden response
func (o *MetaGetForbidden) WithPayload(payload *models.ErrorResponse) *MetaGetForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the meta get forbidden response
func (o *MetaGetForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *MetaGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// MetaGetInternalServerErrorCode is the HTTP code returned for type MetaGetInternalServerError
const MetaGetInternalServerErrorCode int = 500

/*
MetaGetInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response metaGetInternalServerError
*/
type MetaGetInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewMetaGetInternalServerError creates MetaGetInternalServerError with default headers values
func NewMetaGetInternalServerError() *MetaGetInternalServerError {

	return &MetaGetInternalServerError{}
}

// WithPayload adds the payload to the meta get internal server error response
func (o *MetaGetInternalServerError) WithPayload(payload *models.ErrorResponse) *MetaGetInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the meta get internal server error response
func (o *MetaGetInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *MetaGetInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

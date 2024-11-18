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

// TenantExistsOKCode is the HTTP code returned for type TenantExistsOK
const TenantExistsOKCode int = 200

/*
TenantExistsOK The tenant exists in the specified class

swagger:response tenantExistsOK
*/
type TenantExistsOK struct {
}

// NewTenantExistsOK creates TenantExistsOK with default headers values
func NewTenantExistsOK() *TenantExistsOK {

	return &TenantExistsOK{}
}

// WriteResponse to the client
func (o *TenantExistsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// TenantExistsUnauthorizedCode is the HTTP code returned for type TenantExistsUnauthorized
const TenantExistsUnauthorizedCode int = 401

/*
TenantExistsUnauthorized Unauthorized or invalid credentials.

swagger:response tenantExistsUnauthorized
*/
type TenantExistsUnauthorized struct {
}

// NewTenantExistsUnauthorized creates TenantExistsUnauthorized with default headers values
func NewTenantExistsUnauthorized() *TenantExistsUnauthorized {

	return &TenantExistsUnauthorized{}
}

// WriteResponse to the client
func (o *TenantExistsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// TenantExistsForbiddenCode is the HTTP code returned for type TenantExistsForbidden
const TenantExistsForbiddenCode int = 403

/*
TenantExistsForbidden Forbidden

swagger:response tenantExistsForbidden
*/
type TenantExistsForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewTenantExistsForbidden creates TenantExistsForbidden with default headers values
func NewTenantExistsForbidden() *TenantExistsForbidden {

	return &TenantExistsForbidden{}
}

// WithPayload adds the payload to the tenant exists forbidden response
func (o *TenantExistsForbidden) WithPayload(payload *models.ErrorResponse) *TenantExistsForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the tenant exists forbidden response
func (o *TenantExistsForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *TenantExistsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// TenantExistsNotFoundCode is the HTTP code returned for type TenantExistsNotFound
const TenantExistsNotFoundCode int = 404

/*
TenantExistsNotFound The tenant not found

swagger:response tenantExistsNotFound
*/
type TenantExistsNotFound struct {
}

// NewTenantExistsNotFound creates TenantExistsNotFound with default headers values
func NewTenantExistsNotFound() *TenantExistsNotFound {

	return &TenantExistsNotFound{}
}

// WriteResponse to the client
func (o *TenantExistsNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// TenantExistsUnprocessableEntityCode is the HTTP code returned for type TenantExistsUnprocessableEntity
const TenantExistsUnprocessableEntityCode int = 422

/*
TenantExistsUnprocessableEntity Invalid Tenant class

swagger:response tenantExistsUnprocessableEntity
*/
type TenantExistsUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewTenantExistsUnprocessableEntity creates TenantExistsUnprocessableEntity with default headers values
func NewTenantExistsUnprocessableEntity() *TenantExistsUnprocessableEntity {

	return &TenantExistsUnprocessableEntity{}
}

// WithPayload adds the payload to the tenant exists unprocessable entity response
func (o *TenantExistsUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *TenantExistsUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the tenant exists unprocessable entity response
func (o *TenantExistsUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *TenantExistsUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// TenantExistsInternalServerErrorCode is the HTTP code returned for type TenantExistsInternalServerError
const TenantExistsInternalServerErrorCode int = 500

/*
TenantExistsInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response tenantExistsInternalServerError
*/
type TenantExistsInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewTenantExistsInternalServerError creates TenantExistsInternalServerError with default headers values
func NewTenantExistsInternalServerError() *TenantExistsInternalServerError {

	return &TenantExistsInternalServerError{}
}

// WithPayload adds the payload to the tenant exists internal server error response
func (o *TenantExistsInternalServerError) WithPayload(payload *models.ErrorResponse) *TenantExistsInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the tenant exists internal server error response
func (o *TenantExistsInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *TenantExistsInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ThingsUpdateOKCode is the HTTP code returned for type ThingsUpdateOK
const ThingsUpdateOKCode int = 200

/*ThingsUpdateOK Successfully received.

swagger:response thingsUpdateOK
*/
type ThingsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Thing `json:"body,omitempty"`
}

// NewThingsUpdateOK creates ThingsUpdateOK with default headers values
func NewThingsUpdateOK() *ThingsUpdateOK {

	return &ThingsUpdateOK{}
}

// WithPayload adds the payload to the things update o k response
func (o *ThingsUpdateOK) WithPayload(payload *models.Thing) *ThingsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things update o k response
func (o *ThingsUpdateOK) SetPayload(payload *models.Thing) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsUpdateUnauthorizedCode is the HTTP code returned for type ThingsUpdateUnauthorized
const ThingsUpdateUnauthorizedCode int = 401

/*ThingsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response thingsUpdateUnauthorized
*/
type ThingsUpdateUnauthorized struct {
}

// NewThingsUpdateUnauthorized creates ThingsUpdateUnauthorized with default headers values
func NewThingsUpdateUnauthorized() *ThingsUpdateUnauthorized {

	return &ThingsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *ThingsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ThingsUpdateForbiddenCode is the HTTP code returned for type ThingsUpdateForbidden
const ThingsUpdateForbiddenCode int = 403

/*ThingsUpdateForbidden Forbidden

swagger:response thingsUpdateForbidden
*/
type ThingsUpdateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsUpdateForbidden creates ThingsUpdateForbidden with default headers values
func NewThingsUpdateForbidden() *ThingsUpdateForbidden {

	return &ThingsUpdateForbidden{}
}

// WithPayload adds the payload to the things update forbidden response
func (o *ThingsUpdateForbidden) WithPayload(payload *models.ErrorResponse) *ThingsUpdateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things update forbidden response
func (o *ThingsUpdateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsUpdateNotFoundCode is the HTTP code returned for type ThingsUpdateNotFound
const ThingsUpdateNotFoundCode int = 404

/*ThingsUpdateNotFound Successful query result but no resource was found.

swagger:response thingsUpdateNotFound
*/
type ThingsUpdateNotFound struct {
}

// NewThingsUpdateNotFound creates ThingsUpdateNotFound with default headers values
func NewThingsUpdateNotFound() *ThingsUpdateNotFound {

	return &ThingsUpdateNotFound{}
}

// WriteResponse to the client
func (o *ThingsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ThingsUpdateUnprocessableEntityCode is the HTTP code returned for type ThingsUpdateUnprocessableEntity
const ThingsUpdateUnprocessableEntityCode int = 422

/*ThingsUpdateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response thingsUpdateUnprocessableEntity
*/
type ThingsUpdateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsUpdateUnprocessableEntity creates ThingsUpdateUnprocessableEntity with default headers values
func NewThingsUpdateUnprocessableEntity() *ThingsUpdateUnprocessableEntity {

	return &ThingsUpdateUnprocessableEntity{}
}

// WithPayload adds the payload to the things update unprocessable entity response
func (o *ThingsUpdateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ThingsUpdateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things update unprocessable entity response
func (o *ThingsUpdateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsUpdateInternalServerErrorCode is the HTTP code returned for type ThingsUpdateInternalServerError
const ThingsUpdateInternalServerErrorCode int = 500

/*ThingsUpdateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response thingsUpdateInternalServerError
*/
type ThingsUpdateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsUpdateInternalServerError creates ThingsUpdateInternalServerError with default headers values
func NewThingsUpdateInternalServerError() *ThingsUpdateInternalServerError {

	return &ThingsUpdateInternalServerError{}
}

// WithPayload adds the payload to the things update internal server error response
func (o *ThingsUpdateInternalServerError) WithPayload(payload *models.ErrorResponse) *ThingsUpdateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things update internal server error response
func (o *ThingsUpdateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsUpdateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

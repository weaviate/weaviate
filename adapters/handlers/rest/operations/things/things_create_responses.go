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

	models "github.com/semi-technologies/weaviate/entities/models"
)

// ThingsCreateOKCode is the HTTP code returned for type ThingsCreateOK
const ThingsCreateOKCode int = 200

/*ThingsCreateOK Thing created.

swagger:response thingsCreateOK
*/
type ThingsCreateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Thing `json:"body,omitempty"`
}

// NewThingsCreateOK creates ThingsCreateOK with default headers values
func NewThingsCreateOK() *ThingsCreateOK {

	return &ThingsCreateOK{}
}

// WithPayload adds the payload to the things create o k response
func (o *ThingsCreateOK) WithPayload(payload *models.Thing) *ThingsCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things create o k response
func (o *ThingsCreateOK) SetPayload(payload *models.Thing) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsCreateUnauthorizedCode is the HTTP code returned for type ThingsCreateUnauthorized
const ThingsCreateUnauthorizedCode int = 401

/*ThingsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response thingsCreateUnauthorized
*/
type ThingsCreateUnauthorized struct {
}

// NewThingsCreateUnauthorized creates ThingsCreateUnauthorized with default headers values
func NewThingsCreateUnauthorized() *ThingsCreateUnauthorized {

	return &ThingsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *ThingsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ThingsCreateForbiddenCode is the HTTP code returned for type ThingsCreateForbidden
const ThingsCreateForbiddenCode int = 403

/*ThingsCreateForbidden Forbidden

swagger:response thingsCreateForbidden
*/
type ThingsCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsCreateForbidden creates ThingsCreateForbidden with default headers values
func NewThingsCreateForbidden() *ThingsCreateForbidden {

	return &ThingsCreateForbidden{}
}

// WithPayload adds the payload to the things create forbidden response
func (o *ThingsCreateForbidden) WithPayload(payload *models.ErrorResponse) *ThingsCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things create forbidden response
func (o *ThingsCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsCreateUnprocessableEntityCode is the HTTP code returned for type ThingsCreateUnprocessableEntity
const ThingsCreateUnprocessableEntityCode int = 422

/*ThingsCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response thingsCreateUnprocessableEntity
*/
type ThingsCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsCreateUnprocessableEntity creates ThingsCreateUnprocessableEntity with default headers values
func NewThingsCreateUnprocessableEntity() *ThingsCreateUnprocessableEntity {

	return &ThingsCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the things create unprocessable entity response
func (o *ThingsCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ThingsCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things create unprocessable entity response
func (o *ThingsCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ThingsCreateInternalServerErrorCode is the HTTP code returned for type ThingsCreateInternalServerError
const ThingsCreateInternalServerErrorCode int = 500

/*ThingsCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response thingsCreateInternalServerError
*/
type ThingsCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewThingsCreateInternalServerError creates ThingsCreateInternalServerError with default headers values
func NewThingsCreateInternalServerError() *ThingsCreateInternalServerError {

	return &ThingsCreateInternalServerError{}
}

// WithPayload adds the payload to the things create internal server error response
func (o *ThingsCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *ThingsCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the things create internal server error response
func (o *ThingsCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ThingsCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

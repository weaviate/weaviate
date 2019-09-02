//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// ActionsCreateOKCode is the HTTP code returned for type ActionsCreateOK
const ActionsCreateOKCode int = 200

/*ActionsCreateOK Action created.

swagger:response actionsCreateOK
*/
type ActionsCreateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Action `json:"body,omitempty"`
}

// NewActionsCreateOK creates ActionsCreateOK with default headers values
func NewActionsCreateOK() *ActionsCreateOK {

	return &ActionsCreateOK{}
}

// WithPayload adds the payload to the actions create o k response
func (o *ActionsCreateOK) WithPayload(payload *models.Action) *ActionsCreateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions create o k response
func (o *ActionsCreateOK) SetPayload(payload *models.Action) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsCreateUnauthorizedCode is the HTTP code returned for type ActionsCreateUnauthorized
const ActionsCreateUnauthorizedCode int = 401

/*ActionsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response actionsCreateUnauthorized
*/
type ActionsCreateUnauthorized struct {
}

// NewActionsCreateUnauthorized creates ActionsCreateUnauthorized with default headers values
func NewActionsCreateUnauthorized() *ActionsCreateUnauthorized {

	return &ActionsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *ActionsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ActionsCreateForbiddenCode is the HTTP code returned for type ActionsCreateForbidden
const ActionsCreateForbiddenCode int = 403

/*ActionsCreateForbidden Forbidden

swagger:response actionsCreateForbidden
*/
type ActionsCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsCreateForbidden creates ActionsCreateForbidden with default headers values
func NewActionsCreateForbidden() *ActionsCreateForbidden {

	return &ActionsCreateForbidden{}
}

// WithPayload adds the payload to the actions create forbidden response
func (o *ActionsCreateForbidden) WithPayload(payload *models.ErrorResponse) *ActionsCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions create forbidden response
func (o *ActionsCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsCreateUnprocessableEntityCode is the HTTP code returned for type ActionsCreateUnprocessableEntity
const ActionsCreateUnprocessableEntityCode int = 422

/*ActionsCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response actionsCreateUnprocessableEntity
*/
type ActionsCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsCreateUnprocessableEntity creates ActionsCreateUnprocessableEntity with default headers values
func NewActionsCreateUnprocessableEntity() *ActionsCreateUnprocessableEntity {

	return &ActionsCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the actions create unprocessable entity response
func (o *ActionsCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ActionsCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions create unprocessable entity response
func (o *ActionsCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsCreateInternalServerErrorCode is the HTTP code returned for type ActionsCreateInternalServerError
const ActionsCreateInternalServerErrorCode int = 500

/*ActionsCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response actionsCreateInternalServerError
*/
type ActionsCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsCreateInternalServerError creates ActionsCreateInternalServerError with default headers values
func NewActionsCreateInternalServerError() *ActionsCreateInternalServerError {

	return &ActionsCreateInternalServerError{}
}

// WithPayload adds the payload to the actions create internal server error response
func (o *ActionsCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *ActionsCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions create internal server error response
func (o *ActionsCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

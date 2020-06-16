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

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ActionsGetOKCode is the HTTP code returned for type ActionsGetOK
const ActionsGetOKCode int = 200

/*ActionsGetOK Successful response.

swagger:response actionsGetOK
*/
type ActionsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Action `json:"body,omitempty"`
}

// NewActionsGetOK creates ActionsGetOK with default headers values
func NewActionsGetOK() *ActionsGetOK {

	return &ActionsGetOK{}
}

// WithPayload adds the payload to the actions get o k response
func (o *ActionsGetOK) WithPayload(payload *models.Action) *ActionsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions get o k response
func (o *ActionsGetOK) SetPayload(payload *models.Action) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsGetBadRequestCode is the HTTP code returned for type ActionsGetBadRequest
const ActionsGetBadRequestCode int = 400

/*ActionsGetBadRequest Malformed request.

swagger:response actionsGetBadRequest
*/
type ActionsGetBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsGetBadRequest creates ActionsGetBadRequest with default headers values
func NewActionsGetBadRequest() *ActionsGetBadRequest {

	return &ActionsGetBadRequest{}
}

// WithPayload adds the payload to the actions get bad request response
func (o *ActionsGetBadRequest) WithPayload(payload *models.ErrorResponse) *ActionsGetBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions get bad request response
func (o *ActionsGetBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsGetBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsGetUnauthorizedCode is the HTTP code returned for type ActionsGetUnauthorized
const ActionsGetUnauthorizedCode int = 401

/*ActionsGetUnauthorized Unauthorized or invalid credentials.

swagger:response actionsGetUnauthorized
*/
type ActionsGetUnauthorized struct {
}

// NewActionsGetUnauthorized creates ActionsGetUnauthorized with default headers values
func NewActionsGetUnauthorized() *ActionsGetUnauthorized {

	return &ActionsGetUnauthorized{}
}

// WriteResponse to the client
func (o *ActionsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ActionsGetForbiddenCode is the HTTP code returned for type ActionsGetForbidden
const ActionsGetForbiddenCode int = 403

/*ActionsGetForbidden Forbidden

swagger:response actionsGetForbidden
*/
type ActionsGetForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsGetForbidden creates ActionsGetForbidden with default headers values
func NewActionsGetForbidden() *ActionsGetForbidden {

	return &ActionsGetForbidden{}
}

// WithPayload adds the payload to the actions get forbidden response
func (o *ActionsGetForbidden) WithPayload(payload *models.ErrorResponse) *ActionsGetForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions get forbidden response
func (o *ActionsGetForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsGetNotFoundCode is the HTTP code returned for type ActionsGetNotFound
const ActionsGetNotFoundCode int = 404

/*ActionsGetNotFound Successful query result but no resource was found.

swagger:response actionsGetNotFound
*/
type ActionsGetNotFound struct {
}

// NewActionsGetNotFound creates ActionsGetNotFound with default headers values
func NewActionsGetNotFound() *ActionsGetNotFound {

	return &ActionsGetNotFound{}
}

// WriteResponse to the client
func (o *ActionsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ActionsGetInternalServerErrorCode is the HTTP code returned for type ActionsGetInternalServerError
const ActionsGetInternalServerErrorCode int = 500

/*ActionsGetInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response actionsGetInternalServerError
*/
type ActionsGetInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsGetInternalServerError creates ActionsGetInternalServerError with default headers values
func NewActionsGetInternalServerError() *ActionsGetInternalServerError {

	return &ActionsGetInternalServerError{}
}

// WithPayload adds the payload to the actions get internal server error response
func (o *ActionsGetInternalServerError) WithPayload(payload *models.ErrorResponse) *ActionsGetInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions get internal server error response
func (o *ActionsGetInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsGetInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

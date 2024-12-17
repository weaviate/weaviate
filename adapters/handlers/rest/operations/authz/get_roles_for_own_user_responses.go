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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// GetRolesForOwnUserOKCode is the HTTP code returned for type GetRolesForOwnUserOK
const GetRolesForOwnUserOKCode int = 200

/*
GetRolesForOwnUserOK Role assigned to own users

swagger:response getRolesForOwnUserOK
*/
type GetRolesForOwnUserOK struct {

	/*
	  In: Body
	*/
	Payload models.RolesListResponse `json:"body,omitempty"`
}

// NewGetRolesForOwnUserOK creates GetRolesForOwnUserOK with default headers values
func NewGetRolesForOwnUserOK() *GetRolesForOwnUserOK {

	return &GetRolesForOwnUserOK{}
}

// WithPayload adds the payload to the get roles for own user o k response
func (o *GetRolesForOwnUserOK) WithPayload(payload models.RolesListResponse) *GetRolesForOwnUserOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get roles for own user o k response
func (o *GetRolesForOwnUserOK) SetPayload(payload models.RolesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRolesForOwnUserOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if payload == nil {
		// return empty array
		payload = models.RolesListResponse{}
	}

	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

// GetRolesForOwnUserUnauthorizedCode is the HTTP code returned for type GetRolesForOwnUserUnauthorized
const GetRolesForOwnUserUnauthorizedCode int = 401

/*
GetRolesForOwnUserUnauthorized Unauthorized or invalid credentials.

swagger:response getRolesForOwnUserUnauthorized
*/
type GetRolesForOwnUserUnauthorized struct {
}

// NewGetRolesForOwnUserUnauthorized creates GetRolesForOwnUserUnauthorized with default headers values
func NewGetRolesForOwnUserUnauthorized() *GetRolesForOwnUserUnauthorized {

	return &GetRolesForOwnUserUnauthorized{}
}

// WriteResponse to the client
func (o *GetRolesForOwnUserUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// GetRolesForOwnUserInternalServerErrorCode is the HTTP code returned for type GetRolesForOwnUserInternalServerError
const GetRolesForOwnUserInternalServerErrorCode int = 500

/*
GetRolesForOwnUserInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response getRolesForOwnUserInternalServerError
*/
type GetRolesForOwnUserInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetRolesForOwnUserInternalServerError creates GetRolesForOwnUserInternalServerError with default headers values
func NewGetRolesForOwnUserInternalServerError() *GetRolesForOwnUserInternalServerError {

	return &GetRolesForOwnUserInternalServerError{}
}

// WithPayload adds the payload to the get roles for own user internal server error response
func (o *GetRolesForOwnUserInternalServerError) WithPayload(payload *models.ErrorResponse) *GetRolesForOwnUserInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get roles for own user internal server error response
func (o *GetRolesForOwnUserInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRolesForOwnUserInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
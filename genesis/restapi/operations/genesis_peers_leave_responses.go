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

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// GenesisPeersLeaveNoContentCode is the HTTP code returned for type GenesisPeersLeaveNoContent
const GenesisPeersLeaveNoContentCode int = 204

/*
GenesisPeersLeaveNoContent Successful left the network.

swagger:response genesisPeersLeaveNoContent
*/
type GenesisPeersLeaveNoContent struct{}

// NewGenesisPeersLeaveNoContent creates GenesisPeersLeaveNoContent with default headers values
func NewGenesisPeersLeaveNoContent() *GenesisPeersLeaveNoContent {
	return &GenesisPeersLeaveNoContent{}
}

// WriteResponse to the client
func (o *GenesisPeersLeaveNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.Header().Del(runtime.HeaderContentType) // Remove Content-Type on empty responses

	rw.WriteHeader(204)
}

// GenesisPeersLeaveUnauthorizedCode is the HTTP code returned for type GenesisPeersLeaveUnauthorized
const GenesisPeersLeaveUnauthorizedCode int = 401

/*
GenesisPeersLeaveUnauthorized Unauthorized or invalid credentials.

swagger:response genesisPeersLeaveUnauthorized
*/
type GenesisPeersLeaveUnauthorized struct{}

// NewGenesisPeersLeaveUnauthorized creates GenesisPeersLeaveUnauthorized with default headers values
func NewGenesisPeersLeaveUnauthorized() *GenesisPeersLeaveUnauthorized {
	return &GenesisPeersLeaveUnauthorized{}
}

// WriteResponse to the client
func (o *GenesisPeersLeaveUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.Header().Del(runtime.HeaderContentType) // Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// GenesisPeersLeaveForbiddenCode is the HTTP code returned for type GenesisPeersLeaveForbidden
const GenesisPeersLeaveForbiddenCode int = 403

/*
GenesisPeersLeaveForbidden The used API-key has insufficient permissions.

swagger:response genesisPeersLeaveForbidden
*/
type GenesisPeersLeaveForbidden struct{}

// NewGenesisPeersLeaveForbidden creates GenesisPeersLeaveForbidden with default headers values
func NewGenesisPeersLeaveForbidden() *GenesisPeersLeaveForbidden {
	return &GenesisPeersLeaveForbidden{}
}

// WriteResponse to the client
func (o *GenesisPeersLeaveForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.Header().Del(runtime.HeaderContentType) // Remove Content-Type on empty responses

	rw.WriteHeader(403)
}

// GenesisPeersLeaveNotFoundCode is the HTTP code returned for type GenesisPeersLeaveNotFound
const GenesisPeersLeaveNotFoundCode int = 404

/*
GenesisPeersLeaveNotFound Successful query result but no such peer was found.

swagger:response genesisPeersLeaveNotFound
*/
type GenesisPeersLeaveNotFound struct{}

// NewGenesisPeersLeaveNotFound creates GenesisPeersLeaveNotFound with default headers values
func NewGenesisPeersLeaveNotFound() *GenesisPeersLeaveNotFound {
	return &GenesisPeersLeaveNotFound{}
}

// WriteResponse to the client
func (o *GenesisPeersLeaveNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {
	rw.Header().Del(runtime.HeaderContentType) // Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

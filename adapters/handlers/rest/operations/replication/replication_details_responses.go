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

package replication

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// ReplicationDetailsOKCode is the HTTP code returned for type ReplicationDetailsOK
const ReplicationDetailsOKCode int = 200

/*
ReplicationDetailsOK The details of the shard replica operation.

swagger:response replicationDetailsOK
*/
type ReplicationDetailsOK struct {

	/*
	  In: Body
	*/
	Payload *ReplicationDetailsOKBody `json:"body,omitempty"`
}

// NewReplicationDetailsOK creates ReplicationDetailsOK with default headers values
func NewReplicationDetailsOK() *ReplicationDetailsOK {

	return &ReplicationDetailsOK{}
}

// WithPayload adds the payload to the replication details o k response
func (o *ReplicationDetailsOK) WithPayload(payload *ReplicationDetailsOKBody) *ReplicationDetailsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replication details o k response
func (o *ReplicationDetailsOK) SetPayload(payload *ReplicationDetailsOKBody) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicationDetailsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicationDetailsBadRequestCode is the HTTP code returned for type ReplicationDetailsBadRequest
const ReplicationDetailsBadRequestCode int = 400

/*
ReplicationDetailsBadRequest Malformed request.

swagger:response replicationDetailsBadRequest
*/
type ReplicationDetailsBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicationDetailsBadRequest creates ReplicationDetailsBadRequest with default headers values
func NewReplicationDetailsBadRequest() *ReplicationDetailsBadRequest {

	return &ReplicationDetailsBadRequest{}
}

// WithPayload adds the payload to the replication details bad request response
func (o *ReplicationDetailsBadRequest) WithPayload(payload *models.ErrorResponse) *ReplicationDetailsBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replication details bad request response
func (o *ReplicationDetailsBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicationDetailsBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicationDetailsUnauthorizedCode is the HTTP code returned for type ReplicationDetailsUnauthorized
const ReplicationDetailsUnauthorizedCode int = 401

/*
ReplicationDetailsUnauthorized Unauthorized or invalid credentials.

swagger:response replicationDetailsUnauthorized
*/
type ReplicationDetailsUnauthorized struct {
}

// NewReplicationDetailsUnauthorized creates ReplicationDetailsUnauthorized with default headers values
func NewReplicationDetailsUnauthorized() *ReplicationDetailsUnauthorized {

	return &ReplicationDetailsUnauthorized{}
}

// WriteResponse to the client
func (o *ReplicationDetailsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ReplicationDetailsForbiddenCode is the HTTP code returned for type ReplicationDetailsForbidden
const ReplicationDetailsForbiddenCode int = 403

/*
ReplicationDetailsForbidden Forbidden

swagger:response replicationDetailsForbidden
*/
type ReplicationDetailsForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicationDetailsForbidden creates ReplicationDetailsForbidden with default headers values
func NewReplicationDetailsForbidden() *ReplicationDetailsForbidden {

	return &ReplicationDetailsForbidden{}
}

// WithPayload adds the payload to the replication details forbidden response
func (o *ReplicationDetailsForbidden) WithPayload(payload *models.ErrorResponse) *ReplicationDetailsForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replication details forbidden response
func (o *ReplicationDetailsForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicationDetailsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicationDetailsNotFoundCode is the HTTP code returned for type ReplicationDetailsNotFound
const ReplicationDetailsNotFoundCode int = 404

/*
ReplicationDetailsNotFound Shard replica move operation not found

swagger:response replicationDetailsNotFound
*/
type ReplicationDetailsNotFound struct {
}

// NewReplicationDetailsNotFound creates ReplicationDetailsNotFound with default headers values
func NewReplicationDetailsNotFound() *ReplicationDetailsNotFound {

	return &ReplicationDetailsNotFound{}
}

// WriteResponse to the client
func (o *ReplicationDetailsNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ReplicationDetailsInternalServerErrorCode is the HTTP code returned for type ReplicationDetailsInternalServerError
const ReplicationDetailsInternalServerErrorCode int = 500

/*
ReplicationDetailsInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response replicationDetailsInternalServerError
*/
type ReplicationDetailsInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicationDetailsInternalServerError creates ReplicationDetailsInternalServerError with default headers values
func NewReplicationDetailsInternalServerError() *ReplicationDetailsInternalServerError {

	return &ReplicationDetailsInternalServerError{}
}

// WithPayload adds the payload to the replication details internal server error response
func (o *ReplicationDetailsInternalServerError) WithPayload(payload *models.ErrorResponse) *ReplicationDetailsInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replication details internal server error response
func (o *ReplicationDetailsInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicationDetailsInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicationDetailsNotImplementedCode is the HTTP code returned for type ReplicationDetailsNotImplemented
const ReplicationDetailsNotImplementedCode int = 501

/*
ReplicationDetailsNotImplemented An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response replicationDetailsNotImplemented
*/
type ReplicationDetailsNotImplemented struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicationDetailsNotImplemented creates ReplicationDetailsNotImplemented with default headers values
func NewReplicationDetailsNotImplemented() *ReplicationDetailsNotImplemented {

	return &ReplicationDetailsNotImplemented{}
}

// WithPayload adds the payload to the replication details not implemented response
func (o *ReplicationDetailsNotImplemented) WithPayload(payload *models.ErrorResponse) *ReplicationDetailsNotImplemented {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replication details not implemented response
func (o *ReplicationDetailsNotImplemented) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicationDetailsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

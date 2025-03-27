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

// ReplicateStatusOKCode is the HTTP code returned for type ReplicateStatusOK
const ReplicateStatusOKCode int = 200

/*
ReplicateStatusOK The status of the shard replica move operation

swagger:response replicateStatusOK
*/
type ReplicateStatusOK struct {

	/*
	  In: Body
	*/
	Payload *ReplicateStatusOKBody `json:"body,omitempty"`
}

// NewReplicateStatusOK creates ReplicateStatusOK with default headers values
func NewReplicateStatusOK() *ReplicateStatusOK {

	return &ReplicateStatusOK{}
}

// WithPayload adds the payload to the replicate status o k response
func (o *ReplicateStatusOK) WithPayload(payload *ReplicateStatusOKBody) *ReplicateStatusOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replicate status o k response
func (o *ReplicateStatusOK) SetPayload(payload *ReplicateStatusOKBody) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicateStatusOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicateStatusBadRequestCode is the HTTP code returned for type ReplicateStatusBadRequest
const ReplicateStatusBadRequestCode int = 400

/*
ReplicateStatusBadRequest Malformed request.

swagger:response replicateStatusBadRequest
*/
type ReplicateStatusBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicateStatusBadRequest creates ReplicateStatusBadRequest with default headers values
func NewReplicateStatusBadRequest() *ReplicateStatusBadRequest {

	return &ReplicateStatusBadRequest{}
}

// WithPayload adds the payload to the replicate status bad request response
func (o *ReplicateStatusBadRequest) WithPayload(payload *models.ErrorResponse) *ReplicateStatusBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replicate status bad request response
func (o *ReplicateStatusBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicateStatusBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicateStatusNotFoundCode is the HTTP code returned for type ReplicateStatusNotFound
const ReplicateStatusNotFoundCode int = 404

/*
ReplicateStatusNotFound Shard replica move operation not found

swagger:response replicateStatusNotFound
*/
type ReplicateStatusNotFound struct {
}

// NewReplicateStatusNotFound creates ReplicateStatusNotFound with default headers values
func NewReplicateStatusNotFound() *ReplicateStatusNotFound {

	return &ReplicateStatusNotFound{}
}

// WriteResponse to the client
func (o *ReplicateStatusNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ReplicateStatusInternalServerErrorCode is the HTTP code returned for type ReplicateStatusInternalServerError
const ReplicateStatusInternalServerErrorCode int = 500

/*
ReplicateStatusInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response replicateStatusInternalServerError
*/
type ReplicateStatusInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicateStatusInternalServerError creates ReplicateStatusInternalServerError with default headers values
func NewReplicateStatusInternalServerError() *ReplicateStatusInternalServerError {

	return &ReplicateStatusInternalServerError{}
}

// WithPayload adds the payload to the replicate status internal server error response
func (o *ReplicateStatusInternalServerError) WithPayload(payload *models.ErrorResponse) *ReplicateStatusInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replicate status internal server error response
func (o *ReplicateStatusInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicateStatusInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ReplicateStatusNotImplementedCode is the HTTP code returned for type ReplicateStatusNotImplemented
const ReplicateStatusNotImplementedCode int = 501

/*
ReplicateStatusNotImplemented An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response replicateStatusNotImplemented
*/
type ReplicateStatusNotImplemented struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewReplicateStatusNotImplemented creates ReplicateStatusNotImplemented with default headers values
func NewReplicateStatusNotImplemented() *ReplicateStatusNotImplemented {

	return &ReplicateStatusNotImplemented{}
}

// WithPayload adds the payload to the replicate status not implemented response
func (o *ReplicateStatusNotImplemented) WithPayload(payload *models.ErrorResponse) *ReplicateStatusNotImplemented {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the replicate status not implemented response
func (o *ReplicateStatusNotImplemented) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ReplicateStatusNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

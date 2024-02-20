// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsGetOKCode is the HTTP code returned for type ObjectsGetOK
const ObjectsGetOKCode int = 200

/*
ObjectsGetOK Successful response.

swagger:response objectsGetOK
*/
type ObjectsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Object `json:"body,omitempty"`
}

// NewObjectsGetOK creates ObjectsGetOK with default headers values
func NewObjectsGetOK() *ObjectsGetOK {

	return &ObjectsGetOK{}
}

// WithPayload adds the payload to the objects get o k response
func (o *ObjectsGetOK) WithPayload(payload *models.Object) *ObjectsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects get o k response
func (o *ObjectsGetOK) SetPayload(payload *models.Object) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsGetBadRequestCode is the HTTP code returned for type ObjectsGetBadRequest
const ObjectsGetBadRequestCode int = 400

/*
ObjectsGetBadRequest Malformed request.

swagger:response objectsGetBadRequest
*/
type ObjectsGetBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsGetBadRequest creates ObjectsGetBadRequest with default headers values
func NewObjectsGetBadRequest() *ObjectsGetBadRequest {

	return &ObjectsGetBadRequest{}
}

// WithPayload adds the payload to the objects get bad request response
func (o *ObjectsGetBadRequest) WithPayload(payload *models.ErrorResponse) *ObjectsGetBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects get bad request response
func (o *ObjectsGetBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsGetBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsGetUnauthorizedCode is the HTTP code returned for type ObjectsGetUnauthorized
const ObjectsGetUnauthorizedCode int = 401

/*
ObjectsGetUnauthorized Unauthorized or invalid credentials.

swagger:response objectsGetUnauthorized
*/
type ObjectsGetUnauthorized struct {
}

// NewObjectsGetUnauthorized creates ObjectsGetUnauthorized with default headers values
func NewObjectsGetUnauthorized() *ObjectsGetUnauthorized {

	return &ObjectsGetUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsGetForbiddenCode is the HTTP code returned for type ObjectsGetForbidden
const ObjectsGetForbiddenCode int = 403

/*
ObjectsGetForbidden Forbidden

swagger:response objectsGetForbidden
*/
type ObjectsGetForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsGetForbidden creates ObjectsGetForbidden with default headers values
func NewObjectsGetForbidden() *ObjectsGetForbidden {

	return &ObjectsGetForbidden{}
}

// WithPayload adds the payload to the objects get forbidden response
func (o *ObjectsGetForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsGetForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects get forbidden response
func (o *ObjectsGetForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsGetNotFoundCode is the HTTP code returned for type ObjectsGetNotFound
const ObjectsGetNotFoundCode int = 404

/*
ObjectsGetNotFound Successful query result but no resource was found.

swagger:response objectsGetNotFound
*/
type ObjectsGetNotFound struct {
}

// NewObjectsGetNotFound creates ObjectsGetNotFound with default headers values
func NewObjectsGetNotFound() *ObjectsGetNotFound {

	return &ObjectsGetNotFound{}
}

// WriteResponse to the client
func (o *ObjectsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ObjectsGetInternalServerErrorCode is the HTTP code returned for type ObjectsGetInternalServerError
const ObjectsGetInternalServerErrorCode int = 500

/*
ObjectsGetInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsGetInternalServerError
*/
type ObjectsGetInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsGetInternalServerError creates ObjectsGetInternalServerError with default headers values
func NewObjectsGetInternalServerError() *ObjectsGetInternalServerError {

	return &ObjectsGetInternalServerError{}
}

// WithPayload adds the payload to the objects get internal server error response
func (o *ObjectsGetInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsGetInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects get internal server error response
func (o *ObjectsGetInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsGetInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

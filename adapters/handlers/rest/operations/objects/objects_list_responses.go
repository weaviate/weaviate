// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ObjectsListOKCode is the HTTP code returned for type ObjectsListOK
const ObjectsListOKCode int = 200

/*ObjectsListOK Successful response.

swagger:response objectsListOK
*/
type ObjectsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.ObjectsListResponse `json:"body,omitempty"`
}

// NewObjectsListOK creates ObjectsListOK with default headers values
func NewObjectsListOK() *ObjectsListOK {

	return &ObjectsListOK{}
}

// WithPayload adds the payload to the objects list o k response
func (o *ObjectsListOK) WithPayload(payload *models.ObjectsListResponse) *ObjectsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects list o k response
func (o *ObjectsListOK) SetPayload(payload *models.ObjectsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsListBadRequestCode is the HTTP code returned for type ObjectsListBadRequest
const ObjectsListBadRequestCode int = 400

/*ObjectsListBadRequest Malformed request.

swagger:response objectsListBadRequest
*/
type ObjectsListBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsListBadRequest creates ObjectsListBadRequest with default headers values
func NewObjectsListBadRequest() *ObjectsListBadRequest {

	return &ObjectsListBadRequest{}
}

// WithPayload adds the payload to the objects list bad request response
func (o *ObjectsListBadRequest) WithPayload(payload *models.ErrorResponse) *ObjectsListBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects list bad request response
func (o *ObjectsListBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsListBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsListUnauthorizedCode is the HTTP code returned for type ObjectsListUnauthorized
const ObjectsListUnauthorizedCode int = 401

/*ObjectsListUnauthorized Unauthorized or invalid credentials.

swagger:response objectsListUnauthorized
*/
type ObjectsListUnauthorized struct {
}

// NewObjectsListUnauthorized creates ObjectsListUnauthorized with default headers values
func NewObjectsListUnauthorized() *ObjectsListUnauthorized {

	return &ObjectsListUnauthorized{}
}

// WriteResponse to the client
func (o *ObjectsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ObjectsListForbiddenCode is the HTTP code returned for type ObjectsListForbidden
const ObjectsListForbiddenCode int = 403

/*ObjectsListForbidden Forbidden

swagger:response objectsListForbidden
*/
type ObjectsListForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsListForbidden creates ObjectsListForbidden with default headers values
func NewObjectsListForbidden() *ObjectsListForbidden {

	return &ObjectsListForbidden{}
}

// WithPayload adds the payload to the objects list forbidden response
func (o *ObjectsListForbidden) WithPayload(payload *models.ErrorResponse) *ObjectsListForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects list forbidden response
func (o *ObjectsListForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsListNotFoundCode is the HTTP code returned for type ObjectsListNotFound
const ObjectsListNotFoundCode int = 404

/*ObjectsListNotFound Successful query result but no resource was found.

swagger:response objectsListNotFound
*/
type ObjectsListNotFound struct {
}

// NewObjectsListNotFound creates ObjectsListNotFound with default headers values
func NewObjectsListNotFound() *ObjectsListNotFound {

	return &ObjectsListNotFound{}
}

// WriteResponse to the client
func (o *ObjectsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// ObjectsListUnprocessableEntityCode is the HTTP code returned for type ObjectsListUnprocessableEntity
const ObjectsListUnprocessableEntityCode int = 422

/*ObjectsListUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response objectsListUnprocessableEntity
*/
type ObjectsListUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsListUnprocessableEntity creates ObjectsListUnprocessableEntity with default headers values
func NewObjectsListUnprocessableEntity() *ObjectsListUnprocessableEntity {

	return &ObjectsListUnprocessableEntity{}
}

// WithPayload adds the payload to the objects list unprocessable entity response
func (o *ObjectsListUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ObjectsListUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects list unprocessable entity response
func (o *ObjectsListUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsListUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ObjectsListInternalServerErrorCode is the HTTP code returned for type ObjectsListInternalServerError
const ObjectsListInternalServerErrorCode int = 500

/*ObjectsListInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response objectsListInternalServerError
*/
type ObjectsListInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewObjectsListInternalServerError creates ObjectsListInternalServerError with default headers values
func NewObjectsListInternalServerError() *ObjectsListInternalServerError {

	return &ObjectsListInternalServerError{}
}

// WithPayload adds the payload to the objects list internal server error response
func (o *ObjectsListInternalServerError) WithPayload(payload *models.ErrorResponse) *ObjectsListInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the objects list internal server error response
func (o *ObjectsListInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ObjectsListInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

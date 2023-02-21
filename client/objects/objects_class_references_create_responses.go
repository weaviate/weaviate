//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsClassReferencesCreateReader is a Reader for the ObjectsClassReferencesCreate structure.
type ObjectsClassReferencesCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassReferencesCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassReferencesCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewObjectsClassReferencesCreateBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewObjectsClassReferencesCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassReferencesCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassReferencesCreateNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassReferencesCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassReferencesCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsClassReferencesCreateOK creates a ObjectsClassReferencesCreateOK with default headers values
func NewObjectsClassReferencesCreateOK() *ObjectsClassReferencesCreateOK {
	return &ObjectsClassReferencesCreateOK{}
}

/*
ObjectsClassReferencesCreateOK describes a response with status code 200, with default header values.

Successfully added the reference.
*/
type ObjectsClassReferencesCreateOK struct{}

// IsSuccess returns true when this objects class references create o k response has a 2xx status code
func (o *ObjectsClassReferencesCreateOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this objects class references create o k response has a 3xx status code
func (o *ObjectsClassReferencesCreateOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create o k response has a 4xx status code
func (o *ObjectsClassReferencesCreateOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class references create o k response has a 5xx status code
func (o *ObjectsClassReferencesCreateOK) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create o k response a status code equal to that given
func (o *ObjectsClassReferencesCreateOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the objects class references create o k response
func (o *ObjectsClassReferencesCreateOK) Code() int {
	return 200
}

func (o *ObjectsClassReferencesCreateOK) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateOK ", 200)
}

func (o *ObjectsClassReferencesCreateOK) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateOK ", 200)
}

func (o *ObjectsClassReferencesCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesCreateBadRequest creates a ObjectsClassReferencesCreateBadRequest with default headers values
func NewObjectsClassReferencesCreateBadRequest() *ObjectsClassReferencesCreateBadRequest {
	return &ObjectsClassReferencesCreateBadRequest{}
}

/*
ObjectsClassReferencesCreateBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type ObjectsClassReferencesCreateBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references create bad request response has a 2xx status code
func (o *ObjectsClassReferencesCreateBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create bad request response has a 3xx status code
func (o *ObjectsClassReferencesCreateBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create bad request response has a 4xx status code
func (o *ObjectsClassReferencesCreateBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references create bad request response has a 5xx status code
func (o *ObjectsClassReferencesCreateBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create bad request response a status code equal to that given
func (o *ObjectsClassReferencesCreateBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the objects class references create bad request response
func (o *ObjectsClassReferencesCreateBadRequest) Code() int {
	return 400
}

func (o *ObjectsClassReferencesCreateBadRequest) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsClassReferencesCreateBadRequest) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsClassReferencesCreateBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesCreateUnauthorized creates a ObjectsClassReferencesCreateUnauthorized with default headers values
func NewObjectsClassReferencesCreateUnauthorized() *ObjectsClassReferencesCreateUnauthorized {
	return &ObjectsClassReferencesCreateUnauthorized{}
}

/*
ObjectsClassReferencesCreateUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassReferencesCreateUnauthorized struct{}

// IsSuccess returns true when this objects class references create unauthorized response has a 2xx status code
func (o *ObjectsClassReferencesCreateUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create unauthorized response has a 3xx status code
func (o *ObjectsClassReferencesCreateUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create unauthorized response has a 4xx status code
func (o *ObjectsClassReferencesCreateUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references create unauthorized response has a 5xx status code
func (o *ObjectsClassReferencesCreateUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create unauthorized response a status code equal to that given
func (o *ObjectsClassReferencesCreateUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the objects class references create unauthorized response
func (o *ObjectsClassReferencesCreateUnauthorized) Code() int {
	return 401
}

func (o *ObjectsClassReferencesCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnauthorized ", 401)
}

func (o *ObjectsClassReferencesCreateUnauthorized) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnauthorized ", 401)
}

func (o *ObjectsClassReferencesCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesCreateForbidden creates a ObjectsClassReferencesCreateForbidden with default headers values
func NewObjectsClassReferencesCreateForbidden() *ObjectsClassReferencesCreateForbidden {
	return &ObjectsClassReferencesCreateForbidden{}
}

/*
ObjectsClassReferencesCreateForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ObjectsClassReferencesCreateForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references create forbidden response has a 2xx status code
func (o *ObjectsClassReferencesCreateForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create forbidden response has a 3xx status code
func (o *ObjectsClassReferencesCreateForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create forbidden response has a 4xx status code
func (o *ObjectsClassReferencesCreateForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references create forbidden response has a 5xx status code
func (o *ObjectsClassReferencesCreateForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create forbidden response a status code equal to that given
func (o *ObjectsClassReferencesCreateForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the objects class references create forbidden response
func (o *ObjectsClassReferencesCreateForbidden) Code() int {
	return 403
}

func (o *ObjectsClassReferencesCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassReferencesCreateForbidden) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassReferencesCreateForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesCreateNotFound creates a ObjectsClassReferencesCreateNotFound with default headers values
func NewObjectsClassReferencesCreateNotFound() *ObjectsClassReferencesCreateNotFound {
	return &ObjectsClassReferencesCreateNotFound{}
}

/*
ObjectsClassReferencesCreateNotFound describes a response with status code 404, with default header values.

Source object doesn't exist.
*/
type ObjectsClassReferencesCreateNotFound struct{}

// IsSuccess returns true when this objects class references create not found response has a 2xx status code
func (o *ObjectsClassReferencesCreateNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create not found response has a 3xx status code
func (o *ObjectsClassReferencesCreateNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create not found response has a 4xx status code
func (o *ObjectsClassReferencesCreateNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references create not found response has a 5xx status code
func (o *ObjectsClassReferencesCreateNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create not found response a status code equal to that given
func (o *ObjectsClassReferencesCreateNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the objects class references create not found response
func (o *ObjectsClassReferencesCreateNotFound) Code() int {
	return 404
}

func (o *ObjectsClassReferencesCreateNotFound) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateNotFound ", 404)
}

func (o *ObjectsClassReferencesCreateNotFound) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateNotFound ", 404)
}

func (o *ObjectsClassReferencesCreateNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesCreateUnprocessableEntity creates a ObjectsClassReferencesCreateUnprocessableEntity with default headers values
func NewObjectsClassReferencesCreateUnprocessableEntity() *ObjectsClassReferencesCreateUnprocessableEntity {
	return &ObjectsClassReferencesCreateUnprocessableEntity{}
}

/*
ObjectsClassReferencesCreateUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?
*/
type ObjectsClassReferencesCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references create unprocessable entity response has a 2xx status code
func (o *ObjectsClassReferencesCreateUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create unprocessable entity response has a 3xx status code
func (o *ObjectsClassReferencesCreateUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create unprocessable entity response has a 4xx status code
func (o *ObjectsClassReferencesCreateUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references create unprocessable entity response has a 5xx status code
func (o *ObjectsClassReferencesCreateUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references create unprocessable entity response a status code equal to that given
func (o *ObjectsClassReferencesCreateUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the objects class references create unprocessable entity response
func (o *ObjectsClassReferencesCreateUnprocessableEntity) Code() int {
	return 422
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesCreateInternalServerError creates a ObjectsClassReferencesCreateInternalServerError with default headers values
func NewObjectsClassReferencesCreateInternalServerError() *ObjectsClassReferencesCreateInternalServerError {
	return &ObjectsClassReferencesCreateInternalServerError{}
}

/*
ObjectsClassReferencesCreateInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassReferencesCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references create internal server error response has a 2xx status code
func (o *ObjectsClassReferencesCreateInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references create internal server error response has a 3xx status code
func (o *ObjectsClassReferencesCreateInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references create internal server error response has a 4xx status code
func (o *ObjectsClassReferencesCreateInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class references create internal server error response has a 5xx status code
func (o *ObjectsClassReferencesCreateInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this objects class references create internal server error response a status code equal to that given
func (o *ObjectsClassReferencesCreateInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the objects class references create internal server error response
func (o *ObjectsClassReferencesCreateInternalServerError) Code() int {
	return 500
}

func (o *ObjectsClassReferencesCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassReferencesCreateInternalServerError) String() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassReferencesCreateInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

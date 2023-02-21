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

// ObjectsClassReferencesPutReader is a Reader for the ObjectsClassReferencesPut structure.
type ObjectsClassReferencesPutReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassReferencesPutReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassReferencesPutOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewObjectsClassReferencesPutBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewObjectsClassReferencesPutUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassReferencesPutForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassReferencesPutNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassReferencesPutUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassReferencesPutInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsClassReferencesPutOK creates a ObjectsClassReferencesPutOK with default headers values
func NewObjectsClassReferencesPutOK() *ObjectsClassReferencesPutOK {
	return &ObjectsClassReferencesPutOK{}
}

/*
ObjectsClassReferencesPutOK describes a response with status code 200, with default header values.

Successfully replaced all the references.
*/
type ObjectsClassReferencesPutOK struct{}

// IsSuccess returns true when this objects class references put o k response has a 2xx status code
func (o *ObjectsClassReferencesPutOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this objects class references put o k response has a 3xx status code
func (o *ObjectsClassReferencesPutOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put o k response has a 4xx status code
func (o *ObjectsClassReferencesPutOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class references put o k response has a 5xx status code
func (o *ObjectsClassReferencesPutOK) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put o k response a status code equal to that given
func (o *ObjectsClassReferencesPutOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the objects class references put o k response
func (o *ObjectsClassReferencesPutOK) Code() int {
	return 200
}

func (o *ObjectsClassReferencesPutOK) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutOK ", 200)
}

func (o *ObjectsClassReferencesPutOK) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutOK ", 200)
}

func (o *ObjectsClassReferencesPutOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesPutBadRequest creates a ObjectsClassReferencesPutBadRequest with default headers values
func NewObjectsClassReferencesPutBadRequest() *ObjectsClassReferencesPutBadRequest {
	return &ObjectsClassReferencesPutBadRequest{}
}

/*
ObjectsClassReferencesPutBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type ObjectsClassReferencesPutBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references put bad request response has a 2xx status code
func (o *ObjectsClassReferencesPutBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put bad request response has a 3xx status code
func (o *ObjectsClassReferencesPutBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put bad request response has a 4xx status code
func (o *ObjectsClassReferencesPutBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references put bad request response has a 5xx status code
func (o *ObjectsClassReferencesPutBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put bad request response a status code equal to that given
func (o *ObjectsClassReferencesPutBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the objects class references put bad request response
func (o *ObjectsClassReferencesPutBadRequest) Code() int {
	return 400
}

func (o *ObjectsClassReferencesPutBadRequest) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsClassReferencesPutBadRequest) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsClassReferencesPutBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesPutBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesPutUnauthorized creates a ObjectsClassReferencesPutUnauthorized with default headers values
func NewObjectsClassReferencesPutUnauthorized() *ObjectsClassReferencesPutUnauthorized {
	return &ObjectsClassReferencesPutUnauthorized{}
}

/*
ObjectsClassReferencesPutUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassReferencesPutUnauthorized struct{}

// IsSuccess returns true when this objects class references put unauthorized response has a 2xx status code
func (o *ObjectsClassReferencesPutUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put unauthorized response has a 3xx status code
func (o *ObjectsClassReferencesPutUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put unauthorized response has a 4xx status code
func (o *ObjectsClassReferencesPutUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references put unauthorized response has a 5xx status code
func (o *ObjectsClassReferencesPutUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put unauthorized response a status code equal to that given
func (o *ObjectsClassReferencesPutUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the objects class references put unauthorized response
func (o *ObjectsClassReferencesPutUnauthorized) Code() int {
	return 401
}

func (o *ObjectsClassReferencesPutUnauthorized) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutUnauthorized ", 401)
}

func (o *ObjectsClassReferencesPutUnauthorized) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutUnauthorized ", 401)
}

func (o *ObjectsClassReferencesPutUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesPutForbidden creates a ObjectsClassReferencesPutForbidden with default headers values
func NewObjectsClassReferencesPutForbidden() *ObjectsClassReferencesPutForbidden {
	return &ObjectsClassReferencesPutForbidden{}
}

/*
ObjectsClassReferencesPutForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ObjectsClassReferencesPutForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references put forbidden response has a 2xx status code
func (o *ObjectsClassReferencesPutForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put forbidden response has a 3xx status code
func (o *ObjectsClassReferencesPutForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put forbidden response has a 4xx status code
func (o *ObjectsClassReferencesPutForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references put forbidden response has a 5xx status code
func (o *ObjectsClassReferencesPutForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put forbidden response a status code equal to that given
func (o *ObjectsClassReferencesPutForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the objects class references put forbidden response
func (o *ObjectsClassReferencesPutForbidden) Code() int {
	return 403
}

func (o *ObjectsClassReferencesPutForbidden) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassReferencesPutForbidden) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassReferencesPutForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesPutForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesPutNotFound creates a ObjectsClassReferencesPutNotFound with default headers values
func NewObjectsClassReferencesPutNotFound() *ObjectsClassReferencesPutNotFound {
	return &ObjectsClassReferencesPutNotFound{}
}

/*
ObjectsClassReferencesPutNotFound describes a response with status code 404, with default header values.

Source object doesn't exist.
*/
type ObjectsClassReferencesPutNotFound struct{}

// IsSuccess returns true when this objects class references put not found response has a 2xx status code
func (o *ObjectsClassReferencesPutNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put not found response has a 3xx status code
func (o *ObjectsClassReferencesPutNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put not found response has a 4xx status code
func (o *ObjectsClassReferencesPutNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references put not found response has a 5xx status code
func (o *ObjectsClassReferencesPutNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put not found response a status code equal to that given
func (o *ObjectsClassReferencesPutNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the objects class references put not found response
func (o *ObjectsClassReferencesPutNotFound) Code() int {
	return 404
}

func (o *ObjectsClassReferencesPutNotFound) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutNotFound ", 404)
}

func (o *ObjectsClassReferencesPutNotFound) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutNotFound ", 404)
}

func (o *ObjectsClassReferencesPutNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsClassReferencesPutUnprocessableEntity creates a ObjectsClassReferencesPutUnprocessableEntity with default headers values
func NewObjectsClassReferencesPutUnprocessableEntity() *ObjectsClassReferencesPutUnprocessableEntity {
	return &ObjectsClassReferencesPutUnprocessableEntity{}
}

/*
ObjectsClassReferencesPutUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?
*/
type ObjectsClassReferencesPutUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references put unprocessable entity response has a 2xx status code
func (o *ObjectsClassReferencesPutUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put unprocessable entity response has a 3xx status code
func (o *ObjectsClassReferencesPutUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put unprocessable entity response has a 4xx status code
func (o *ObjectsClassReferencesPutUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class references put unprocessable entity response has a 5xx status code
func (o *ObjectsClassReferencesPutUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class references put unprocessable entity response a status code equal to that given
func (o *ObjectsClassReferencesPutUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the objects class references put unprocessable entity response
func (o *ObjectsClassReferencesPutUnprocessableEntity) Code() int {
	return 422
}

func (o *ObjectsClassReferencesPutUnprocessableEntity) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassReferencesPutUnprocessableEntity) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassReferencesPutUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesPutUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesPutInternalServerError creates a ObjectsClassReferencesPutInternalServerError with default headers values
func NewObjectsClassReferencesPutInternalServerError() *ObjectsClassReferencesPutInternalServerError {
	return &ObjectsClassReferencesPutInternalServerError{}
}

/*
ObjectsClassReferencesPutInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassReferencesPutInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class references put internal server error response has a 2xx status code
func (o *ObjectsClassReferencesPutInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class references put internal server error response has a 3xx status code
func (o *ObjectsClassReferencesPutInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class references put internal server error response has a 4xx status code
func (o *ObjectsClassReferencesPutInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class references put internal server error response has a 5xx status code
func (o *ObjectsClassReferencesPutInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this objects class references put internal server error response a status code equal to that given
func (o *ObjectsClassReferencesPutInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the objects class references put internal server error response
func (o *ObjectsClassReferencesPutInternalServerError) Code() int {
	return 500
}

func (o *ObjectsClassReferencesPutInternalServerError) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassReferencesPutInternalServerError) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesPutInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassReferencesPutInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesPutInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

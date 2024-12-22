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

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/liutizhong/weaviate/entities/models"
)

// ObjectsClassPutReader is a Reader for the ObjectsClassPut structure.
type ObjectsClassPutReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassPutReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassPutOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsClassPutUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassPutForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassPutNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassPutUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassPutInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsClassPutOK creates a ObjectsClassPutOK with default headers values
func NewObjectsClassPutOK() *ObjectsClassPutOK {
	return &ObjectsClassPutOK{}
}

/*
ObjectsClassPutOK describes a response with status code 200, with default header values.

Successfully received.
*/
type ObjectsClassPutOK struct {
	Payload *models.Object
}

// IsSuccess returns true when this objects class put o k response has a 2xx status code
func (o *ObjectsClassPutOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this objects class put o k response has a 3xx status code
func (o *ObjectsClassPutOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put o k response has a 4xx status code
func (o *ObjectsClassPutOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class put o k response has a 5xx status code
func (o *ObjectsClassPutOK) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class put o k response a status code equal to that given
func (o *ObjectsClassPutOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the objects class put o k response
func (o *ObjectsClassPutOK) Code() int {
	return 200
}

func (o *ObjectsClassPutOK) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutOK  %+v", 200, o.Payload)
}

func (o *ObjectsClassPutOK) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutOK  %+v", 200, o.Payload)
}

func (o *ObjectsClassPutOK) GetPayload() *models.Object {
	return o.Payload
}

func (o *ObjectsClassPutOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Object)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutUnauthorized creates a ObjectsClassPutUnauthorized with default headers values
func NewObjectsClassPutUnauthorized() *ObjectsClassPutUnauthorized {
	return &ObjectsClassPutUnauthorized{}
}

/*
ObjectsClassPutUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassPutUnauthorized struct {
}

// IsSuccess returns true when this objects class put unauthorized response has a 2xx status code
func (o *ObjectsClassPutUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class put unauthorized response has a 3xx status code
func (o *ObjectsClassPutUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put unauthorized response has a 4xx status code
func (o *ObjectsClassPutUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class put unauthorized response has a 5xx status code
func (o *ObjectsClassPutUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class put unauthorized response a status code equal to that given
func (o *ObjectsClassPutUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the objects class put unauthorized response
func (o *ObjectsClassPutUnauthorized) Code() int {
	return 401
}

func (o *ObjectsClassPutUnauthorized) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnauthorized ", 401)
}

func (o *ObjectsClassPutUnauthorized) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnauthorized ", 401)
}

func (o *ObjectsClassPutUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassPutForbidden creates a ObjectsClassPutForbidden with default headers values
func NewObjectsClassPutForbidden() *ObjectsClassPutForbidden {
	return &ObjectsClassPutForbidden{}
}

/*
ObjectsClassPutForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ObjectsClassPutForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class put forbidden response has a 2xx status code
func (o *ObjectsClassPutForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class put forbidden response has a 3xx status code
func (o *ObjectsClassPutForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put forbidden response has a 4xx status code
func (o *ObjectsClassPutForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class put forbidden response has a 5xx status code
func (o *ObjectsClassPutForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class put forbidden response a status code equal to that given
func (o *ObjectsClassPutForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the objects class put forbidden response
func (o *ObjectsClassPutForbidden) Code() int {
	return 403
}

func (o *ObjectsClassPutForbidden) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassPutForbidden) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassPutForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutNotFound creates a ObjectsClassPutNotFound with default headers values
func NewObjectsClassPutNotFound() *ObjectsClassPutNotFound {
	return &ObjectsClassPutNotFound{}
}

/*
ObjectsClassPutNotFound describes a response with status code 404, with default header values.

Successful query result but no resource was found.
*/
type ObjectsClassPutNotFound struct {
}

// IsSuccess returns true when this objects class put not found response has a 2xx status code
func (o *ObjectsClassPutNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class put not found response has a 3xx status code
func (o *ObjectsClassPutNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put not found response has a 4xx status code
func (o *ObjectsClassPutNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class put not found response has a 5xx status code
func (o *ObjectsClassPutNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class put not found response a status code equal to that given
func (o *ObjectsClassPutNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the objects class put not found response
func (o *ObjectsClassPutNotFound) Code() int {
	return 404
}

func (o *ObjectsClassPutNotFound) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutNotFound ", 404)
}

func (o *ObjectsClassPutNotFound) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutNotFound ", 404)
}

func (o *ObjectsClassPutNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassPutUnprocessableEntity creates a ObjectsClassPutUnprocessableEntity with default headers values
func NewObjectsClassPutUnprocessableEntity() *ObjectsClassPutUnprocessableEntity {
	return &ObjectsClassPutUnprocessableEntity{}
}

/*
ObjectsClassPutUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type ObjectsClassPutUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class put unprocessable entity response has a 2xx status code
func (o *ObjectsClassPutUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class put unprocessable entity response has a 3xx status code
func (o *ObjectsClassPutUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put unprocessable entity response has a 4xx status code
func (o *ObjectsClassPutUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class put unprocessable entity response has a 5xx status code
func (o *ObjectsClassPutUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class put unprocessable entity response a status code equal to that given
func (o *ObjectsClassPutUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the objects class put unprocessable entity response
func (o *ObjectsClassPutUnprocessableEntity) Code() int {
	return 422
}

func (o *ObjectsClassPutUnprocessableEntity) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassPutUnprocessableEntity) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassPutUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutInternalServerError creates a ObjectsClassPutInternalServerError with default headers values
func NewObjectsClassPutInternalServerError() *ObjectsClassPutInternalServerError {
	return &ObjectsClassPutInternalServerError{}
}

/*
ObjectsClassPutInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassPutInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class put internal server error response has a 2xx status code
func (o *ObjectsClassPutInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class put internal server error response has a 3xx status code
func (o *ObjectsClassPutInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class put internal server error response has a 4xx status code
func (o *ObjectsClassPutInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class put internal server error response has a 5xx status code
func (o *ObjectsClassPutInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this objects class put internal server error response a status code equal to that given
func (o *ObjectsClassPutInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the objects class put internal server error response
func (o *ObjectsClassPutInternalServerError) Code() int {
	return 500
}

func (o *ObjectsClassPutInternalServerError) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassPutInternalServerError) String() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassPutInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

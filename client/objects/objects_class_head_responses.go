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

// ObjectsClassHeadReader is a Reader for the ObjectsClassHead structure.
type ObjectsClassHeadReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassHeadReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewObjectsClassHeadNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsClassHeadUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassHeadForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassHeadNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassHeadUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassHeadInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsClassHeadNoContent creates a ObjectsClassHeadNoContent with default headers values
func NewObjectsClassHeadNoContent() *ObjectsClassHeadNoContent {
	return &ObjectsClassHeadNoContent{}
}

/*
ObjectsClassHeadNoContent describes a response with status code 204, with default header values.

Object exists.
*/
type ObjectsClassHeadNoContent struct {
}

// IsSuccess returns true when this objects class head no content response has a 2xx status code
func (o *ObjectsClassHeadNoContent) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this objects class head no content response has a 3xx status code
func (o *ObjectsClassHeadNoContent) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head no content response has a 4xx status code
func (o *ObjectsClassHeadNoContent) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class head no content response has a 5xx status code
func (o *ObjectsClassHeadNoContent) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class head no content response a status code equal to that given
func (o *ObjectsClassHeadNoContent) IsCode(code int) bool {
	return code == 204
}

// Code gets the status code for the objects class head no content response
func (o *ObjectsClassHeadNoContent) Code() int {
	return 204
}

func (o *ObjectsClassHeadNoContent) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadNoContent ", 204)
}

func (o *ObjectsClassHeadNoContent) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadNoContent ", 204)
}

func (o *ObjectsClassHeadNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassHeadUnauthorized creates a ObjectsClassHeadUnauthorized with default headers values
func NewObjectsClassHeadUnauthorized() *ObjectsClassHeadUnauthorized {
	return &ObjectsClassHeadUnauthorized{}
}

/*
ObjectsClassHeadUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassHeadUnauthorized struct {
}

// IsSuccess returns true when this objects class head unauthorized response has a 2xx status code
func (o *ObjectsClassHeadUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class head unauthorized response has a 3xx status code
func (o *ObjectsClassHeadUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head unauthorized response has a 4xx status code
func (o *ObjectsClassHeadUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class head unauthorized response has a 5xx status code
func (o *ObjectsClassHeadUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class head unauthorized response a status code equal to that given
func (o *ObjectsClassHeadUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the objects class head unauthorized response
func (o *ObjectsClassHeadUnauthorized) Code() int {
	return 401
}

func (o *ObjectsClassHeadUnauthorized) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadUnauthorized ", 401)
}

func (o *ObjectsClassHeadUnauthorized) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadUnauthorized ", 401)
}

func (o *ObjectsClassHeadUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassHeadForbidden creates a ObjectsClassHeadForbidden with default headers values
func NewObjectsClassHeadForbidden() *ObjectsClassHeadForbidden {
	return &ObjectsClassHeadForbidden{}
}

/*
ObjectsClassHeadForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ObjectsClassHeadForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class head forbidden response has a 2xx status code
func (o *ObjectsClassHeadForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class head forbidden response has a 3xx status code
func (o *ObjectsClassHeadForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head forbidden response has a 4xx status code
func (o *ObjectsClassHeadForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class head forbidden response has a 5xx status code
func (o *ObjectsClassHeadForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class head forbidden response a status code equal to that given
func (o *ObjectsClassHeadForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the objects class head forbidden response
func (o *ObjectsClassHeadForbidden) Code() int {
	return 403
}

func (o *ObjectsClassHeadForbidden) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassHeadForbidden) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassHeadForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassHeadForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassHeadNotFound creates a ObjectsClassHeadNotFound with default headers values
func NewObjectsClassHeadNotFound() *ObjectsClassHeadNotFound {
	return &ObjectsClassHeadNotFound{}
}

/*
ObjectsClassHeadNotFound describes a response with status code 404, with default header values.

Object doesn't exist.
*/
type ObjectsClassHeadNotFound struct {
}

// IsSuccess returns true when this objects class head not found response has a 2xx status code
func (o *ObjectsClassHeadNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class head not found response has a 3xx status code
func (o *ObjectsClassHeadNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head not found response has a 4xx status code
func (o *ObjectsClassHeadNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class head not found response has a 5xx status code
func (o *ObjectsClassHeadNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class head not found response a status code equal to that given
func (o *ObjectsClassHeadNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the objects class head not found response
func (o *ObjectsClassHeadNotFound) Code() int {
	return 404
}

func (o *ObjectsClassHeadNotFound) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadNotFound ", 404)
}

func (o *ObjectsClassHeadNotFound) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadNotFound ", 404)
}

func (o *ObjectsClassHeadNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassHeadUnprocessableEntity creates a ObjectsClassHeadUnprocessableEntity with default headers values
func NewObjectsClassHeadUnprocessableEntity() *ObjectsClassHeadUnprocessableEntity {
	return &ObjectsClassHeadUnprocessableEntity{}
}

/*
ObjectsClassHeadUnprocessableEntity describes a response with status code 422, with default header values.

Request is well-formed (i.e., syntactically correct), but erroneous.
*/
type ObjectsClassHeadUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class head unprocessable entity response has a 2xx status code
func (o *ObjectsClassHeadUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class head unprocessable entity response has a 3xx status code
func (o *ObjectsClassHeadUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head unprocessable entity response has a 4xx status code
func (o *ObjectsClassHeadUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects class head unprocessable entity response has a 5xx status code
func (o *ObjectsClassHeadUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this objects class head unprocessable entity response a status code equal to that given
func (o *ObjectsClassHeadUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the objects class head unprocessable entity response
func (o *ObjectsClassHeadUnprocessableEntity) Code() int {
	return 422
}

func (o *ObjectsClassHeadUnprocessableEntity) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassHeadUnprocessableEntity) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassHeadUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassHeadUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassHeadInternalServerError creates a ObjectsClassHeadInternalServerError with default headers values
func NewObjectsClassHeadInternalServerError() *ObjectsClassHeadInternalServerError {
	return &ObjectsClassHeadInternalServerError{}
}

/*
ObjectsClassHeadInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassHeadInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects class head internal server error response has a 2xx status code
func (o *ObjectsClassHeadInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects class head internal server error response has a 3xx status code
func (o *ObjectsClassHeadInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects class head internal server error response has a 4xx status code
func (o *ObjectsClassHeadInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects class head internal server error response has a 5xx status code
func (o *ObjectsClassHeadInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this objects class head internal server error response a status code equal to that given
func (o *ObjectsClassHeadInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the objects class head internal server error response
func (o *ObjectsClassHeadInternalServerError) Code() int {
	return 500
}

func (o *ObjectsClassHeadInternalServerError) Error() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassHeadInternalServerError) String() string {
	return fmt.Sprintf("[HEAD /objects/{className}/{id}][%d] objectsClassHeadInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassHeadInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassHeadInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

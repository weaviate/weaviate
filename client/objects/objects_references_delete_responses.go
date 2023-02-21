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

// ObjectsReferencesDeleteReader is a Reader for the ObjectsReferencesDelete structure.
type ObjectsReferencesDeleteReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsReferencesDeleteReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewObjectsReferencesDeleteNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsReferencesDeleteUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsReferencesDeleteForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsReferencesDeleteNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsReferencesDeleteInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsReferencesDeleteNoContent creates a ObjectsReferencesDeleteNoContent with default headers values
func NewObjectsReferencesDeleteNoContent() *ObjectsReferencesDeleteNoContent {
	return &ObjectsReferencesDeleteNoContent{}
}

/*
ObjectsReferencesDeleteNoContent describes a response with status code 204, with default header values.

Successfully deleted.
*/
type ObjectsReferencesDeleteNoContent struct{}

// IsSuccess returns true when this objects references delete no content response has a 2xx status code
func (o *ObjectsReferencesDeleteNoContent) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this objects references delete no content response has a 3xx status code
func (o *ObjectsReferencesDeleteNoContent) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects references delete no content response has a 4xx status code
func (o *ObjectsReferencesDeleteNoContent) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects references delete no content response has a 5xx status code
func (o *ObjectsReferencesDeleteNoContent) IsServerError() bool {
	return false
}

// IsCode returns true when this objects references delete no content response a status code equal to that given
func (o *ObjectsReferencesDeleteNoContent) IsCode(code int) bool {
	return code == 204
}

// Code gets the status code for the objects references delete no content response
func (o *ObjectsReferencesDeleteNoContent) Code() int {
	return 204
}

func (o *ObjectsReferencesDeleteNoContent) Error() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteNoContent ", 204)
}

func (o *ObjectsReferencesDeleteNoContent) String() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteNoContent ", 204)
}

func (o *ObjectsReferencesDeleteNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsReferencesDeleteUnauthorized creates a ObjectsReferencesDeleteUnauthorized with default headers values
func NewObjectsReferencesDeleteUnauthorized() *ObjectsReferencesDeleteUnauthorized {
	return &ObjectsReferencesDeleteUnauthorized{}
}

/*
ObjectsReferencesDeleteUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsReferencesDeleteUnauthorized struct{}

// IsSuccess returns true when this objects references delete unauthorized response has a 2xx status code
func (o *ObjectsReferencesDeleteUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects references delete unauthorized response has a 3xx status code
func (o *ObjectsReferencesDeleteUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects references delete unauthorized response has a 4xx status code
func (o *ObjectsReferencesDeleteUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects references delete unauthorized response has a 5xx status code
func (o *ObjectsReferencesDeleteUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this objects references delete unauthorized response a status code equal to that given
func (o *ObjectsReferencesDeleteUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the objects references delete unauthorized response
func (o *ObjectsReferencesDeleteUnauthorized) Code() int {
	return 401
}

func (o *ObjectsReferencesDeleteUnauthorized) Error() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteUnauthorized ", 401)
}

func (o *ObjectsReferencesDeleteUnauthorized) String() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteUnauthorized ", 401)
}

func (o *ObjectsReferencesDeleteUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewObjectsReferencesDeleteForbidden creates a ObjectsReferencesDeleteForbidden with default headers values
func NewObjectsReferencesDeleteForbidden() *ObjectsReferencesDeleteForbidden {
	return &ObjectsReferencesDeleteForbidden{}
}

/*
ObjectsReferencesDeleteForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ObjectsReferencesDeleteForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects references delete forbidden response has a 2xx status code
func (o *ObjectsReferencesDeleteForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects references delete forbidden response has a 3xx status code
func (o *ObjectsReferencesDeleteForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects references delete forbidden response has a 4xx status code
func (o *ObjectsReferencesDeleteForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects references delete forbidden response has a 5xx status code
func (o *ObjectsReferencesDeleteForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this objects references delete forbidden response a status code equal to that given
func (o *ObjectsReferencesDeleteForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the objects references delete forbidden response
func (o *ObjectsReferencesDeleteForbidden) Code() int {
	return 403
}

func (o *ObjectsReferencesDeleteForbidden) Error() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsReferencesDeleteForbidden) String() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsReferencesDeleteForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsReferencesDeleteForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsReferencesDeleteNotFound creates a ObjectsReferencesDeleteNotFound with default headers values
func NewObjectsReferencesDeleteNotFound() *ObjectsReferencesDeleteNotFound {
	return &ObjectsReferencesDeleteNotFound{}
}

/*
ObjectsReferencesDeleteNotFound describes a response with status code 404, with default header values.

Successful query result but no resource was found.
*/
type ObjectsReferencesDeleteNotFound struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects references delete not found response has a 2xx status code
func (o *ObjectsReferencesDeleteNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects references delete not found response has a 3xx status code
func (o *ObjectsReferencesDeleteNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects references delete not found response has a 4xx status code
func (o *ObjectsReferencesDeleteNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this objects references delete not found response has a 5xx status code
func (o *ObjectsReferencesDeleteNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this objects references delete not found response a status code equal to that given
func (o *ObjectsReferencesDeleteNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the objects references delete not found response
func (o *ObjectsReferencesDeleteNotFound) Code() int {
	return 404
}

func (o *ObjectsReferencesDeleteNotFound) Error() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteNotFound  %+v", 404, o.Payload)
}

func (o *ObjectsReferencesDeleteNotFound) String() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteNotFound  %+v", 404, o.Payload)
}

func (o *ObjectsReferencesDeleteNotFound) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsReferencesDeleteNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsReferencesDeleteInternalServerError creates a ObjectsReferencesDeleteInternalServerError with default headers values
func NewObjectsReferencesDeleteInternalServerError() *ObjectsReferencesDeleteInternalServerError {
	return &ObjectsReferencesDeleteInternalServerError{}
}

/*
ObjectsReferencesDeleteInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsReferencesDeleteInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this objects references delete internal server error response has a 2xx status code
func (o *ObjectsReferencesDeleteInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this objects references delete internal server error response has a 3xx status code
func (o *ObjectsReferencesDeleteInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this objects references delete internal server error response has a 4xx status code
func (o *ObjectsReferencesDeleteInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this objects references delete internal server error response has a 5xx status code
func (o *ObjectsReferencesDeleteInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this objects references delete internal server error response a status code equal to that given
func (o *ObjectsReferencesDeleteInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the objects references delete internal server error response
func (o *ObjectsReferencesDeleteInternalServerError) Code() int {
	return 500
}

func (o *ObjectsReferencesDeleteInternalServerError) Error() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsReferencesDeleteInternalServerError) String() string {
	return fmt.Sprintf("[DELETE /objects/{id}/references/{propertyName}][%d] objectsReferencesDeleteInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsReferencesDeleteInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsReferencesDeleteInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

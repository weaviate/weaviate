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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/liutizhong/weaviate/entities/models"
)

// HasPermissionReader is a Reader for the HasPermission structure.
type HasPermissionReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *HasPermissionReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewHasPermissionOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewHasPermissionBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewHasPermissionUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewHasPermissionForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewHasPermissionUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewHasPermissionInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewHasPermissionOK creates a HasPermissionOK with default headers values
func NewHasPermissionOK() *HasPermissionOK {
	return &HasPermissionOK{}
}

/*
HasPermissionOK describes a response with status code 200, with default header values.

Permission check was successful
*/
type HasPermissionOK struct {
	Payload bool
}

// IsSuccess returns true when this has permission o k response has a 2xx status code
func (o *HasPermissionOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this has permission o k response has a 3xx status code
func (o *HasPermissionOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission o k response has a 4xx status code
func (o *HasPermissionOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this has permission o k response has a 5xx status code
func (o *HasPermissionOK) IsServerError() bool {
	return false
}

// IsCode returns true when this has permission o k response a status code equal to that given
func (o *HasPermissionOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the has permission o k response
func (o *HasPermissionOK) Code() int {
	return 200
}

func (o *HasPermissionOK) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionOK  %+v", 200, o.Payload)
}

func (o *HasPermissionOK) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionOK  %+v", 200, o.Payload)
}

func (o *HasPermissionOK) GetPayload() bool {
	return o.Payload
}

func (o *HasPermissionOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewHasPermissionBadRequest creates a HasPermissionBadRequest with default headers values
func NewHasPermissionBadRequest() *HasPermissionBadRequest {
	return &HasPermissionBadRequest{}
}

/*
HasPermissionBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type HasPermissionBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this has permission bad request response has a 2xx status code
func (o *HasPermissionBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this has permission bad request response has a 3xx status code
func (o *HasPermissionBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission bad request response has a 4xx status code
func (o *HasPermissionBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this has permission bad request response has a 5xx status code
func (o *HasPermissionBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this has permission bad request response a status code equal to that given
func (o *HasPermissionBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the has permission bad request response
func (o *HasPermissionBadRequest) Code() int {
	return 400
}

func (o *HasPermissionBadRequest) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionBadRequest  %+v", 400, o.Payload)
}

func (o *HasPermissionBadRequest) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionBadRequest  %+v", 400, o.Payload)
}

func (o *HasPermissionBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *HasPermissionBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewHasPermissionUnauthorized creates a HasPermissionUnauthorized with default headers values
func NewHasPermissionUnauthorized() *HasPermissionUnauthorized {
	return &HasPermissionUnauthorized{}
}

/*
HasPermissionUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type HasPermissionUnauthorized struct {
}

// IsSuccess returns true when this has permission unauthorized response has a 2xx status code
func (o *HasPermissionUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this has permission unauthorized response has a 3xx status code
func (o *HasPermissionUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission unauthorized response has a 4xx status code
func (o *HasPermissionUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this has permission unauthorized response has a 5xx status code
func (o *HasPermissionUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this has permission unauthorized response a status code equal to that given
func (o *HasPermissionUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the has permission unauthorized response
func (o *HasPermissionUnauthorized) Code() int {
	return 401
}

func (o *HasPermissionUnauthorized) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionUnauthorized ", 401)
}

func (o *HasPermissionUnauthorized) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionUnauthorized ", 401)
}

func (o *HasPermissionUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewHasPermissionForbidden creates a HasPermissionForbidden with default headers values
func NewHasPermissionForbidden() *HasPermissionForbidden {
	return &HasPermissionForbidden{}
}

/*
HasPermissionForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type HasPermissionForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this has permission forbidden response has a 2xx status code
func (o *HasPermissionForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this has permission forbidden response has a 3xx status code
func (o *HasPermissionForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission forbidden response has a 4xx status code
func (o *HasPermissionForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this has permission forbidden response has a 5xx status code
func (o *HasPermissionForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this has permission forbidden response a status code equal to that given
func (o *HasPermissionForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the has permission forbidden response
func (o *HasPermissionForbidden) Code() int {
	return 403
}

func (o *HasPermissionForbidden) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionForbidden  %+v", 403, o.Payload)
}

func (o *HasPermissionForbidden) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionForbidden  %+v", 403, o.Payload)
}

func (o *HasPermissionForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *HasPermissionForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewHasPermissionUnprocessableEntity creates a HasPermissionUnprocessableEntity with default headers values
func NewHasPermissionUnprocessableEntity() *HasPermissionUnprocessableEntity {
	return &HasPermissionUnprocessableEntity{}
}

/*
HasPermissionUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type HasPermissionUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this has permission unprocessable entity response has a 2xx status code
func (o *HasPermissionUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this has permission unprocessable entity response has a 3xx status code
func (o *HasPermissionUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission unprocessable entity response has a 4xx status code
func (o *HasPermissionUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this has permission unprocessable entity response has a 5xx status code
func (o *HasPermissionUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this has permission unprocessable entity response a status code equal to that given
func (o *HasPermissionUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the has permission unprocessable entity response
func (o *HasPermissionUnprocessableEntity) Code() int {
	return 422
}

func (o *HasPermissionUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *HasPermissionUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *HasPermissionUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *HasPermissionUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewHasPermissionInternalServerError creates a HasPermissionInternalServerError with default headers values
func NewHasPermissionInternalServerError() *HasPermissionInternalServerError {
	return &HasPermissionInternalServerError{}
}

/*
HasPermissionInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type HasPermissionInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this has permission internal server error response has a 2xx status code
func (o *HasPermissionInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this has permission internal server error response has a 3xx status code
func (o *HasPermissionInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this has permission internal server error response has a 4xx status code
func (o *HasPermissionInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this has permission internal server error response has a 5xx status code
func (o *HasPermissionInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this has permission internal server error response a status code equal to that given
func (o *HasPermissionInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the has permission internal server error response
func (o *HasPermissionInternalServerError) Code() int {
	return 500
}

func (o *HasPermissionInternalServerError) Error() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionInternalServerError  %+v", 500, o.Payload)
}

func (o *HasPermissionInternalServerError) String() string {
	return fmt.Sprintf("[POST /authz/roles/{id}/has-permission][%d] hasPermissionInternalServerError  %+v", 500, o.Payload)
}

func (o *HasPermissionInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *HasPermissionInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

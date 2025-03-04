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

package users

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// ActivateUserReader is a Reader for the ActivateUser structure.
type ActivateUserReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ActivateUserReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewActivateUserOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewActivateUserBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewActivateUserUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewActivateUserForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewActivateUserNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewActivateUserUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewActivateUserInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewActivateUserOK creates a ActivateUserOK with default headers values
func NewActivateUserOK() *ActivateUserOK {
	return &ActivateUserOK{}
}

/*
ActivateUserOK describes a response with status code 200, with default header values.

User successfully activated
*/
type ActivateUserOK struct {
}

// IsSuccess returns true when this activate user o k response has a 2xx status code
func (o *ActivateUserOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this activate user o k response has a 3xx status code
func (o *ActivateUserOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user o k response has a 4xx status code
func (o *ActivateUserOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this activate user o k response has a 5xx status code
func (o *ActivateUserOK) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user o k response a status code equal to that given
func (o *ActivateUserOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the activate user o k response
func (o *ActivateUserOK) Code() int {
	return 200
}

func (o *ActivateUserOK) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserOK ", 200)
}

func (o *ActivateUserOK) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserOK ", 200)
}

func (o *ActivateUserOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewActivateUserBadRequest creates a ActivateUserBadRequest with default headers values
func NewActivateUserBadRequest() *ActivateUserBadRequest {
	return &ActivateUserBadRequest{}
}

/*
ActivateUserBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type ActivateUserBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this activate user bad request response has a 2xx status code
func (o *ActivateUserBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user bad request response has a 3xx status code
func (o *ActivateUserBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user bad request response has a 4xx status code
func (o *ActivateUserBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this activate user bad request response has a 5xx status code
func (o *ActivateUserBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user bad request response a status code equal to that given
func (o *ActivateUserBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the activate user bad request response
func (o *ActivateUserBadRequest) Code() int {
	return 400
}

func (o *ActivateUserBadRequest) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserBadRequest  %+v", 400, o.Payload)
}

func (o *ActivateUserBadRequest) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserBadRequest  %+v", 400, o.Payload)
}

func (o *ActivateUserBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActivateUserBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewActivateUserUnauthorized creates a ActivateUserUnauthorized with default headers values
func NewActivateUserUnauthorized() *ActivateUserUnauthorized {
	return &ActivateUserUnauthorized{}
}

/*
ActivateUserUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ActivateUserUnauthorized struct {
}

// IsSuccess returns true when this activate user unauthorized response has a 2xx status code
func (o *ActivateUserUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user unauthorized response has a 3xx status code
func (o *ActivateUserUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user unauthorized response has a 4xx status code
func (o *ActivateUserUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this activate user unauthorized response has a 5xx status code
func (o *ActivateUserUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user unauthorized response a status code equal to that given
func (o *ActivateUserUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the activate user unauthorized response
func (o *ActivateUserUnauthorized) Code() int {
	return 401
}

func (o *ActivateUserUnauthorized) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserUnauthorized ", 401)
}

func (o *ActivateUserUnauthorized) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserUnauthorized ", 401)
}

func (o *ActivateUserUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewActivateUserForbidden creates a ActivateUserForbidden with default headers values
func NewActivateUserForbidden() *ActivateUserForbidden {
	return &ActivateUserForbidden{}
}

/*
ActivateUserForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ActivateUserForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this activate user forbidden response has a 2xx status code
func (o *ActivateUserForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user forbidden response has a 3xx status code
func (o *ActivateUserForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user forbidden response has a 4xx status code
func (o *ActivateUserForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this activate user forbidden response has a 5xx status code
func (o *ActivateUserForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user forbidden response a status code equal to that given
func (o *ActivateUserForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the activate user forbidden response
func (o *ActivateUserForbidden) Code() int {
	return 403
}

func (o *ActivateUserForbidden) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserForbidden  %+v", 403, o.Payload)
}

func (o *ActivateUserForbidden) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserForbidden  %+v", 403, o.Payload)
}

func (o *ActivateUserForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActivateUserForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewActivateUserNotFound creates a ActivateUserNotFound with default headers values
func NewActivateUserNotFound() *ActivateUserNotFound {
	return &ActivateUserNotFound{}
}

/*
ActivateUserNotFound describes a response with status code 404, with default header values.

user not found
*/
type ActivateUserNotFound struct {
}

// IsSuccess returns true when this activate user not found response has a 2xx status code
func (o *ActivateUserNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user not found response has a 3xx status code
func (o *ActivateUserNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user not found response has a 4xx status code
func (o *ActivateUserNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this activate user not found response has a 5xx status code
func (o *ActivateUserNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user not found response a status code equal to that given
func (o *ActivateUserNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the activate user not found response
func (o *ActivateUserNotFound) Code() int {
	return 404
}

func (o *ActivateUserNotFound) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserNotFound ", 404)
}

func (o *ActivateUserNotFound) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserNotFound ", 404)
}

func (o *ActivateUserNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewActivateUserUnprocessableEntity creates a ActivateUserUnprocessableEntity with default headers values
func NewActivateUserUnprocessableEntity() *ActivateUserUnprocessableEntity {
	return &ActivateUserUnprocessableEntity{}
}

/*
ActivateUserUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous.
*/
type ActivateUserUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this activate user unprocessable entity response has a 2xx status code
func (o *ActivateUserUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user unprocessable entity response has a 3xx status code
func (o *ActivateUserUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user unprocessable entity response has a 4xx status code
func (o *ActivateUserUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this activate user unprocessable entity response has a 5xx status code
func (o *ActivateUserUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this activate user unprocessable entity response a status code equal to that given
func (o *ActivateUserUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the activate user unprocessable entity response
func (o *ActivateUserUnprocessableEntity) Code() int {
	return 422
}

func (o *ActivateUserUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ActivateUserUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ActivateUserUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActivateUserUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewActivateUserInternalServerError creates a ActivateUserInternalServerError with default headers values
func NewActivateUserInternalServerError() *ActivateUserInternalServerError {
	return &ActivateUserInternalServerError{}
}

/*
ActivateUserInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ActivateUserInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this activate user internal server error response has a 2xx status code
func (o *ActivateUserInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this activate user internal server error response has a 3xx status code
func (o *ActivateUserInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this activate user internal server error response has a 4xx status code
func (o *ActivateUserInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this activate user internal server error response has a 5xx status code
func (o *ActivateUserInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this activate user internal server error response a status code equal to that given
func (o *ActivateUserInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the activate user internal server error response
func (o *ActivateUserInternalServerError) Code() int {
	return 500
}

func (o *ActivateUserInternalServerError) Error() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserInternalServerError  %+v", 500, o.Payload)
}

func (o *ActivateUserInternalServerError) String() string {
	return fmt.Sprintf("[POST /users/{user_id}/activate][%d] activateUserInternalServerError  %+v", 500, o.Payload)
}

func (o *ActivateUserInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActivateUserInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

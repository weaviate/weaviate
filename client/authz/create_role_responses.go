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

	"github.com/weaviate/weaviate/entities/models"
)

// CreateRoleReader is a Reader for the CreateRole structure.
type CreateRoleReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CreateRoleReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 201:
		result := NewCreateRoleCreated()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewCreateRoleBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewCreateRoleUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewCreateRoleForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 409:
		result := NewCreateRoleConflict()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewCreateRoleUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewCreateRoleInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewCreateRoleCreated creates a CreateRoleCreated with default headers values
func NewCreateRoleCreated() *CreateRoleCreated {
	return &CreateRoleCreated{}
}

/*
CreateRoleCreated describes a response with status code 201, with default header values.

Role created successfully
*/
type CreateRoleCreated struct {
}

// IsSuccess returns true when this create role created response has a 2xx status code
func (o *CreateRoleCreated) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this create role created response has a 3xx status code
func (o *CreateRoleCreated) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role created response has a 4xx status code
func (o *CreateRoleCreated) IsClientError() bool {
	return false
}

// IsServerError returns true when this create role created response has a 5xx status code
func (o *CreateRoleCreated) IsServerError() bool {
	return false
}

// IsCode returns true when this create role created response a status code equal to that given
func (o *CreateRoleCreated) IsCode(code int) bool {
	return code == 201
}

// Code gets the status code for the create role created response
func (o *CreateRoleCreated) Code() int {
	return 201
}

func (o *CreateRoleCreated) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleCreated ", 201)
}

func (o *CreateRoleCreated) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleCreated ", 201)
}

func (o *CreateRoleCreated) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewCreateRoleBadRequest creates a CreateRoleBadRequest with default headers values
func NewCreateRoleBadRequest() *CreateRoleBadRequest {
	return &CreateRoleBadRequest{}
}

/*
CreateRoleBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type CreateRoleBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this create role bad request response has a 2xx status code
func (o *CreateRoleBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role bad request response has a 3xx status code
func (o *CreateRoleBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role bad request response has a 4xx status code
func (o *CreateRoleBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this create role bad request response has a 5xx status code
func (o *CreateRoleBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this create role bad request response a status code equal to that given
func (o *CreateRoleBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the create role bad request response
func (o *CreateRoleBadRequest) Code() int {
	return 400
}

func (o *CreateRoleBadRequest) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleBadRequest  %+v", 400, o.Payload)
}

func (o *CreateRoleBadRequest) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleBadRequest  %+v", 400, o.Payload)
}

func (o *CreateRoleBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *CreateRoleBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCreateRoleUnauthorized creates a CreateRoleUnauthorized with default headers values
func NewCreateRoleUnauthorized() *CreateRoleUnauthorized {
	return &CreateRoleUnauthorized{}
}

/*
CreateRoleUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type CreateRoleUnauthorized struct {
}

// IsSuccess returns true when this create role unauthorized response has a 2xx status code
func (o *CreateRoleUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role unauthorized response has a 3xx status code
func (o *CreateRoleUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role unauthorized response has a 4xx status code
func (o *CreateRoleUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this create role unauthorized response has a 5xx status code
func (o *CreateRoleUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this create role unauthorized response a status code equal to that given
func (o *CreateRoleUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the create role unauthorized response
func (o *CreateRoleUnauthorized) Code() int {
	return 401
}

func (o *CreateRoleUnauthorized) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleUnauthorized ", 401)
}

func (o *CreateRoleUnauthorized) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleUnauthorized ", 401)
}

func (o *CreateRoleUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewCreateRoleForbidden creates a CreateRoleForbidden with default headers values
func NewCreateRoleForbidden() *CreateRoleForbidden {
	return &CreateRoleForbidden{}
}

/*
CreateRoleForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type CreateRoleForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this create role forbidden response has a 2xx status code
func (o *CreateRoleForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role forbidden response has a 3xx status code
func (o *CreateRoleForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role forbidden response has a 4xx status code
func (o *CreateRoleForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this create role forbidden response has a 5xx status code
func (o *CreateRoleForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this create role forbidden response a status code equal to that given
func (o *CreateRoleForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the create role forbidden response
func (o *CreateRoleForbidden) Code() int {
	return 403
}

func (o *CreateRoleForbidden) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleForbidden  %+v", 403, o.Payload)
}

func (o *CreateRoleForbidden) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleForbidden  %+v", 403, o.Payload)
}

func (o *CreateRoleForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *CreateRoleForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCreateRoleConflict creates a CreateRoleConflict with default headers values
func NewCreateRoleConflict() *CreateRoleConflict {
	return &CreateRoleConflict{}
}

/*
CreateRoleConflict describes a response with status code 409, with default header values.

Role already exists
*/
type CreateRoleConflict struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this create role conflict response has a 2xx status code
func (o *CreateRoleConflict) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role conflict response has a 3xx status code
func (o *CreateRoleConflict) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role conflict response has a 4xx status code
func (o *CreateRoleConflict) IsClientError() bool {
	return true
}

// IsServerError returns true when this create role conflict response has a 5xx status code
func (o *CreateRoleConflict) IsServerError() bool {
	return false
}

// IsCode returns true when this create role conflict response a status code equal to that given
func (o *CreateRoleConflict) IsCode(code int) bool {
	return code == 409
}

// Code gets the status code for the create role conflict response
func (o *CreateRoleConflict) Code() int {
	return 409
}

func (o *CreateRoleConflict) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleConflict  %+v", 409, o.Payload)
}

func (o *CreateRoleConflict) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleConflict  %+v", 409, o.Payload)
}

func (o *CreateRoleConflict) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *CreateRoleConflict) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCreateRoleUnprocessableEntity creates a CreateRoleUnprocessableEntity with default headers values
func NewCreateRoleUnprocessableEntity() *CreateRoleUnprocessableEntity {
	return &CreateRoleUnprocessableEntity{}
}

/*
CreateRoleUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type CreateRoleUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this create role unprocessable entity response has a 2xx status code
func (o *CreateRoleUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role unprocessable entity response has a 3xx status code
func (o *CreateRoleUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role unprocessable entity response has a 4xx status code
func (o *CreateRoleUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this create role unprocessable entity response has a 5xx status code
func (o *CreateRoleUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this create role unprocessable entity response a status code equal to that given
func (o *CreateRoleUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the create role unprocessable entity response
func (o *CreateRoleUnprocessableEntity) Code() int {
	return 422
}

func (o *CreateRoleUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *CreateRoleUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *CreateRoleUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *CreateRoleUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCreateRoleInternalServerError creates a CreateRoleInternalServerError with default headers values
func NewCreateRoleInternalServerError() *CreateRoleInternalServerError {
	return &CreateRoleInternalServerError{}
}

/*
CreateRoleInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type CreateRoleInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this create role internal server error response has a 2xx status code
func (o *CreateRoleInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this create role internal server error response has a 3xx status code
func (o *CreateRoleInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this create role internal server error response has a 4xx status code
func (o *CreateRoleInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this create role internal server error response has a 5xx status code
func (o *CreateRoleInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this create role internal server error response a status code equal to that given
func (o *CreateRoleInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the create role internal server error response
func (o *CreateRoleInternalServerError) Code() int {
	return 500
}

func (o *CreateRoleInternalServerError) Error() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleInternalServerError  %+v", 500, o.Payload)
}

func (o *CreateRoleInternalServerError) String() string {
	return fmt.Sprintf("[POST /authz/roles][%d] createRoleInternalServerError  %+v", 500, o.Payload)
}

func (o *CreateRoleInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *CreateRoleInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

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

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/liutizhong/weaviate/entities/models"
)

// SchemaObjectsPropertiesAddReader is a Reader for the SchemaObjectsPropertiesAdd structure.
type SchemaObjectsPropertiesAddReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *SchemaObjectsPropertiesAddReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewSchemaObjectsPropertiesAddOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewSchemaObjectsPropertiesAddUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewSchemaObjectsPropertiesAddForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewSchemaObjectsPropertiesAddUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewSchemaObjectsPropertiesAddInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewSchemaObjectsPropertiesAddOK creates a SchemaObjectsPropertiesAddOK with default headers values
func NewSchemaObjectsPropertiesAddOK() *SchemaObjectsPropertiesAddOK {
	return &SchemaObjectsPropertiesAddOK{}
}

/*
SchemaObjectsPropertiesAddOK describes a response with status code 200, with default header values.

Added the property.
*/
type SchemaObjectsPropertiesAddOK struct {
	Payload *models.Property
}

// IsSuccess returns true when this schema objects properties add o k response has a 2xx status code
func (o *SchemaObjectsPropertiesAddOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this schema objects properties add o k response has a 3xx status code
func (o *SchemaObjectsPropertiesAddOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects properties add o k response has a 4xx status code
func (o *SchemaObjectsPropertiesAddOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this schema objects properties add o k response has a 5xx status code
func (o *SchemaObjectsPropertiesAddOK) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects properties add o k response a status code equal to that given
func (o *SchemaObjectsPropertiesAddOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the schema objects properties add o k response
func (o *SchemaObjectsPropertiesAddOK) Code() int {
	return 200
}

func (o *SchemaObjectsPropertiesAddOK) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddOK  %+v", 200, o.Payload)
}

func (o *SchemaObjectsPropertiesAddOK) String() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddOK  %+v", 200, o.Payload)
}

func (o *SchemaObjectsPropertiesAddOK) GetPayload() *models.Property {
	return o.Payload
}

func (o *SchemaObjectsPropertiesAddOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Property)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsPropertiesAddUnauthorized creates a SchemaObjectsPropertiesAddUnauthorized with default headers values
func NewSchemaObjectsPropertiesAddUnauthorized() *SchemaObjectsPropertiesAddUnauthorized {
	return &SchemaObjectsPropertiesAddUnauthorized{}
}

/*
SchemaObjectsPropertiesAddUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type SchemaObjectsPropertiesAddUnauthorized struct {
}

// IsSuccess returns true when this schema objects properties add unauthorized response has a 2xx status code
func (o *SchemaObjectsPropertiesAddUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects properties add unauthorized response has a 3xx status code
func (o *SchemaObjectsPropertiesAddUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects properties add unauthorized response has a 4xx status code
func (o *SchemaObjectsPropertiesAddUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects properties add unauthorized response has a 5xx status code
func (o *SchemaObjectsPropertiesAddUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects properties add unauthorized response a status code equal to that given
func (o *SchemaObjectsPropertiesAddUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the schema objects properties add unauthorized response
func (o *SchemaObjectsPropertiesAddUnauthorized) Code() int {
	return 401
}

func (o *SchemaObjectsPropertiesAddUnauthorized) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddUnauthorized ", 401)
}

func (o *SchemaObjectsPropertiesAddUnauthorized) String() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddUnauthorized ", 401)
}

func (o *SchemaObjectsPropertiesAddUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewSchemaObjectsPropertiesAddForbidden creates a SchemaObjectsPropertiesAddForbidden with default headers values
func NewSchemaObjectsPropertiesAddForbidden() *SchemaObjectsPropertiesAddForbidden {
	return &SchemaObjectsPropertiesAddForbidden{}
}

/*
SchemaObjectsPropertiesAddForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type SchemaObjectsPropertiesAddForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects properties add forbidden response has a 2xx status code
func (o *SchemaObjectsPropertiesAddForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects properties add forbidden response has a 3xx status code
func (o *SchemaObjectsPropertiesAddForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects properties add forbidden response has a 4xx status code
func (o *SchemaObjectsPropertiesAddForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects properties add forbidden response has a 5xx status code
func (o *SchemaObjectsPropertiesAddForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects properties add forbidden response a status code equal to that given
func (o *SchemaObjectsPropertiesAddForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the schema objects properties add forbidden response
func (o *SchemaObjectsPropertiesAddForbidden) Code() int {
	return 403
}

func (o *SchemaObjectsPropertiesAddForbidden) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddForbidden  %+v", 403, o.Payload)
}

func (o *SchemaObjectsPropertiesAddForbidden) String() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddForbidden  %+v", 403, o.Payload)
}

func (o *SchemaObjectsPropertiesAddForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsPropertiesAddForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsPropertiesAddUnprocessableEntity creates a SchemaObjectsPropertiesAddUnprocessableEntity with default headers values
func NewSchemaObjectsPropertiesAddUnprocessableEntity() *SchemaObjectsPropertiesAddUnprocessableEntity {
	return &SchemaObjectsPropertiesAddUnprocessableEntity{}
}

/*
SchemaObjectsPropertiesAddUnprocessableEntity describes a response with status code 422, with default header values.

Invalid property.
*/
type SchemaObjectsPropertiesAddUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects properties add unprocessable entity response has a 2xx status code
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects properties add unprocessable entity response has a 3xx status code
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects properties add unprocessable entity response has a 4xx status code
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects properties add unprocessable entity response has a 5xx status code
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects properties add unprocessable entity response a status code equal to that given
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the schema objects properties add unprocessable entity response
func (o *SchemaObjectsPropertiesAddUnprocessableEntity) Code() int {
	return 422
}

func (o *SchemaObjectsPropertiesAddUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *SchemaObjectsPropertiesAddUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *SchemaObjectsPropertiesAddUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsPropertiesAddUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsPropertiesAddInternalServerError creates a SchemaObjectsPropertiesAddInternalServerError with default headers values
func NewSchemaObjectsPropertiesAddInternalServerError() *SchemaObjectsPropertiesAddInternalServerError {
	return &SchemaObjectsPropertiesAddInternalServerError{}
}

/*
SchemaObjectsPropertiesAddInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type SchemaObjectsPropertiesAddInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects properties add internal server error response has a 2xx status code
func (o *SchemaObjectsPropertiesAddInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects properties add internal server error response has a 3xx status code
func (o *SchemaObjectsPropertiesAddInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects properties add internal server error response has a 4xx status code
func (o *SchemaObjectsPropertiesAddInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this schema objects properties add internal server error response has a 5xx status code
func (o *SchemaObjectsPropertiesAddInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this schema objects properties add internal server error response a status code equal to that given
func (o *SchemaObjectsPropertiesAddInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the schema objects properties add internal server error response
func (o *SchemaObjectsPropertiesAddInternalServerError) Code() int {
	return 500
}

func (o *SchemaObjectsPropertiesAddInternalServerError) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaObjectsPropertiesAddInternalServerError) String() string {
	return fmt.Sprintf("[POST /schema/{className}/properties][%d] schemaObjectsPropertiesAddInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaObjectsPropertiesAddInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsPropertiesAddInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

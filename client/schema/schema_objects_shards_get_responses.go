//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/entities/models"
)

// SchemaObjectsShardsGetReader is a Reader for the SchemaObjectsShardsGet structure.
type SchemaObjectsShardsGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *SchemaObjectsShardsGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewSchemaObjectsShardsGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewSchemaObjectsShardsGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewSchemaObjectsShardsGetForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewSchemaObjectsShardsGetNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewSchemaObjectsShardsGetInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewSchemaObjectsShardsGetOK creates a SchemaObjectsShardsGetOK with default headers values
func NewSchemaObjectsShardsGetOK() *SchemaObjectsShardsGetOK {
	return &SchemaObjectsShardsGetOK{}
}

/*
SchemaObjectsShardsGetOK describes a response with status code 200, with default header values.

Found the status of the shards, returned as body
*/
type SchemaObjectsShardsGetOK struct {
	Payload models.ShardStatusList
}

// IsSuccess returns true when this schema objects shards get o k response has a 2xx status code
func (o *SchemaObjectsShardsGetOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this schema objects shards get o k response has a 3xx status code
func (o *SchemaObjectsShardsGetOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects shards get o k response has a 4xx status code
func (o *SchemaObjectsShardsGetOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this schema objects shards get o k response has a 5xx status code
func (o *SchemaObjectsShardsGetOK) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects shards get o k response a status code equal to that given
func (o *SchemaObjectsShardsGetOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the schema objects shards get o k response
func (o *SchemaObjectsShardsGetOK) Code() int {
	return 200
}

func (o *SchemaObjectsShardsGetOK) Error() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetOK  %+v", 200, o.Payload)
}

func (o *SchemaObjectsShardsGetOK) String() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetOK  %+v", 200, o.Payload)
}

func (o *SchemaObjectsShardsGetOK) GetPayload() models.ShardStatusList {
	return o.Payload
}

func (o *SchemaObjectsShardsGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsShardsGetUnauthorized creates a SchemaObjectsShardsGetUnauthorized with default headers values
func NewSchemaObjectsShardsGetUnauthorized() *SchemaObjectsShardsGetUnauthorized {
	return &SchemaObjectsShardsGetUnauthorized{}
}

/*
SchemaObjectsShardsGetUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type SchemaObjectsShardsGetUnauthorized struct {
}

// IsSuccess returns true when this schema objects shards get unauthorized response has a 2xx status code
func (o *SchemaObjectsShardsGetUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects shards get unauthorized response has a 3xx status code
func (o *SchemaObjectsShardsGetUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects shards get unauthorized response has a 4xx status code
func (o *SchemaObjectsShardsGetUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects shards get unauthorized response has a 5xx status code
func (o *SchemaObjectsShardsGetUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects shards get unauthorized response a status code equal to that given
func (o *SchemaObjectsShardsGetUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the schema objects shards get unauthorized response
func (o *SchemaObjectsShardsGetUnauthorized) Code() int {
	return 401
}

func (o *SchemaObjectsShardsGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetUnauthorized ", 401)
}

func (o *SchemaObjectsShardsGetUnauthorized) String() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetUnauthorized ", 401)
}

func (o *SchemaObjectsShardsGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewSchemaObjectsShardsGetForbidden creates a SchemaObjectsShardsGetForbidden with default headers values
func NewSchemaObjectsShardsGetForbidden() *SchemaObjectsShardsGetForbidden {
	return &SchemaObjectsShardsGetForbidden{}
}

/*
SchemaObjectsShardsGetForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type SchemaObjectsShardsGetForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects shards get forbidden response has a 2xx status code
func (o *SchemaObjectsShardsGetForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects shards get forbidden response has a 3xx status code
func (o *SchemaObjectsShardsGetForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects shards get forbidden response has a 4xx status code
func (o *SchemaObjectsShardsGetForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects shards get forbidden response has a 5xx status code
func (o *SchemaObjectsShardsGetForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects shards get forbidden response a status code equal to that given
func (o *SchemaObjectsShardsGetForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the schema objects shards get forbidden response
func (o *SchemaObjectsShardsGetForbidden) Code() int {
	return 403
}

func (o *SchemaObjectsShardsGetForbidden) Error() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetForbidden  %+v", 403, o.Payload)
}

func (o *SchemaObjectsShardsGetForbidden) String() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetForbidden  %+v", 403, o.Payload)
}

func (o *SchemaObjectsShardsGetForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsShardsGetForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsShardsGetNotFound creates a SchemaObjectsShardsGetNotFound with default headers values
func NewSchemaObjectsShardsGetNotFound() *SchemaObjectsShardsGetNotFound {
	return &SchemaObjectsShardsGetNotFound{}
}

/*
SchemaObjectsShardsGetNotFound describes a response with status code 404, with default header values.

This class does not exist
*/
type SchemaObjectsShardsGetNotFound struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects shards get not found response has a 2xx status code
func (o *SchemaObjectsShardsGetNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects shards get not found response has a 3xx status code
func (o *SchemaObjectsShardsGetNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects shards get not found response has a 4xx status code
func (o *SchemaObjectsShardsGetNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this schema objects shards get not found response has a 5xx status code
func (o *SchemaObjectsShardsGetNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this schema objects shards get not found response a status code equal to that given
func (o *SchemaObjectsShardsGetNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the schema objects shards get not found response
func (o *SchemaObjectsShardsGetNotFound) Code() int {
	return 404
}

func (o *SchemaObjectsShardsGetNotFound) Error() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetNotFound  %+v", 404, o.Payload)
}

func (o *SchemaObjectsShardsGetNotFound) String() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetNotFound  %+v", 404, o.Payload)
}

func (o *SchemaObjectsShardsGetNotFound) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsShardsGetNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsShardsGetInternalServerError creates a SchemaObjectsShardsGetInternalServerError with default headers values
func NewSchemaObjectsShardsGetInternalServerError() *SchemaObjectsShardsGetInternalServerError {
	return &SchemaObjectsShardsGetInternalServerError{}
}

/*
SchemaObjectsShardsGetInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type SchemaObjectsShardsGetInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this schema objects shards get internal server error response has a 2xx status code
func (o *SchemaObjectsShardsGetInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this schema objects shards get internal server error response has a 3xx status code
func (o *SchemaObjectsShardsGetInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this schema objects shards get internal server error response has a 4xx status code
func (o *SchemaObjectsShardsGetInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this schema objects shards get internal server error response has a 5xx status code
func (o *SchemaObjectsShardsGetInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this schema objects shards get internal server error response a status code equal to that given
func (o *SchemaObjectsShardsGetInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the schema objects shards get internal server error response
func (o *SchemaObjectsShardsGetInternalServerError) Code() int {
	return 500
}

func (o *SchemaObjectsShardsGetInternalServerError) Error() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaObjectsShardsGetInternalServerError) String() string {
	return fmt.Sprintf("[GET /schema/{className}/shards][%d] schemaObjectsShardsGetInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaObjectsShardsGetInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsShardsGetInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

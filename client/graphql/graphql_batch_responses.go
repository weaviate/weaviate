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

package graphql

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// GraphqlBatchReader is a Reader for the GraphqlBatch structure.
type GraphqlBatchReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GraphqlBatchReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewGraphqlBatchOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewGraphqlBatchUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewGraphqlBatchForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewGraphqlBatchUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewGraphqlBatchInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewGraphqlBatchOK creates a GraphqlBatchOK with default headers values
func NewGraphqlBatchOK() *GraphqlBatchOK {
	return &GraphqlBatchOK{}
}

/*
GraphqlBatchOK handles this case with default header values.

Successful query (with select).
*/
type GraphqlBatchOK struct {
	Payload models.GraphQLResponses
}

func (o *GraphqlBatchOK) Error() string {
	return fmt.Sprintf("[POST /graphql/batch][%d] graphqlBatchOK  %+v", 200, o.Payload)
}

func (o *GraphqlBatchOK) GetPayload() models.GraphQLResponses {
	return o.Payload
}

func (o *GraphqlBatchOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGraphqlBatchUnauthorized creates a GraphqlBatchUnauthorized with default headers values
func NewGraphqlBatchUnauthorized() *GraphqlBatchUnauthorized {
	return &GraphqlBatchUnauthorized{}
}

/*
GraphqlBatchUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type GraphqlBatchUnauthorized struct {
}

func (o *GraphqlBatchUnauthorized) Error() string {
	return fmt.Sprintf("[POST /graphql/batch][%d] graphqlBatchUnauthorized ", 401)
}

func (o *GraphqlBatchUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewGraphqlBatchForbidden creates a GraphqlBatchForbidden with default headers values
func NewGraphqlBatchForbidden() *GraphqlBatchForbidden {
	return &GraphqlBatchForbidden{}
}

/*
GraphqlBatchForbidden handles this case with default header values.

Forbidden
*/
type GraphqlBatchForbidden struct {
	Payload *models.ErrorResponse
}

func (o *GraphqlBatchForbidden) Error() string {
	return fmt.Sprintf("[POST /graphql/batch][%d] graphqlBatchForbidden  %+v", 403, o.Payload)
}

func (o *GraphqlBatchForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *GraphqlBatchForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGraphqlBatchUnprocessableEntity creates a GraphqlBatchUnprocessableEntity with default headers values
func NewGraphqlBatchUnprocessableEntity() *GraphqlBatchUnprocessableEntity {
	return &GraphqlBatchUnprocessableEntity{}
}

/*
GraphqlBatchUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type GraphqlBatchUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *GraphqlBatchUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /graphql/batch][%d] graphqlBatchUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *GraphqlBatchUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *GraphqlBatchUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGraphqlBatchInternalServerError creates a GraphqlBatchInternalServerError with default headers values
func NewGraphqlBatchInternalServerError() *GraphqlBatchInternalServerError {
	return &GraphqlBatchInternalServerError{}
}

/*
GraphqlBatchInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type GraphqlBatchInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *GraphqlBatchInternalServerError) Error() string {
	return fmt.Sprintf("[POST /graphql/batch][%d] graphqlBatchInternalServerError  %+v", 500, o.Payload)
}

func (o *GraphqlBatchInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *GraphqlBatchInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

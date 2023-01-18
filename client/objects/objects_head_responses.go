//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

// ObjectsHeadReader is a Reader for the ObjectsHead structure.
type ObjectsHeadReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsHeadReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewObjectsHeadNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsHeadUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsHeadForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsHeadNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsHeadInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewObjectsHeadNoContent creates a ObjectsHeadNoContent with default headers values
func NewObjectsHeadNoContent() *ObjectsHeadNoContent {
	return &ObjectsHeadNoContent{}
}

/*
ObjectsHeadNoContent handles this case with default header values.

Object exists.
*/
type ObjectsHeadNoContent struct {
}

func (o *ObjectsHeadNoContent) Error() string {
	return fmt.Sprintf("[HEAD /objects/{id}][%d] objectsHeadNoContent ", 204)
}

func (o *ObjectsHeadNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsHeadUnauthorized creates a ObjectsHeadUnauthorized with default headers values
func NewObjectsHeadUnauthorized() *ObjectsHeadUnauthorized {
	return &ObjectsHeadUnauthorized{}
}

/*
ObjectsHeadUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsHeadUnauthorized struct {
}

func (o *ObjectsHeadUnauthorized) Error() string {
	return fmt.Sprintf("[HEAD /objects/{id}][%d] objectsHeadUnauthorized ", 401)
}

func (o *ObjectsHeadUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsHeadForbidden creates a ObjectsHeadForbidden with default headers values
func NewObjectsHeadForbidden() *ObjectsHeadForbidden {
	return &ObjectsHeadForbidden{}
}

/*
ObjectsHeadForbidden handles this case with default header values.

Forbidden
*/
type ObjectsHeadForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsHeadForbidden) Error() string {
	return fmt.Sprintf("[HEAD /objects/{id}][%d] objectsHeadForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsHeadForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsHeadForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsHeadNotFound creates a ObjectsHeadNotFound with default headers values
func NewObjectsHeadNotFound() *ObjectsHeadNotFound {
	return &ObjectsHeadNotFound{}
}

/*
ObjectsHeadNotFound handles this case with default header values.

Object doesn't exist.
*/
type ObjectsHeadNotFound struct {
}

func (o *ObjectsHeadNotFound) Error() string {
	return fmt.Sprintf("[HEAD /objects/{id}][%d] objectsHeadNotFound ", 404)
}

func (o *ObjectsHeadNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsHeadInternalServerError creates a ObjectsHeadInternalServerError with default headers values
func NewObjectsHeadInternalServerError() *ObjectsHeadInternalServerError {
	return &ObjectsHeadInternalServerError{}
}

/*
ObjectsHeadInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsHeadInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsHeadInternalServerError) Error() string {
	return fmt.Sprintf("[HEAD /objects/{id}][%d] objectsHeadInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsHeadInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsHeadInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

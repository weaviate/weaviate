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

// ObjectsCreateReader is a Reader for the ObjectsCreate structure.
type ObjectsCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewObjectsCreateOK creates a ObjectsCreateOK with default headers values
func NewObjectsCreateOK() *ObjectsCreateOK {
	return &ObjectsCreateOK{}
}

/*
ObjectsCreateOK handles this case with default header values.

Object created.
*/
type ObjectsCreateOK struct {
	Payload *models.Object
}

func (o *ObjectsCreateOK) Error() string {
	return fmt.Sprintf("[POST /objects][%d] objectsCreateOK  %+v", 200, o.Payload)
}

func (o *ObjectsCreateOK) GetPayload() *models.Object {
	return o.Payload
}

func (o *ObjectsCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Object)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsCreateUnauthorized creates a ObjectsCreateUnauthorized with default headers values
func NewObjectsCreateUnauthorized() *ObjectsCreateUnauthorized {
	return &ObjectsCreateUnauthorized{}
}

/*
ObjectsCreateUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsCreateUnauthorized struct {
}

func (o *ObjectsCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /objects][%d] objectsCreateUnauthorized ", 401)
}

func (o *ObjectsCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsCreateForbidden creates a ObjectsCreateForbidden with default headers values
func NewObjectsCreateForbidden() *ObjectsCreateForbidden {
	return &ObjectsCreateForbidden{}
}

/*
ObjectsCreateForbidden handles this case with default header values.

Forbidden
*/
type ObjectsCreateForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /objects][%d] objectsCreateForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsCreateForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsCreateUnprocessableEntity creates a ObjectsCreateUnprocessableEntity with default headers values
func NewObjectsCreateUnprocessableEntity() *ObjectsCreateUnprocessableEntity {
	return &ObjectsCreateUnprocessableEntity{}
}

/*
ObjectsCreateUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type ObjectsCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /objects][%d] objectsCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsCreateUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsCreateInternalServerError creates a ObjectsCreateInternalServerError with default headers values
func NewObjectsCreateInternalServerError() *ObjectsCreateInternalServerError {
	return &ObjectsCreateInternalServerError{}
}

/*
ObjectsCreateInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /objects][%d] objectsCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsCreateInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

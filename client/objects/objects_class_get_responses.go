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

	"github.com/semi-technologies/weaviate/models"
)

// ObjectsClassGetReader is a Reader for the ObjectsClassGet structure.
type ObjectsClassGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewObjectsClassGetBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewObjectsClassGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassGetForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassGetNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassGetInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewObjectsClassGetOK creates a ObjectsClassGetOK with default headers values
func NewObjectsClassGetOK() *ObjectsClassGetOK {
	return &ObjectsClassGetOK{}
}

/*
ObjectsClassGetOK handles this case with default header values.

Successful response.
*/
type ObjectsClassGetOK struct {
	Payload *models.Object
}

func (o *ObjectsClassGetOK) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetOK  %+v", 200, o.Payload)
}

func (o *ObjectsClassGetOK) GetPayload() *models.Object {
	return o.Payload
}

func (o *ObjectsClassGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Object)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassGetBadRequest creates a ObjectsClassGetBadRequest with default headers values
func NewObjectsClassGetBadRequest() *ObjectsClassGetBadRequest {
	return &ObjectsClassGetBadRequest{}
}

/*
ObjectsClassGetBadRequest handles this case with default header values.

Malformed request.
*/
type ObjectsClassGetBadRequest struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassGetBadRequest) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsClassGetBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassGetBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassGetUnauthorized creates a ObjectsClassGetUnauthorized with default headers values
func NewObjectsClassGetUnauthorized() *ObjectsClassGetUnauthorized {
	return &ObjectsClassGetUnauthorized{}
}

/*
ObjectsClassGetUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassGetUnauthorized struct {
}

func (o *ObjectsClassGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetUnauthorized ", 401)
}

func (o *ObjectsClassGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassGetForbidden creates a ObjectsClassGetForbidden with default headers values
func NewObjectsClassGetForbidden() *ObjectsClassGetForbidden {
	return &ObjectsClassGetForbidden{}
}

/*
ObjectsClassGetForbidden handles this case with default header values.

Forbidden
*/
type ObjectsClassGetForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassGetForbidden) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassGetForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassGetForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassGetNotFound creates a ObjectsClassGetNotFound with default headers values
func NewObjectsClassGetNotFound() *ObjectsClassGetNotFound {
	return &ObjectsClassGetNotFound{}
}

/*
ObjectsClassGetNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type ObjectsClassGetNotFound struct {
}

func (o *ObjectsClassGetNotFound) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetNotFound ", 404)
}

func (o *ObjectsClassGetNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassGetInternalServerError creates a ObjectsClassGetInternalServerError with default headers values
func NewObjectsClassGetInternalServerError() *ObjectsClassGetInternalServerError {
	return &ObjectsClassGetInternalServerError{}
}

/*
ObjectsClassGetInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassGetInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassGetInternalServerError) Error() string {
	return fmt.Sprintf("[GET /objects/{className}/{id}][%d] objectsClassGetInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassGetInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassGetInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

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

// ObjectsListReader is a Reader for the ObjectsList structure.
type ObjectsListReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsListReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsListOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewObjectsListBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewObjectsListUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsListForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsListNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsListUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsListInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewObjectsListOK creates a ObjectsListOK with default headers values
func NewObjectsListOK() *ObjectsListOK {
	return &ObjectsListOK{}
}

/*ObjectsListOK handles this case with default header values.

Successful response.
*/
type ObjectsListOK struct {
	Payload *models.ObjectsListResponse
}

func (o *ObjectsListOK) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListOK  %+v", 200, o.Payload)
}

func (o *ObjectsListOK) GetPayload() *models.ObjectsListResponse {
	return o.Payload
}

func (o *ObjectsListOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ObjectsListResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsListBadRequest creates a ObjectsListBadRequest with default headers values
func NewObjectsListBadRequest() *ObjectsListBadRequest {
	return &ObjectsListBadRequest{}
}

/*ObjectsListBadRequest handles this case with default header values.

Malformed request.
*/
type ObjectsListBadRequest struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsListBadRequest) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListBadRequest  %+v", 400, o.Payload)
}

func (o *ObjectsListBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsListBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsListUnauthorized creates a ObjectsListUnauthorized with default headers values
func NewObjectsListUnauthorized() *ObjectsListUnauthorized {
	return &ObjectsListUnauthorized{}
}

/*ObjectsListUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsListUnauthorized struct {
}

func (o *ObjectsListUnauthorized) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListUnauthorized ", 401)
}

func (o *ObjectsListUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsListForbidden creates a ObjectsListForbidden with default headers values
func NewObjectsListForbidden() *ObjectsListForbidden {
	return &ObjectsListForbidden{}
}

/*ObjectsListForbidden handles this case with default header values.

Forbidden
*/
type ObjectsListForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsListForbidden) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsListForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsListForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsListNotFound creates a ObjectsListNotFound with default headers values
func NewObjectsListNotFound() *ObjectsListNotFound {
	return &ObjectsListNotFound{}
}

/*ObjectsListNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type ObjectsListNotFound struct {
}

func (o *ObjectsListNotFound) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListNotFound ", 404)
}

func (o *ObjectsListNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsListUnprocessableEntity creates a ObjectsListUnprocessableEntity with default headers values
func NewObjectsListUnprocessableEntity() *ObjectsListUnprocessableEntity {
	return &ObjectsListUnprocessableEntity{}
}

/*ObjectsListUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type ObjectsListUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsListUnprocessableEntity) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsListUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsListUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsListInternalServerError creates a ObjectsListInternalServerError with default headers values
func NewObjectsListInternalServerError() *ObjectsListInternalServerError {
	return &ObjectsListInternalServerError{}
}

/*ObjectsListInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsListInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsListInternalServerError) Error() string {
	return fmt.Sprintf("[GET /objects][%d] objectsListInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsListInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsListInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

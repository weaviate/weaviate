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

	"github.com/semi-technologies/weaviate/entities/models"
)

// ObjectsClassReferencesCreateReader is a Reader for the ObjectsClassReferencesCreate structure.
type ObjectsClassReferencesCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassReferencesCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassReferencesCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsClassReferencesCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassReferencesCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassReferencesCreateNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassReferencesCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassReferencesCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewObjectsClassReferencesCreateOK creates a ObjectsClassReferencesCreateOK with default headers values
func NewObjectsClassReferencesCreateOK() *ObjectsClassReferencesCreateOK {
	return &ObjectsClassReferencesCreateOK{}
}

/*ObjectsClassReferencesCreateOK handles this case with default header values.

Successfully added the reference.
*/
type ObjectsClassReferencesCreateOK struct {
}

func (o *ObjectsClassReferencesCreateOK) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateOK ", 200)
}

func (o *ObjectsClassReferencesCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassReferencesCreateUnauthorized creates a ObjectsClassReferencesCreateUnauthorized with default headers values
func NewObjectsClassReferencesCreateUnauthorized() *ObjectsClassReferencesCreateUnauthorized {
	return &ObjectsClassReferencesCreateUnauthorized{}
}

/*ObjectsClassReferencesCreateUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassReferencesCreateUnauthorized struct {
}

func (o *ObjectsClassReferencesCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnauthorized ", 401)
}

func (o *ObjectsClassReferencesCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassReferencesCreateForbidden creates a ObjectsClassReferencesCreateForbidden with default headers values
func NewObjectsClassReferencesCreateForbidden() *ObjectsClassReferencesCreateForbidden {
	return &ObjectsClassReferencesCreateForbidden{}
}

/*ObjectsClassReferencesCreateForbidden handles this case with default header values.

Forbidden
*/
type ObjectsClassReferencesCreateForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassReferencesCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassReferencesCreateForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesCreateNotFound creates a ObjectsClassReferencesCreateNotFound with default headers values
func NewObjectsClassReferencesCreateNotFound() *ObjectsClassReferencesCreateNotFound {
	return &ObjectsClassReferencesCreateNotFound{}
}

/*ObjectsClassReferencesCreateNotFound handles this case with default header values.

Source object doesn't exist.
*/
type ObjectsClassReferencesCreateNotFound struct {
}

func (o *ObjectsClassReferencesCreateNotFound) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateNotFound ", 404)
}

func (o *ObjectsClassReferencesCreateNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassReferencesCreateUnprocessableEntity creates a ObjectsClassReferencesCreateUnprocessableEntity with default headers values
func NewObjectsClassReferencesCreateUnprocessableEntity() *ObjectsClassReferencesCreateUnprocessableEntity {
	return &ObjectsClassReferencesCreateUnprocessableEntity{}
}

/*ObjectsClassReferencesCreateUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?
*/
type ObjectsClassReferencesCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassReferencesCreateInternalServerError creates a ObjectsClassReferencesCreateInternalServerError with default headers values
func NewObjectsClassReferencesCreateInternalServerError() *ObjectsClassReferencesCreateInternalServerError {
	return &ObjectsClassReferencesCreateInternalServerError{}
}

/*ObjectsClassReferencesCreateInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassReferencesCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassReferencesCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /objects/{className}/{id}/references/{propertyName}][%d] objectsClassReferencesCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassReferencesCreateInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassReferencesCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

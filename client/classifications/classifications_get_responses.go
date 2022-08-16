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

package classifications

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ClassificationsGetReader is a Reader for the ClassificationsGet structure.
type ClassificationsGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ClassificationsGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewClassificationsGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewClassificationsGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewClassificationsGetForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewClassificationsGetNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewClassificationsGetInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewClassificationsGetOK creates a ClassificationsGetOK with default headers values
func NewClassificationsGetOK() *ClassificationsGetOK {
	return &ClassificationsGetOK{}
}

/*ClassificationsGetOK handles this case with default header values.

Found the classification, returned as body
*/
type ClassificationsGetOK struct {
	Payload *models.Classification
}

func (o *ClassificationsGetOK) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetOK  %+v", 200, o.Payload)
}

func (o *ClassificationsGetOK) GetPayload() *models.Classification {
	return o.Payload
}

func (o *ClassificationsGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Classification)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewClassificationsGetUnauthorized creates a ClassificationsGetUnauthorized with default headers values
func NewClassificationsGetUnauthorized() *ClassificationsGetUnauthorized {
	return &ClassificationsGetUnauthorized{}
}

/*ClassificationsGetUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ClassificationsGetUnauthorized struct {
}

func (o *ClassificationsGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetUnauthorized ", 401)
}

func (o *ClassificationsGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewClassificationsGetForbidden creates a ClassificationsGetForbidden with default headers values
func NewClassificationsGetForbidden() *ClassificationsGetForbidden {
	return &ClassificationsGetForbidden{}
}

/*ClassificationsGetForbidden handles this case with default header values.

Forbidden
*/
type ClassificationsGetForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ClassificationsGetForbidden) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetForbidden  %+v", 403, o.Payload)
}

func (o *ClassificationsGetForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ClassificationsGetForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewClassificationsGetNotFound creates a ClassificationsGetNotFound with default headers values
func NewClassificationsGetNotFound() *ClassificationsGetNotFound {
	return &ClassificationsGetNotFound{}
}

/*ClassificationsGetNotFound handles this case with default header values.

Not Found - Classification does not exist
*/
type ClassificationsGetNotFound struct {
}

func (o *ClassificationsGetNotFound) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetNotFound ", 404)
}

func (o *ClassificationsGetNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewClassificationsGetInternalServerError creates a ClassificationsGetInternalServerError with default headers values
func NewClassificationsGetInternalServerError() *ClassificationsGetInternalServerError {
	return &ClassificationsGetInternalServerError{}
}

/*ClassificationsGetInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ClassificationsGetInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ClassificationsGetInternalServerError) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetInternalServerError  %+v", 500, o.Payload)
}

func (o *ClassificationsGetInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ClassificationsGetInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

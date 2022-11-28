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

package meta

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/semi-technologies/weaviate/models"
)

// MetaGetReader is a Reader for the MetaGet structure.
type MetaGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *MetaGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewMetaGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewMetaGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewMetaGetForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewMetaGetInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewMetaGetOK creates a MetaGetOK with default headers values
func NewMetaGetOK() *MetaGetOK {
	return &MetaGetOK{}
}

/*
MetaGetOK handles this case with default header values.

Successful response.
*/
type MetaGetOK struct {
	Payload *models.Meta
}

func (o *MetaGetOK) Error() string {
	return fmt.Sprintf("[GET /meta][%d] metaGetOK  %+v", 200, o.Payload)
}

func (o *MetaGetOK) GetPayload() *models.Meta {
	return o.Payload
}

func (o *MetaGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Meta)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewMetaGetUnauthorized creates a MetaGetUnauthorized with default headers values
func NewMetaGetUnauthorized() *MetaGetUnauthorized {
	return &MetaGetUnauthorized{}
}

/*
MetaGetUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type MetaGetUnauthorized struct {
}

func (o *MetaGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /meta][%d] metaGetUnauthorized ", 401)
}

func (o *MetaGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewMetaGetForbidden creates a MetaGetForbidden with default headers values
func NewMetaGetForbidden() *MetaGetForbidden {
	return &MetaGetForbidden{}
}

/*
MetaGetForbidden handles this case with default header values.

Forbidden
*/
type MetaGetForbidden struct {
	Payload *models.ErrorResponse
}

func (o *MetaGetForbidden) Error() string {
	return fmt.Sprintf("[GET /meta][%d] metaGetForbidden  %+v", 403, o.Payload)
}

func (o *MetaGetForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *MetaGetForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewMetaGetInternalServerError creates a MetaGetInternalServerError with default headers values
func NewMetaGetInternalServerError() *MetaGetInternalServerError {
	return &MetaGetInternalServerError{}
}

/*
MetaGetInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type MetaGetInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *MetaGetInternalServerError) Error() string {
	return fmt.Sprintf("[GET /meta][%d] metaGetInternalServerError  %+v", 500, o.Payload)
}

func (o *MetaGetInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *MetaGetInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

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

package admin

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// AdminInvertedIndexRebuildReader is a Reader for the AdminInvertedIndexRebuild structure.
type AdminInvertedIndexRebuildReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *AdminInvertedIndexRebuildReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewAdminInvertedIndexRebuildOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewAdminInvertedIndexRebuildBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewAdminInvertedIndexRebuildUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewAdminInvertedIndexRebuildForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewAdminInvertedIndexRebuildNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewAdminInvertedIndexRebuildInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewAdminInvertedIndexRebuildOK creates a AdminInvertedIndexRebuildOK with default headers values
func NewAdminInvertedIndexRebuildOK() *AdminInvertedIndexRebuildOK {
	return &AdminInvertedIndexRebuildOK{}
}

/*
AdminInvertedIndexRebuildOK describes a response with status code 200, with default header values.

Successful response.
*/
type AdminInvertedIndexRebuildOK struct {
}

// IsSuccess returns true when this admin inverted index rebuild o k response has a 2xx status code
func (o *AdminInvertedIndexRebuildOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this admin inverted index rebuild o k response has a 3xx status code
func (o *AdminInvertedIndexRebuildOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild o k response has a 4xx status code
func (o *AdminInvertedIndexRebuildOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this admin inverted index rebuild o k response has a 5xx status code
func (o *AdminInvertedIndexRebuildOK) IsServerError() bool {
	return false
}

// IsCode returns true when this admin inverted index rebuild o k response a status code equal to that given
func (o *AdminInvertedIndexRebuildOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the admin inverted index rebuild o k response
func (o *AdminInvertedIndexRebuildOK) Code() int {
	return 200
}

func (o *AdminInvertedIndexRebuildOK) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildOK ", 200)
}

func (o *AdminInvertedIndexRebuildOK) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildOK ", 200)
}

func (o *AdminInvertedIndexRebuildOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewAdminInvertedIndexRebuildBadRequest creates a AdminInvertedIndexRebuildBadRequest with default headers values
func NewAdminInvertedIndexRebuildBadRequest() *AdminInvertedIndexRebuildBadRequest {
	return &AdminInvertedIndexRebuildBadRequest{}
}

/*
AdminInvertedIndexRebuildBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type AdminInvertedIndexRebuildBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this admin inverted index rebuild bad request response has a 2xx status code
func (o *AdminInvertedIndexRebuildBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this admin inverted index rebuild bad request response has a 3xx status code
func (o *AdminInvertedIndexRebuildBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild bad request response has a 4xx status code
func (o *AdminInvertedIndexRebuildBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this admin inverted index rebuild bad request response has a 5xx status code
func (o *AdminInvertedIndexRebuildBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this admin inverted index rebuild bad request response a status code equal to that given
func (o *AdminInvertedIndexRebuildBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the admin inverted index rebuild bad request response
func (o *AdminInvertedIndexRebuildBadRequest) Code() int {
	return 400
}

func (o *AdminInvertedIndexRebuildBadRequest) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildBadRequest  %+v", 400, o.Payload)
}

func (o *AdminInvertedIndexRebuildBadRequest) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildBadRequest  %+v", 400, o.Payload)
}

func (o *AdminInvertedIndexRebuildBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *AdminInvertedIndexRebuildBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewAdminInvertedIndexRebuildUnauthorized creates a AdminInvertedIndexRebuildUnauthorized with default headers values
func NewAdminInvertedIndexRebuildUnauthorized() *AdminInvertedIndexRebuildUnauthorized {
	return &AdminInvertedIndexRebuildUnauthorized{}
}

/*
AdminInvertedIndexRebuildUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type AdminInvertedIndexRebuildUnauthorized struct {
}

// IsSuccess returns true when this admin inverted index rebuild unauthorized response has a 2xx status code
func (o *AdminInvertedIndexRebuildUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this admin inverted index rebuild unauthorized response has a 3xx status code
func (o *AdminInvertedIndexRebuildUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild unauthorized response has a 4xx status code
func (o *AdminInvertedIndexRebuildUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this admin inverted index rebuild unauthorized response has a 5xx status code
func (o *AdminInvertedIndexRebuildUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this admin inverted index rebuild unauthorized response a status code equal to that given
func (o *AdminInvertedIndexRebuildUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the admin inverted index rebuild unauthorized response
func (o *AdminInvertedIndexRebuildUnauthorized) Code() int {
	return 401
}

func (o *AdminInvertedIndexRebuildUnauthorized) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildUnauthorized ", 401)
}

func (o *AdminInvertedIndexRebuildUnauthorized) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildUnauthorized ", 401)
}

func (o *AdminInvertedIndexRebuildUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewAdminInvertedIndexRebuildForbidden creates a AdminInvertedIndexRebuildForbidden with default headers values
func NewAdminInvertedIndexRebuildForbidden() *AdminInvertedIndexRebuildForbidden {
	return &AdminInvertedIndexRebuildForbidden{}
}

/*
AdminInvertedIndexRebuildForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type AdminInvertedIndexRebuildForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this admin inverted index rebuild forbidden response has a 2xx status code
func (o *AdminInvertedIndexRebuildForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this admin inverted index rebuild forbidden response has a 3xx status code
func (o *AdminInvertedIndexRebuildForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild forbidden response has a 4xx status code
func (o *AdminInvertedIndexRebuildForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this admin inverted index rebuild forbidden response has a 5xx status code
func (o *AdminInvertedIndexRebuildForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this admin inverted index rebuild forbidden response a status code equal to that given
func (o *AdminInvertedIndexRebuildForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the admin inverted index rebuild forbidden response
func (o *AdminInvertedIndexRebuildForbidden) Code() int {
	return 403
}

func (o *AdminInvertedIndexRebuildForbidden) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildForbidden  %+v", 403, o.Payload)
}

func (o *AdminInvertedIndexRebuildForbidden) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildForbidden  %+v", 403, o.Payload)
}

func (o *AdminInvertedIndexRebuildForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *AdminInvertedIndexRebuildForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewAdminInvertedIndexRebuildNotFound creates a AdminInvertedIndexRebuildNotFound with default headers values
func NewAdminInvertedIndexRebuildNotFound() *AdminInvertedIndexRebuildNotFound {
	return &AdminInvertedIndexRebuildNotFound{}
}

/*
AdminInvertedIndexRebuildNotFound describes a response with status code 404, with default header values.

Successful query result but no resource was found.
*/
type AdminInvertedIndexRebuildNotFound struct {
}

// IsSuccess returns true when this admin inverted index rebuild not found response has a 2xx status code
func (o *AdminInvertedIndexRebuildNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this admin inverted index rebuild not found response has a 3xx status code
func (o *AdminInvertedIndexRebuildNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild not found response has a 4xx status code
func (o *AdminInvertedIndexRebuildNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this admin inverted index rebuild not found response has a 5xx status code
func (o *AdminInvertedIndexRebuildNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this admin inverted index rebuild not found response a status code equal to that given
func (o *AdminInvertedIndexRebuildNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the admin inverted index rebuild not found response
func (o *AdminInvertedIndexRebuildNotFound) Code() int {
	return 404
}

func (o *AdminInvertedIndexRebuildNotFound) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildNotFound ", 404)
}

func (o *AdminInvertedIndexRebuildNotFound) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildNotFound ", 404)
}

func (o *AdminInvertedIndexRebuildNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewAdminInvertedIndexRebuildInternalServerError creates a AdminInvertedIndexRebuildInternalServerError with default headers values
func NewAdminInvertedIndexRebuildInternalServerError() *AdminInvertedIndexRebuildInternalServerError {
	return &AdminInvertedIndexRebuildInternalServerError{}
}

/*
AdminInvertedIndexRebuildInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type AdminInvertedIndexRebuildInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this admin inverted index rebuild internal server error response has a 2xx status code
func (o *AdminInvertedIndexRebuildInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this admin inverted index rebuild internal server error response has a 3xx status code
func (o *AdminInvertedIndexRebuildInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this admin inverted index rebuild internal server error response has a 4xx status code
func (o *AdminInvertedIndexRebuildInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this admin inverted index rebuild internal server error response has a 5xx status code
func (o *AdminInvertedIndexRebuildInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this admin inverted index rebuild internal server error response a status code equal to that given
func (o *AdminInvertedIndexRebuildInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the admin inverted index rebuild internal server error response
func (o *AdminInvertedIndexRebuildInternalServerError) Code() int {
	return 500
}

func (o *AdminInvertedIndexRebuildInternalServerError) Error() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildInternalServerError  %+v", 500, o.Payload)
}

func (o *AdminInvertedIndexRebuildInternalServerError) String() string {
	return fmt.Sprintf("[POST /admin/inverted_index/rebuild/{id}][%d] adminInvertedIndexRebuildInternalServerError  %+v", 500, o.Payload)
}

func (o *AdminInvertedIndexRebuildInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *AdminInvertedIndexRebuildInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

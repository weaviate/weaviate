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

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/liutizhong/weaviate/entities/models"
)

// BackupsCreateReader is a Reader for the BackupsCreate structure.
type BackupsCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *BackupsCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewBackupsCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewBackupsCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewBackupsCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewBackupsCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewBackupsCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewBackupsCreateOK creates a BackupsCreateOK with default headers values
func NewBackupsCreateOK() *BackupsCreateOK {
	return &BackupsCreateOK{}
}

/*
BackupsCreateOK describes a response with status code 200, with default header values.

Backup create process successfully started.
*/
type BackupsCreateOK struct {
	Payload *models.BackupCreateResponse
}

// IsSuccess returns true when this backups create o k response has a 2xx status code
func (o *BackupsCreateOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this backups create o k response has a 3xx status code
func (o *BackupsCreateOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups create o k response has a 4xx status code
func (o *BackupsCreateOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups create o k response has a 5xx status code
func (o *BackupsCreateOK) IsServerError() bool {
	return false
}

// IsCode returns true when this backups create o k response a status code equal to that given
func (o *BackupsCreateOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the backups create o k response
func (o *BackupsCreateOK) Code() int {
	return 200
}

func (o *BackupsCreateOK) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateOK  %+v", 200, o.Payload)
}

func (o *BackupsCreateOK) String() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateOK  %+v", 200, o.Payload)
}

func (o *BackupsCreateOK) GetPayload() *models.BackupCreateResponse {
	return o.Payload
}

func (o *BackupsCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.BackupCreateResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateUnauthorized creates a BackupsCreateUnauthorized with default headers values
func NewBackupsCreateUnauthorized() *BackupsCreateUnauthorized {
	return &BackupsCreateUnauthorized{}
}

/*
BackupsCreateUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type BackupsCreateUnauthorized struct {
}

// IsSuccess returns true when this backups create unauthorized response has a 2xx status code
func (o *BackupsCreateUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups create unauthorized response has a 3xx status code
func (o *BackupsCreateUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups create unauthorized response has a 4xx status code
func (o *BackupsCreateUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups create unauthorized response has a 5xx status code
func (o *BackupsCreateUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this backups create unauthorized response a status code equal to that given
func (o *BackupsCreateUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the backups create unauthorized response
func (o *BackupsCreateUnauthorized) Code() int {
	return 401
}

func (o *BackupsCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateUnauthorized ", 401)
}

func (o *BackupsCreateUnauthorized) String() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateUnauthorized ", 401)
}

func (o *BackupsCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewBackupsCreateForbidden creates a BackupsCreateForbidden with default headers values
func NewBackupsCreateForbidden() *BackupsCreateForbidden {
	return &BackupsCreateForbidden{}
}

/*
BackupsCreateForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type BackupsCreateForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups create forbidden response has a 2xx status code
func (o *BackupsCreateForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups create forbidden response has a 3xx status code
func (o *BackupsCreateForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups create forbidden response has a 4xx status code
func (o *BackupsCreateForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups create forbidden response has a 5xx status code
func (o *BackupsCreateForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this backups create forbidden response a status code equal to that given
func (o *BackupsCreateForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the backups create forbidden response
func (o *BackupsCreateForbidden) Code() int {
	return 403
}

func (o *BackupsCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateForbidden  %+v", 403, o.Payload)
}

func (o *BackupsCreateForbidden) String() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateForbidden  %+v", 403, o.Payload)
}

func (o *BackupsCreateForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateUnprocessableEntity creates a BackupsCreateUnprocessableEntity with default headers values
func NewBackupsCreateUnprocessableEntity() *BackupsCreateUnprocessableEntity {
	return &BackupsCreateUnprocessableEntity{}
}

/*
BackupsCreateUnprocessableEntity describes a response with status code 422, with default header values.

Invalid backup creation attempt.
*/
type BackupsCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups create unprocessable entity response has a 2xx status code
func (o *BackupsCreateUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups create unprocessable entity response has a 3xx status code
func (o *BackupsCreateUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups create unprocessable entity response has a 4xx status code
func (o *BackupsCreateUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups create unprocessable entity response has a 5xx status code
func (o *BackupsCreateUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this backups create unprocessable entity response a status code equal to that given
func (o *BackupsCreateUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the backups create unprocessable entity response
func (o *BackupsCreateUnprocessableEntity) Code() int {
	return 422
}

func (o *BackupsCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsCreateUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsCreateUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateInternalServerError creates a BackupsCreateInternalServerError with default headers values
func NewBackupsCreateInternalServerError() *BackupsCreateInternalServerError {
	return &BackupsCreateInternalServerError{}
}

/*
BackupsCreateInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type BackupsCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups create internal server error response has a 2xx status code
func (o *BackupsCreateInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups create internal server error response has a 3xx status code
func (o *BackupsCreateInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups create internal server error response has a 4xx status code
func (o *BackupsCreateInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups create internal server error response has a 5xx status code
func (o *BackupsCreateInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this backups create internal server error response a status code equal to that given
func (o *BackupsCreateInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the backups create internal server error response
func (o *BackupsCreateInternalServerError) Code() int {
	return 500
}

func (o *BackupsCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsCreateInternalServerError) String() string {
	return fmt.Sprintf("[POST /backups/{backend}][%d] backupsCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsCreateInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

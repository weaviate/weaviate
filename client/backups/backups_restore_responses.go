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

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// BackupsRestoreReader is a Reader for the BackupsRestore structure.
type BackupsRestoreReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *BackupsRestoreReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewBackupsRestoreOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewBackupsRestoreUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewBackupsRestoreForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewBackupsRestoreNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewBackupsRestoreUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewBackupsRestoreInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewBackupsRestoreOK creates a BackupsRestoreOK with default headers values
func NewBackupsRestoreOK() *BackupsRestoreOK {
	return &BackupsRestoreOK{}
}

/*
BackupsRestoreOK describes a response with status code 200, with default header values.

Backup restoration process successfully started.
*/
type BackupsRestoreOK struct {
	Payload *models.BackupRestoreResponse
}

// IsSuccess returns true when this backups restore o k response has a 2xx status code
func (o *BackupsRestoreOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this backups restore o k response has a 3xx status code
func (o *BackupsRestoreOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore o k response has a 4xx status code
func (o *BackupsRestoreOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups restore o k response has a 5xx status code
func (o *BackupsRestoreOK) IsServerError() bool {
	return false
}

// IsCode returns true when this backups restore o k response a status code equal to that given
func (o *BackupsRestoreOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the backups restore o k response
func (o *BackupsRestoreOK) Code() int {
	return 200
}

func (o *BackupsRestoreOK) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreOK  %+v", 200, o.Payload)
}

func (o *BackupsRestoreOK) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreOK  %+v", 200, o.Payload)
}

func (o *BackupsRestoreOK) GetPayload() *models.BackupRestoreResponse {
	return o.Payload
}

func (o *BackupsRestoreOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.BackupRestoreResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsRestoreUnauthorized creates a BackupsRestoreUnauthorized with default headers values
func NewBackupsRestoreUnauthorized() *BackupsRestoreUnauthorized {
	return &BackupsRestoreUnauthorized{}
}

/*
BackupsRestoreUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type BackupsRestoreUnauthorized struct{}

// IsSuccess returns true when this backups restore unauthorized response has a 2xx status code
func (o *BackupsRestoreUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups restore unauthorized response has a 3xx status code
func (o *BackupsRestoreUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore unauthorized response has a 4xx status code
func (o *BackupsRestoreUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups restore unauthorized response has a 5xx status code
func (o *BackupsRestoreUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this backups restore unauthorized response a status code equal to that given
func (o *BackupsRestoreUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the backups restore unauthorized response
func (o *BackupsRestoreUnauthorized) Code() int {
	return 401
}

func (o *BackupsRestoreUnauthorized) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreUnauthorized ", 401)
}

func (o *BackupsRestoreUnauthorized) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreUnauthorized ", 401)
}

func (o *BackupsRestoreUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewBackupsRestoreForbidden creates a BackupsRestoreForbidden with default headers values
func NewBackupsRestoreForbidden() *BackupsRestoreForbidden {
	return &BackupsRestoreForbidden{}
}

/*
BackupsRestoreForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type BackupsRestoreForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups restore forbidden response has a 2xx status code
func (o *BackupsRestoreForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups restore forbidden response has a 3xx status code
func (o *BackupsRestoreForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore forbidden response has a 4xx status code
func (o *BackupsRestoreForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups restore forbidden response has a 5xx status code
func (o *BackupsRestoreForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this backups restore forbidden response a status code equal to that given
func (o *BackupsRestoreForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the backups restore forbidden response
func (o *BackupsRestoreForbidden) Code() int {
	return 403
}

func (o *BackupsRestoreForbidden) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreForbidden  %+v", 403, o.Payload)
}

func (o *BackupsRestoreForbidden) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreForbidden  %+v", 403, o.Payload)
}

func (o *BackupsRestoreForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsRestoreForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsRestoreNotFound creates a BackupsRestoreNotFound with default headers values
func NewBackupsRestoreNotFound() *BackupsRestoreNotFound {
	return &BackupsRestoreNotFound{}
}

/*
BackupsRestoreNotFound describes a response with status code 404, with default header values.

Not Found - Backup does not exist
*/
type BackupsRestoreNotFound struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups restore not found response has a 2xx status code
func (o *BackupsRestoreNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups restore not found response has a 3xx status code
func (o *BackupsRestoreNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore not found response has a 4xx status code
func (o *BackupsRestoreNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups restore not found response has a 5xx status code
func (o *BackupsRestoreNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this backups restore not found response a status code equal to that given
func (o *BackupsRestoreNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the backups restore not found response
func (o *BackupsRestoreNotFound) Code() int {
	return 404
}

func (o *BackupsRestoreNotFound) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreNotFound  %+v", 404, o.Payload)
}

func (o *BackupsRestoreNotFound) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreNotFound  %+v", 404, o.Payload)
}

func (o *BackupsRestoreNotFound) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsRestoreNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsRestoreUnprocessableEntity creates a BackupsRestoreUnprocessableEntity with default headers values
func NewBackupsRestoreUnprocessableEntity() *BackupsRestoreUnprocessableEntity {
	return &BackupsRestoreUnprocessableEntity{}
}

/*
BackupsRestoreUnprocessableEntity describes a response with status code 422, with default header values.

Invalid backup restoration attempt.
*/
type BackupsRestoreUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups restore unprocessable entity response has a 2xx status code
func (o *BackupsRestoreUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups restore unprocessable entity response has a 3xx status code
func (o *BackupsRestoreUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore unprocessable entity response has a 4xx status code
func (o *BackupsRestoreUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups restore unprocessable entity response has a 5xx status code
func (o *BackupsRestoreUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this backups restore unprocessable entity response a status code equal to that given
func (o *BackupsRestoreUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the backups restore unprocessable entity response
func (o *BackupsRestoreUnprocessableEntity) Code() int {
	return 422
}

func (o *BackupsRestoreUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsRestoreUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsRestoreUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsRestoreUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsRestoreInternalServerError creates a BackupsRestoreInternalServerError with default headers values
func NewBackupsRestoreInternalServerError() *BackupsRestoreInternalServerError {
	return &BackupsRestoreInternalServerError{}
}

/*
BackupsRestoreInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type BackupsRestoreInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups restore internal server error response has a 2xx status code
func (o *BackupsRestoreInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups restore internal server error response has a 3xx status code
func (o *BackupsRestoreInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups restore internal server error response has a 4xx status code
func (o *BackupsRestoreInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups restore internal server error response has a 5xx status code
func (o *BackupsRestoreInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this backups restore internal server error response a status code equal to that given
func (o *BackupsRestoreInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the backups restore internal server error response
func (o *BackupsRestoreInternalServerError) Code() int {
	return 500
}

func (o *BackupsRestoreInternalServerError) Error() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsRestoreInternalServerError) String() string {
	return fmt.Sprintf("[POST /backups/{backend}/{id}/restore][%d] backupsRestoreInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsRestoreInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsRestoreInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

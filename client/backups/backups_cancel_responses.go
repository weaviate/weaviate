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

	"github.com/weaviate/weaviate/entities/models"
)

// BackupsCancelReader is a Reader for the BackupsCancel structure.
type BackupsCancelReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *BackupsCancelReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewBackupsCancelNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewBackupsCancelUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewBackupsCancelForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewBackupsCancelUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewBackupsCancelInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewBackupsCancelNoContent creates a BackupsCancelNoContent with default headers values
func NewBackupsCancelNoContent() *BackupsCancelNoContent {
	return &BackupsCancelNoContent{}
}

/*
BackupsCancelNoContent describes a response with status code 204, with default header values.

Successfully deleted.
*/
type BackupsCancelNoContent struct {
}

// IsSuccess returns true when this backups cancel no content response has a 2xx status code
func (o *BackupsCancelNoContent) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this backups cancel no content response has a 3xx status code
func (o *BackupsCancelNoContent) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups cancel no content response has a 4xx status code
func (o *BackupsCancelNoContent) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups cancel no content response has a 5xx status code
func (o *BackupsCancelNoContent) IsServerError() bool {
	return false
}

// IsCode returns true when this backups cancel no content response a status code equal to that given
func (o *BackupsCancelNoContent) IsCode(code int) bool {
	return code == 204
}

// Code gets the status code for the backups cancel no content response
func (o *BackupsCancelNoContent) Code() int {
	return 204
}

func (o *BackupsCancelNoContent) Error() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelNoContent ", 204)
}

func (o *BackupsCancelNoContent) String() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelNoContent ", 204)
}

func (o *BackupsCancelNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewBackupsCancelUnauthorized creates a BackupsCancelUnauthorized with default headers values
func NewBackupsCancelUnauthorized() *BackupsCancelUnauthorized {
	return &BackupsCancelUnauthorized{}
}

/*
BackupsCancelUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type BackupsCancelUnauthorized struct {
}

// IsSuccess returns true when this backups cancel unauthorized response has a 2xx status code
func (o *BackupsCancelUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups cancel unauthorized response has a 3xx status code
func (o *BackupsCancelUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups cancel unauthorized response has a 4xx status code
func (o *BackupsCancelUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups cancel unauthorized response has a 5xx status code
func (o *BackupsCancelUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this backups cancel unauthorized response a status code equal to that given
func (o *BackupsCancelUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the backups cancel unauthorized response
func (o *BackupsCancelUnauthorized) Code() int {
	return 401
}

func (o *BackupsCancelUnauthorized) Error() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelUnauthorized ", 401)
}

func (o *BackupsCancelUnauthorized) String() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelUnauthorized ", 401)
}

func (o *BackupsCancelUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewBackupsCancelForbidden creates a BackupsCancelForbidden with default headers values
func NewBackupsCancelForbidden() *BackupsCancelForbidden {
	return &BackupsCancelForbidden{}
}

/*
BackupsCancelForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type BackupsCancelForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups cancel forbidden response has a 2xx status code
func (o *BackupsCancelForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups cancel forbidden response has a 3xx status code
func (o *BackupsCancelForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups cancel forbidden response has a 4xx status code
func (o *BackupsCancelForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups cancel forbidden response has a 5xx status code
func (o *BackupsCancelForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this backups cancel forbidden response a status code equal to that given
func (o *BackupsCancelForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the backups cancel forbidden response
func (o *BackupsCancelForbidden) Code() int {
	return 403
}

func (o *BackupsCancelForbidden) Error() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelForbidden  %+v", 403, o.Payload)
}

func (o *BackupsCancelForbidden) String() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelForbidden  %+v", 403, o.Payload)
}

func (o *BackupsCancelForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCancelForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCancelUnprocessableEntity creates a BackupsCancelUnprocessableEntity with default headers values
func NewBackupsCancelUnprocessableEntity() *BackupsCancelUnprocessableEntity {
	return &BackupsCancelUnprocessableEntity{}
}

/*
BackupsCancelUnprocessableEntity describes a response with status code 422, with default header values.

Invalid backup cancellation attempt.
*/
type BackupsCancelUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups cancel unprocessable entity response has a 2xx status code
func (o *BackupsCancelUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups cancel unprocessable entity response has a 3xx status code
func (o *BackupsCancelUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups cancel unprocessable entity response has a 4xx status code
func (o *BackupsCancelUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this backups cancel unprocessable entity response has a 5xx status code
func (o *BackupsCancelUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this backups cancel unprocessable entity response a status code equal to that given
func (o *BackupsCancelUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the backups cancel unprocessable entity response
func (o *BackupsCancelUnprocessableEntity) Code() int {
	return 422
}

func (o *BackupsCancelUnprocessableEntity) Error() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsCancelUnprocessableEntity) String() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsCancelUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCancelUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCancelInternalServerError creates a BackupsCancelInternalServerError with default headers values
func NewBackupsCancelInternalServerError() *BackupsCancelInternalServerError {
	return &BackupsCancelInternalServerError{}
}

/*
BackupsCancelInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type BackupsCancelInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this backups cancel internal server error response has a 2xx status code
func (o *BackupsCancelInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this backups cancel internal server error response has a 3xx status code
func (o *BackupsCancelInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this backups cancel internal server error response has a 4xx status code
func (o *BackupsCancelInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this backups cancel internal server error response has a 5xx status code
func (o *BackupsCancelInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this backups cancel internal server error response a status code equal to that given
func (o *BackupsCancelInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the backups cancel internal server error response
func (o *BackupsCancelInternalServerError) Code() int {
	return 500
}

func (o *BackupsCancelInternalServerError) Error() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsCancelInternalServerError) String() string {
	return fmt.Sprintf("[DELETE /backups/{backend}/{id}][%d] backupsCancelInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsCancelInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCancelInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

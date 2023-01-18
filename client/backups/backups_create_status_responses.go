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

// BackupsCreateStatusReader is a Reader for the BackupsCreateStatus structure.
type BackupsCreateStatusReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *BackupsCreateStatusReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewBackupsCreateStatusOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewBackupsCreateStatusUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewBackupsCreateStatusForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewBackupsCreateStatusNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewBackupsCreateStatusUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewBackupsCreateStatusInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewBackupsCreateStatusOK creates a BackupsCreateStatusOK with default headers values
func NewBackupsCreateStatusOK() *BackupsCreateStatusOK {
	return &BackupsCreateStatusOK{}
}

/*
BackupsCreateStatusOK handles this case with default header values.

Backup creation status successfully returned
*/
type BackupsCreateStatusOK struct {
	Payload *models.BackupCreateStatusResponse
}

func (o *BackupsCreateStatusOK) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusOK  %+v", 200, o.Payload)
}

func (o *BackupsCreateStatusOK) GetPayload() *models.BackupCreateStatusResponse {
	return o.Payload
}

func (o *BackupsCreateStatusOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.BackupCreateStatusResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateStatusUnauthorized creates a BackupsCreateStatusUnauthorized with default headers values
func NewBackupsCreateStatusUnauthorized() *BackupsCreateStatusUnauthorized {
	return &BackupsCreateStatusUnauthorized{}
}

/*
BackupsCreateStatusUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type BackupsCreateStatusUnauthorized struct {
}

func (o *BackupsCreateStatusUnauthorized) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusUnauthorized ", 401)
}

func (o *BackupsCreateStatusUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewBackupsCreateStatusForbidden creates a BackupsCreateStatusForbidden with default headers values
func NewBackupsCreateStatusForbidden() *BackupsCreateStatusForbidden {
	return &BackupsCreateStatusForbidden{}
}

/*
BackupsCreateStatusForbidden handles this case with default header values.

Forbidden
*/
type BackupsCreateStatusForbidden struct {
	Payload *models.ErrorResponse
}

func (o *BackupsCreateStatusForbidden) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusForbidden  %+v", 403, o.Payload)
}

func (o *BackupsCreateStatusForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateStatusForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateStatusNotFound creates a BackupsCreateStatusNotFound with default headers values
func NewBackupsCreateStatusNotFound() *BackupsCreateStatusNotFound {
	return &BackupsCreateStatusNotFound{}
}

/*
BackupsCreateStatusNotFound handles this case with default header values.

Not Found - Backup does not exist
*/
type BackupsCreateStatusNotFound struct {
	Payload *models.ErrorResponse
}

func (o *BackupsCreateStatusNotFound) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusNotFound  %+v", 404, o.Payload)
}

func (o *BackupsCreateStatusNotFound) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateStatusNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateStatusUnprocessableEntity creates a BackupsCreateStatusUnprocessableEntity with default headers values
func NewBackupsCreateStatusUnprocessableEntity() *BackupsCreateStatusUnprocessableEntity {
	return &BackupsCreateStatusUnprocessableEntity{}
}

/*
BackupsCreateStatusUnprocessableEntity handles this case with default header values.

Invalid backup restoration status attempt.
*/
type BackupsCreateStatusUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *BackupsCreateStatusUnprocessableEntity) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *BackupsCreateStatusUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateStatusUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewBackupsCreateStatusInternalServerError creates a BackupsCreateStatusInternalServerError with default headers values
func NewBackupsCreateStatusInternalServerError() *BackupsCreateStatusInternalServerError {
	return &BackupsCreateStatusInternalServerError{}
}

/*
BackupsCreateStatusInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type BackupsCreateStatusInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *BackupsCreateStatusInternalServerError) Error() string {
	return fmt.Sprintf("[GET /backups/{backend}/{id}][%d] backupsCreateStatusInternalServerError  %+v", 500, o.Payload)
}

func (o *BackupsCreateStatusInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *BackupsCreateStatusInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
